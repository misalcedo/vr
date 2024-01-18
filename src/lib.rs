//! A Primary Copy Method to Support Highly-Available Distributed Systems.
use std::collections::{BTreeMap, HashMap, HashSet};
use std::collections::btree_map::Entry;
use std::io;
use std::net::SocketAddr;

mod model;
mod network;

pub use network::{Network, CommunicationStream};
use crate::model::{Commit, Inform, Message, Prepare, PrepareOk, Reply, Request};

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub enum Status {
    #[default]
    Normal,
    ViewChange,
    Recovering
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestState {
    address: SocketAddr,
    request: u128,
    accepted: HashSet<usize>,
}

impl RequestState {
    pub fn new(address: SocketAddr, request: u128, ) -> Self {
        RequestState {
            address,
            request,
            accepted: HashSet::new(),
        }
    }

    pub fn is_committed(&self, group_size: usize) -> bool {
        let sub_majority = (group_size - 1) / 2;

        return self.accepted.len() >= sub_majority;
    }
}

#[derive(Debug)]
pub struct Replica<Service> {
    /// The service code for processing committed client requests.
    service: Service,
    /// The interface for this replica to communicate with other replicas.
    communication: CommunicationStream,
    /// The configuration, i.e., the IP address and replica number for each of the 2f + 1 replicas.
    /// The replicas are numbered 0 to 2f.
    configuration: Vec<SocketAddr>,
    /// Each replica also knows its own replica number.
    index: usize,
    /// The current view-number, initially 0.
    view_number: usize,
    /// The current status, either normal, view-change, or recovering.
    status: Status,
    /// This is an array containing op-number entries.
    /// The entries contain the requests that have been received so far in their assigned order.
    log: Vec<Request>,
    /// This records for each client the number of its most recent request,
    /// plus, if the request has been executed, the result sent for that request.
    client_table: HashMap<u128, BTreeMap<u128, Option<Reply>>>,
    /// The last operation number committed in the current view.
    committed: usize,
    /// The count of operations executed in the current view.
    executed: usize,
    /// Log entries yet to be committed log entry.
    queue: BTreeMap<usize, RequestState>
}

impl<Service> Replica<Service>
where Service: FnMut(&[u8]) -> Vec<u8> {
    pub fn new(service: Service, communication: CommunicationStream, configuration: Vec<SocketAddr>, index: usize) -> Self {
        Self {
            service,
            communication,
            configuration,
            index,
            view_number: 0,
            status: Default::default(),
            log: vec![],
            client_table: Default::default(),
            committed: 0,
            executed: 0,
            queue: BTreeMap::new(),
        }
    }

    pub fn poll(&mut self) -> io::Result<()> {
        let primary = self.view_number % self.configuration.len();
        let is_primary = primary == self.index;

        let result = match self.communication.receive() {
            Ok((from, message)) => {
                match self.status {
                    Status::Normal if message.view_number() < self.view_number => self.inform(from),
                    Status::Normal if message.view_number() > self.view_number => todo!("Perform state transfer"),
                    Status::Normal if is_primary => self.process_primary(from, message),
                    Status::Normal if from != self.configuration[primary] => self.inform(from),
                    Status::Normal => self.process_replica(from, message),
                    Status::ViewChange => todo!("Support view change status"),
                    Status::Recovering => todo!("Support recovering status")
                }
            },
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(e) => Err(e)
        };

        // Perform up-call for committed operations in order.
        if is_primary {
            // TODO: re-send broadcast messages for uncommitted changes.
            self.update_primary()?;
        } else {
            self.update_replica()?;
        }

        result
    }

    fn inform(&mut self, to: SocketAddr) -> io::Result<()> {
        self.communication.send(to, Inform { v: self.view_number })
    }

    fn update_primary(&mut self) -> io::Result<()> {
        while let Some(entry) = self.queue.first_entry() {
            if !entry.get().is_committed(self.configuration.len()) {
                break;
            }

            let to = entry.get().address;
            let request = &self.log[entry.key() - 1];
            let cache = self.client_table.entry(request.c).or_default();
            let reply = Reply {
                v: self.view_number,
                s: request.s,
                x: (self.service)(request.op.as_slice()),
            };

            cache.insert(request.s, Some(reply.clone()));
            self.executed += 1;
            entry.remove();

            // TODO: handle partial failure in committing an operation.
            self.communication.send(to, reply)?;
        }

        // TODO: piggy-back committed messages with prepare messages
        self.broadcast(Commit { v: self.view_number, n: self.committed})
    }

    fn update_replica(&mut self) -> io::Result<()> {
        while self.committed < self.executed && self.executed < self.log.len() {
            let request = &self.log[self.executed];
            self.executed += 1;
            (self.service)(request.op.as_slice());
        }

        Ok(())
    }

    fn process_primary(&mut self, from: SocketAddr, message: Message) -> io::Result<()> {
        match message {
            Message::Request(request) => {
                let cache = self.client_table.entry(request.c)
                    .or_default();

                if let Some((&key, value)) = cache.last_key_value() {
                    if key > request.s {
                        // TODO: handle discarding old requests resent by the client.
                    } else if key < request.s && value.is_none() {
                        // TODO: handle concurrent requests from a single client.
                    } else if key < request.s {
                        // got a newer request. so clear out the client's cache.
                        cache.clear();
                    }
                }

                match cache.entry(request.s) {
                    Entry::Vacant(entry) => {
                        entry.insert(None);
                        self.prepare(from, request)
                    }
                    Entry::Occupied(entry) => {
                        match entry.get() {
                            // TODO: this is a client resending the latest request.
                            // may want to re-broadcast prepare here if uncommitted.
                            None => Ok(()),
                            // send back a cached response for latest request from the client.
                            Some(reply) => self.communication.send(from, reply.clone()),
                        }
                    }
                }
            }
            Message::PrepareOk(message) => {
                // Only committed ops are popped from the queue, so we can safely ignore prepare messages for anything not in the queue.
                if let Entry::Occupied(mut entry) = self.queue.entry(message.n) {
                    entry.get_mut().accepted.insert(message.i);

                    if entry.get().is_committed(self.configuration.len()) {
                        // Operations can be committed out of order depending on the guarantees of the network .
                        self.committed = self.committed.max(message.n);
                    }
                }

                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn prepare(&mut self, from: SocketAddr, request: Request) -> io::Result<()> {
        self.log.push(request.clone());
        self.queue.insert(self.log.len(), RequestState::new(from, request.s));

        self.broadcast(Prepare {
            v: self.view_number,
            n: self.log.len(),
            m: request,
        })
    }

    fn process_replica(&mut self, from: SocketAddr, message: Message) -> io::Result<()> {
        match message {
            Message::Prepare(message) => {
                let next = self.log.len() + 1;

                if next < message.n {
                    todo!("Wait for all earlier log entries or perform state transfer to get missing information")
                } else if next == message.n {
                    self.log.push(message.m);

                    let message = PrepareOk {
                        v: self.view_number,
                        n: self.log.len(),
                        i: self.index,
                    };

                    self.communication.send(from, message.clone())
                } else {
                    Ok(())
                }
            }
            Message::Commit(message) if message.v == self.view_number => {
                // TODO: figure out how to handle committed less than current.
                self.committed = message.n;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn broadcast<M: Into<Message>>(&mut self, message: M) -> io::Result<()> {
        let mut errors = 0;
        let message = message.into();

        for (i, replica) in self.configuration.iter().enumerate() {
            if i != self.index {
                if self.communication.send(*replica, message.clone()).is_err() {
                    errors += 1;
                }
            }
        }

        if errors == 0 {
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "partial broadcast"))
        }
    }
}

pub struct Client {
    configuration: Vec<SocketAddr>,
    view_number: usize,
    id: u128,
    requests: u128,
}

impl Client {
    // TODO: use uuid v7 to handle client crashes and recovery (must start with higher request number than before crash).
    pub fn new(configuration: Vec<SocketAddr>, id: u128) -> Self {
        Self {
            configuration,
            view_number: 0,
            id,
            requests: 0,
        }
    }

    pub fn new_request(&mut self, payload: Vec<u8>) -> (SocketAddr, Request) {
        let primary = self.view_number % self.configuration.len();
        let request = Request {
            op: payload,
            c: self.id,
            s: self.requests,
            v: self.view_number,
        };

        self.requests += 1;

        (self.configuration[primary], request)
    }

    pub fn update(&mut self, message: &Message) {
         self.view_number = message.view_number();
    }
}

#[cfg(test)]
mod tests {
    use crate::model::Message;
    use super::*;

    #[test]
    fn queue() {
        let mut map = BTreeMap::new();

        map.insert(1, 1);
        map.insert(2, 2);

        assert_eq!(map.first_entry().unwrap().remove(), 1);
        assert_eq!(map.first_entry().unwrap().remove(), 2);
    }

    #[test]
    fn simulate() {
        let configuration = vec!["127.0.0.1:3001".parse().unwrap(), "127.0.0.1:3002".parse().unwrap(), "127.0.0.1:3003".parse().unwrap()];
        let mut network = Network::default();
        let mut replicas = Vec::with_capacity(configuration.len());

        for (index, address) in configuration.iter().enumerate() {
            let mut counter = 0usize;
            let service = move |request: &[u8]| {
                counter += request.len();
                counter.to_be_bytes().to_vec()
            };
            replicas.push(Replica::new(service, network.bind(*address).unwrap(), configuration.clone(), index));
        }

        let mut client_stream = network.bind("127.0.0.1:4001".parse().unwrap()).unwrap();
        let mut client = Client::new(configuration.clone(), 1);

        let payload = b"Hello, World!".to_vec();
        let (primary, request) = client.new_request(payload.clone());

        client_stream.send(primary, Message::Request(request.clone())).unwrap();

        replicas[0].poll().unwrap();
        replicas[1].poll().unwrap();
        replicas[2].poll().unwrap();
        replicas[0].poll().unwrap();
        replicas[0].poll().unwrap();

        let (sender, message) = client_stream.receive().unwrap();

        client.update(&message);

        assert_eq!(client.view_number, replicas[0].view_number);
        assert_eq!(sender, configuration[0]);
        assert_eq!(message, Message::Reply(Reply {
            v: request.v,
            s: request.s,
            x: payload.len().to_be_bytes().to_vec(),
        }));
    }
}

