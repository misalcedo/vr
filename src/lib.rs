//! A Primary Copy Method to Support Highly-Available Distributed Systems.

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;

mod model;
mod network;

pub use network::{Network, CommunicationStream};
use crate::model::{Message, Prepare, PrepareOk, Reply, Request};

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
    reply: Option<Reply>,
}

impl RequestState {
    pub fn new(address: SocketAddr, request: u128, ) -> Self {
        RequestState {
            address,
            request,
            accepted: HashSet::new(),
            reply: None
        }
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
    client_table: HashMap<u128, RequestState>,
    /// The last committed log entry.
    committed: usize
}

impl<Service> Replica<Service>
where Service: FnMut(Vec<u8>) -> Vec<u8> {
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
        }
    }

    pub fn poll(&mut self) -> io::Result<()> {
        let is_primary = (self.view_number % self.configuration.len()) == self.index;

        match self.communication.receive() {
            Ok((from, Message::Request(request))) if is_primary => {
                let state = self.client_table.entry(request.c)
                    .or_insert_with(|| { RequestState::new(from, request.s) });

                match state.reply.as_ref().map(Reply::clone) {
                    Some(reply) => {
                        self.communication.send(from, Message::Reply(reply))
                    },
                    None => {
                        self.log.push(request.clone());

                        let message = Message::Prepare(Prepare {
                            v: self.view_number,
                            n: self.log.len(),
                            m: request.clone(),
                        });

                        self.broadcast(message)
                    }
                }
            }
            Ok((from, Message::Prepare(message))) if !is_primary => {
                // TODO: ensure the prepare is from the current view's primary.

                let next = self.log.len() + 1;

                if message.v == self.view_number && message.n > next {
                    // TODO: wait until it has entries in its log for all earlier requests
                    // (doing state transfer if necessary to get the missing information)
                    Err(io::Error::new(io::ErrorKind::Unsupported, "state transfer and buffering not yet supported"))
                } else if message.v == self.view_number && next == message.n {
                    self.log.push(message.m);

                    let message = Message::PrepareOk(PrepareOk {
                        v: self.view_number,
                        n: self.log.len(),
                        i: self.index,
                    });

                    self.communication.send(from, message.clone())
                } else {
                    Ok(())
                }
            }
            Ok((from, Message::PrepareOk(message))) if is_primary => {
                match self.log.get(message.n) {
                    None => Ok(()),
                    Some(request) => {
                        let state = self.client_table.entry(request.c)
                            .or_insert_with(|| { RequestState::new(from, request.s) });

                        if state.request == request.s {
                            state.accepted.insert(message.i);

                            // TODO: Update committed.
                            let sub_majority = (self.configuration.len() - 1) / 2;

                            (self.service)(request.op.clone())
                        } else {
                            Ok(())
                        }
                    }
                }
            }
            Ok((from, Message::Commit(message))) => {
                Ok(())
            }
            Ok(_) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(e) => Err(e)
        }
    }

    fn broadcast(&mut self, message: Message) -> io::Result<()> {
        for (i, replica) in self.configuration.iter().enumerate() {
            if i != self.index {
                self.communication.send(*replica, message.clone())?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::model::Message;
    use super::*;

    #[test]
    fn simulate() {
        let configuration = vec!["127.0.0.1:3001".parse().unwrap(), "127.0.0.1:3002".parse().unwrap(), "127.0.0.1:3003".parse().unwrap()];
        let mut network = Network::default();
        let mut replicas = Vec::with_capacity(configuration.len());

        for (index, address) in configuration.iter().enumerate() {
            let mut counter = 0usize;
            let service = move |request| {
                counter += 1;
                counter.to_be_bytes().to_vec()
            };
            replicas.push(Replica::new(service, network.bind(*address).unwrap(), configuration.clone(), index));
        }

        let mut client = network.bind("127.0.0.1:4001".parse().unwrap()).unwrap();

        let request = Request {
            op: b"Hello, World!".to_vec(),
            c: 1,
            s: 1,
            v: 0,
        };

        client.send(configuration[0], Message::Request(request.clone())).unwrap();

        replicas[0].poll().unwrap();
        replicas[1].poll().unwrap();
        replicas[2].poll().unwrap();
        replicas[0].poll().unwrap();
        replicas[0].poll().unwrap();

        assert_eq!(client.receive().unwrap(), (configuration[0], Message::Reply(Reply {
            v: request.v,
            s: request.s,
            x: 1usize.to_be_bytes().to_vec(),
        })));
    }
}

