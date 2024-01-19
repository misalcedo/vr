//! A Primary Copy Method to Support Highly-Available Distributed Systems.
use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;

mod model;
mod network;
mod stamps;

use crate::model::{Commit, DoViewChange, Envelope, Inform, Message, Ping, Prepare, PrepareOk, Reply, Request};
pub use network::Network;
use crate::network::Outbound;
use crate::stamps::{OpNumber, View, ViewTable};

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub enum Status {
    #[default]
    Normal,
    ViewChange,
    Recovering,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestState {
    address: SocketAddr,
    request: u128,
    accepted: HashSet<usize>,
}

impl RequestState {
    pub fn new(address: SocketAddr, request: u128) -> Self {
        RequestState {
            address,
            request,
            accepted: HashSet::new(),
        }
    }

    pub fn is_committed(&self, group_size: usize) -> bool {
        let sub_majority = (group_size - 1) / 2;

        self.accepted.len() >= sub_majority
    }
}

pub trait Service {
    fn invoke(&mut self, payload: &[u8]) -> Vec<u8>;
}

impl<F> Service for F where F: FnMut(&[u8]) -> Vec<u8> {
    fn invoke(&mut self, payload: &[u8]) -> Vec<u8> {
        self(payload)
    }
}

pub trait FailureDetector {
    fn detect(&self) -> bool;
    fn update(&mut self, view: View, from: SocketAddr);
}

pub trait IdleDetector {
    fn detect(&self) -> bool;
    fn tick(&mut self);
}

#[derive(Debug)]
pub struct Replica<S, FD, ID> {
    /// The service code for processing committed client requests.
    service: S,
    /// Detects when a primary is no longer responsive.
    failure_detector: FD,
    /// Detects when the current replica has been idle.
    idle_detector: ID,
    /// The configuration, i.e., the IP address and replica number for each of the 2f + 1 replicas.
    /// The replicas are numbered 0 to 2f.
    configuration: Vec<SocketAddr>,
    /// Each replica also knows its own replica number.
    index: usize,
    /// The view-table containing for each view up to and including the current one the op-number of the latest request known in that view.
    view_table: ViewTable,
    /// The current status, either normal, view-change, or recovering.
    status: Status,
    /// This is an array containing op-number entries.
    /// The entries contain the requests that have been received so far in their assigned order.
    log: Vec<Request>,
    /// This records for each client the number of its most recent request,
    /// plus, if the request has been executed, the result sent for that request.
    client_table: HashMap<u128, BTreeMap<u128, Option<Reply>>>,
    /// The last operation number committed in the current view.
    committed: OpNumber,
    /// The count of operations executed in the current view.
    executed: usize,
    /// Log entries yet to be committed log entry.
    queue: BTreeMap<OpNumber, RequestState>,
}

impl<S, FD, ID> Replica<S, FD, ID>
where
    S: Service,
    FD: FailureDetector,
    ID: IdleDetector
{
    pub fn new(
        service: S,
        failure_detector: FD,
        idle_detector: ID,
        configuration: Vec<SocketAddr>,
        index: usize,
    ) -> Self {
        Self {
            service,
            failure_detector,
            idle_detector,
            configuration,
            index,
            view_table: Default::default(),
            status: Default::default(),
            log: vec![],
            client_table: Default::default(),
            committed: Default::default(),
            executed: 0,
            queue: BTreeMap::new(),
        }
    }

    pub fn poll(&mut self, envelope: Option<Envelope>, outbound: &mut impl Outbound) {
        let primary = self.view_table.primary_index(self.configuration.len());
        let is_primary = primary == self.index;

        if let Some(Envelope { from, message}) = envelope {
            match self.status {
                Status::Normal if message.view() < self.view_table.view() => self.inform(outbound, from),
                Status::Normal if message.view() > self.view_table.view() => {
                    todo!("Perform state transfer")
                }
                Status::Normal if is_primary => self.process_primary(from, message, outbound),
                Status::Normal if from != self.configuration[primary] => self.inform(outbound, from),
                Status::Normal => {
                    self.failure_detector.update(self.view_table.view(), from);
                    self.process_replica(from, message, outbound)
                },
                Status::ViewChange => todo!("Support view change status"),
                Status::Recovering => todo!("Support recovering status"),
            }
        }

        // Perform up-call for committed operations in order.
        if is_primary {
            // TODO: re-send broadcast messages for uncommitted changes.
            self.update_primary(outbound);

            if self.idle_detector.detect() {
                self.broadcast(outbound, Ping { v: self.view_table.view() })
            }
        } else {
            self.update_replica();

            if self.failure_detector.detect() {
                self.do_view_change(outbound);
            }
        }
    }

    fn inform(&mut self, outbound: &mut impl Outbound, to: SocketAddr) {
        outbound.send(
            to,
            Envelope::new(self.configuration[self.index], Inform {
                v: self.view_table.view(),
            })
        )
    }

    fn update_primary(&mut self, outbound: &mut impl Outbound) {
        let mut commit = true;

        while let Some(entry) = self.queue.first_entry() {
            if !entry.get().is_committed(self.configuration.len()) {
                commit = false;

                break;
            }

            let to = entry.get().address;
            let request = &self.log[(u128::from(*entry.key()) - 1) as usize];
            let cache = self.client_table.entry(request.c).or_default();
            let reply = Reply {
                v: self.view_table.view(),
                s: request.s,
                x: self.service.invoke(request.op.as_slice()),
            };

            cache.insert(request.s, Some(reply.clone()));
            self.executed += 1;
            entry.remove();

            outbound.send(to, Envelope::new(self.configuration[self.index], reply));
        }

        if commit {
            // This means we will be sending commits so we are not idle.
            self.idle_detector.tick();

            // TODO: piggy-back committed messages with prepare messages
            self.broadcast(outbound, Commit {
                v: self.view_table.view(),
                n: self.committed,
            })
        }
    }

    fn update_replica(&mut self) {
        while u128::from(self.committed) < (self.executed as u128) && self.executed < self.log.len() {
            let request = &self.log[self.executed];
            self.executed += 1;
            self.service.invoke(request.op.as_slice());
        }
    }

    fn process_primary(&mut self, from: SocketAddr, message: Message, outbound: &mut impl Outbound) {
        match message {
            Message::Request(request) => {
                // This means we will be sending prepares so we are not idle.
                self.idle_detector.tick();

                let cache = self.client_table.entry(request.c).or_default();

                if let Some((&key, value)) = cache.last_key_value() {
                    if key > request.s {
                        todo!("handle discarding old requests resent by the client.")
                    } else if key < request.s && value.is_none() {
                        todo!("handle concurrent requests from a single client.")
                    } else if key < request.s {
                        // got a newer request. so clear out the client's cache.
                        cache.clear();
                    }
                }

                match cache.entry(request.s) {
                    Entry::Vacant(entry) => {
                        entry.insert(None);
                        self.prepare(from, request, outbound)
                    }
                    Entry::Occupied(entry) => {
                        match entry.get() {
                            None => todo!("the client resent the latest request. may want to re-broadcast prepare here if uncommitted"),
                            // send back a cached response for latest request from the client.
                            Some(reply) => outbound.send(from, Envelope::new(self.configuration[self.index], reply.clone())),
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
            }
            _ => (),
        }
    }

    fn prepare(&mut self, client: SocketAddr, request: Request, outbound: &mut impl Outbound) {
        self.log.push(request.clone());
        self.view_table.next_op_number();
        self.queue
            .insert(OpNumber::from(self.log.len()), RequestState::new(client, request.s));

        self.broadcast(outbound, Prepare {
            v: self.view_table.view(),
            n: OpNumber::from(self.log.len()),
            m: request,
        })
    }

    fn process_replica(&mut self, from: SocketAddr, message: Message, outbound: &mut impl Outbound) {
        match message {
            Message::Prepare(message) => {
                let next = self.view_table.op_number().increment();

                match next.cmp(&message.n) {
                    Ordering::Less => todo!("Wait for all earlier log entries or perform state transfer to get missing information"),
                    Ordering::Equal => {
                        self.log.push(message.m);
                        self.view_table.next_op_number();

                        let message = PrepareOk {
                            v: self.view_table.view(),
                            n: self.view_table.op_number(),
                            i: self.index,
                        };

                        outbound.send(from, Envelope::new(self.configuration[self.index], message.clone()));
                    }
                    Ordering::Greater => ()
                }
            }
            Message::Commit(message) if message.v == self.view_table.view() => {
                self.committed = self.committed.max(message.n);
            }
            _ => (),
        }
    }

    fn broadcast(&mut self, outbound: &mut impl Outbound, message: impl Into<Message>) {
        let message = message.into();

        for (i, replica) in self.configuration.iter().enumerate() {
            if i != self.index {
                outbound.send(*replica, Envelope::new(self.configuration[self.index], message.clone()));
            }
        }
    }

    fn do_view_change(&mut self, outbound: &mut impl Outbound) {
        self.view_table.next_view();
        self.status = Status::ViewChange;

        let primary = self.view_table.primary_index(self.configuration.len());
        let envelope = Envelope::new(self.configuration[self.index], DoViewChange {
            v: self.view_table.view(),
            l: self.log.clone(),
            k: self.view_table.op_number(),
            i: self.index,
        });

        outbound.send(self.configuration[primary], envelope)
    }
}

pub struct Client {
    configuration: Vec<SocketAddr>,
    view: View,
    id: u128,
    requests: u128,
}

impl Client {
    pub fn new(configuration: Vec<SocketAddr>, id: u128) -> Self {
        Self {
            configuration,
            view: Default::default(),
            id,
            requests: uuid::Uuid::now_v7().as_u128(),
        }
    }

    pub fn new_request(&mut self, payload: Vec<u8>) -> (SocketAddr, Request) {
        let primary = self.view.primary_index(self.configuration.len());
        let request = Request {
            op: payload,
            c: self.id,
            s: self.requests,
            v: self.view,
        };

        self.requests += 1;

        (self.configuration[primary], request)
    }

    pub fn update(&mut self, message: &Message) {
        self.view = message.view();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DoViewChange, Message};

    struct TestFailureDetector {
        trigger_view: View,
        current_view: View,
        enabled: bool
    }

    impl FailureDetector for TestFailureDetector {
        fn detect(&self) -> bool {
            self.enabled && self.trigger_view == self.current_view
        }

        fn update(&mut self, view: View, _: SocketAddr) {
            self.current_view = view
        }
    }

    impl FailureDetector for () {
        fn detect(&self) -> bool {
            false
        }

        fn update(&mut self, _: View, _: SocketAddr) {}
    }

    impl IdleDetector for () {
        fn detect(&self) -> bool {
            false
        }

        fn tick(&mut self) {}
    }

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
        let configuration = configuration();
        let mut network = Network::default();
        let mut replicas = Vec::with_capacity(configuration.len());

        for (index, address) in configuration.iter().enumerate() {
            network.bind(*address).unwrap();

            let mut counter = 0usize;
            let service = move |request: &[u8]| {
                counter += request.len();
                counter.to_be_bytes().to_vec()
            };
            replicas.push(Replica::new(
                service,
                (),
                (),
                configuration.clone(),
                index,
            ));
        }

        let client_address = "127.0.0.1:4001".parse().unwrap();
        let mut client = Client::new(configuration.clone(), 1);

        network.bind(client_address).unwrap();

        let payload = b"Hello, World!".to_vec();
        let (primary, request) = client.new_request(payload.clone());

        network
            .send(primary, Envelope::new(client_address, request.clone()))
            .unwrap();

        replicas[0].poll(network.receive(configuration[0]).ok(), &mut network);
        replicas[1].poll(network.receive(configuration[1]).ok(), &mut network);
        replicas[2].poll(network.receive(configuration[2]).ok(), &mut network);
        replicas[0].poll(network.receive(configuration[0]).ok(), &mut network);
        replicas[0].poll(network.receive(configuration[0]).ok(), &mut network);

        let Envelope { from: sender, message} = network.receive(client_address).unwrap();

        client.update(&message);

        assert_eq!(client.view, replicas[0].view_table.view());
        assert_eq!(sender, configuration[0]);
        assert_eq!(
            message,
            Message::Reply(Reply {
                v: request.v,
                s: request.s,
                x: payload.len().to_be_bytes().to_vec(),
            })
        );
    }

    #[test]
    fn simulate_failure() {
        let configuration = configuration();
        let mut network = Network::default();
        let mut replicas = Vec::with_capacity(configuration.len());

        for (index, address) in configuration.iter().enumerate() {
            network.bind(*address).unwrap();

            let mut counter = 0usize;
            let service = move |request: &[u8]| {
                counter += request.len();
                counter.to_be_bytes().to_vec()
            };
            replicas.push(Replica::new(
                service,
                TestFailureDetector {
                    trigger_view: Default::default(),
                    current_view: Default::default(),
                    enabled: index == 2,
                },
                (),
                configuration.clone(),
                index,
            ));
        }

        let client_address = "127.0.0.1:4001".parse().unwrap();
        let mut client = Client::new(configuration.clone(), 1);

        network.bind(client_address).unwrap();

        let payload = b"Hello, World!".to_vec();
        let (primary, request) = client.new_request(payload.clone());

        network
            .send(primary, Envelope::new(client_address, request.clone()))
            .unwrap();

        replicas[0].poll(network.receive(configuration[0]).ok(), &mut network);
        replicas[1].poll(network.receive(configuration[1]).ok(), &mut network);
        replicas[0].poll(network.receive(configuration[0]).ok(), &mut network);
        replicas[1].poll(network.receive(configuration[1]).ok(), &mut network);

        let Envelope { message, ..} = network.receive(client_address).unwrap();

        client.update(&message);

        // skip prepare
        network.receive(configuration[2]).unwrap();
        // skip commit
        network.receive(configuration[2]).unwrap();
        // start view change
        replicas[2].poll(network.receive(configuration[2]).ok(), &mut network);

        let Envelope { from, message} = network.receive(configuration[1]).unwrap();

        assert_eq!(
            message,
            Message::DoViewChange(DoViewChange {
                v: View::from(1),
                l: vec![],
                k: OpNumber::from(0),
                i: 2
            })
        );
        assert_eq!(from, configuration[2]);
    }

    fn configuration() -> Vec<SocketAddr> {
        vec![
            "127.0.0.1:3001".parse().unwrap(),
            "127.0.0.1:3002".parse().unwrap(),
            "127.0.0.1:3003".parse().unwrap(),
        ]
    }
}
