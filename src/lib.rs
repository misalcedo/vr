//! A Primary Copy Method to Support Highly-Available Distributed Systems.
use std::cmp::{Ordering, Reverse};
use std::collections::btree_map::Entry;
use std::collections::{BinaryHeap, BTreeMap, HashSet};
use std::net::SocketAddr;
use std::num::NonZeroUsize;

mod client;
mod model;
mod network;
mod stamps;
mod view_change;

use crate::model::{DoViewChange, Envelope, Inform, Message, Ping, Prepare, PrepareOk, Reply, Request, StartView};
pub use network::Network;
use crate::client::RequestCache;
use crate::network::Outbound;
use crate::stamps::{OpNumber, View, ViewTable};
use crate::view_change::ViewChangeBuffer;

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
    fn update(&mut self, view: View, from: SocketAddr, group_size: usize);
}

pub trait IdleDetector {
    fn detect(&self) -> bool;
    fn tick(&mut self);
}

#[derive(Clone, Debug, Default)]
pub struct ReplicaBuilder<S, FD, ID> {
    /// The service code for processing committed client requests.
    service: Option<S>,
    /// Detects when a primary is no longer responsive.
    failure_detector: Option<FD>,
    /// Detects when the current replica has been idle.
    idle_detector: Option<ID>,
    /// The configuration, i.e., the IP address and replica number for each of the 2f + 1 replicas.
    /// The replicas are numbered 0 to 2f.
    configuration: Vec<SocketAddr>,
    /// The depth of the queue at which point the primary will re-broadcast prepare messages.
    commit_queue_threshold: usize,
    /// The depth of the queue at which point the primary will re-broadcast prepare messages.
    /// Defined as a multiplier on the commit queue depth to help with prevent spurious view changes.
    prepare_queue_threshold_multiplier: Option<NonZeroUsize>,
}

impl<S, FD, ID> ReplicaBuilder<S, FD, ID>
    where
        S: Service + Clone,
        FD: FailureDetector + Clone,
        ID: IdleDetector + Clone
{
    pub fn new() -> Self {
        Self {
            service: None,
            failure_detector: None,
            idle_detector: None,
            configuration: Vec::new(),
            commit_queue_threshold: 1,
            prepare_queue_threshold_multiplier: None
        }
    }

    /// Sets the multiplier for the prepare queue depth threshold.
    /// Values of `0` are treated as use the default.
    pub fn with_prepare_multiplier(mut self, multiplier: usize) -> Self {
        self.prepare_queue_threshold_multiplier = NonZeroUsize::new(multiplier);
        self
    }

    pub fn with_commit_queue_threshold(mut self, threshold: usize) -> Self {
        self.commit_queue_threshold = threshold;
        self
    }

    pub fn with_replica(mut self, address: SocketAddr) -> Self {
        self.configuration.push(address);
        self
    }

    pub fn with_service(mut self, service: S) -> Self {
        self.service = Some(service);
        self
    }

    pub fn with_failure_detector(mut self, detector: FD) -> Self {
        self.failure_detector = Some(detector);
        self
    }

    pub fn with_idle_detector(mut self, detector: ID) -> Self {
        self.idle_detector = Some(detector);
        self
    }

    pub fn build(self) -> Result<Vec<Replica<S, FD, ID>>, Self> {
        let mut clone = self.clone();

        match (clone.service.take(), clone.failure_detector.take(), clone.idle_detector.take(), clone.configuration) {
            (Some(service), Some(failure_detector), Some(idle_detector), configuration) if !configuration.is_empty() => {
                let mut replicas = Vec::with_capacity(configuration.len());

                for index in 0..configuration.len() {
                    replicas.push(Replica::new(
                        service.clone(),
                        failure_detector.clone(),
                        idle_detector.clone(),
                        configuration.clone(),
                        index,
                        self.commit_queue_threshold,
                        self.prepare_queue_threshold_multiplier.map(NonZeroUsize::get).unwrap_or(2)
                    ));
                }

                Ok(replicas)
            },
            _ => Err(self)
        }
    }
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
    client_table: RequestCache,
    /// The last operation number committed in the current view.
    committed: OpNumber,
    /// The count of operations executed in the current view.
    executed: usize,
    /// The depth of the queue at which point the primary will re-broadcast prepare messages.
    commit_queue_threshold: usize,
    /// Log entries yet to be committed log entry.
    commit_queue: BTreeMap<OpNumber, RequestState>,
    /// The depth of the queue at which point the primary will re-broadcast prepare messages.
    prepare_queue_threshold: usize,
    /// Priority queue of prepare messages to wait until the prepares are in order.
    prepare_queue: BinaryHeap<Reverse<Prepare>>,
    /// Buffer of do-view-change messages on the primary of the new view.
    view_change_buffer: ViewChangeBuffer
}

impl<S, FD, ID> Replica<S, FD, ID>
where
    S: Service,
    FD: FailureDetector,
    ID: IdleDetector
{
    // TODO: Implement some sort of builder that can stamp out replicas with different indices.
    pub fn new(
        service: S,
        failure_detector: FD,
        idle_detector: ID,
        configuration: Vec<SocketAddr>,
        index: usize,
        commit_queue_threshold: usize,
        prepare_queue_threshold_multiplier: usize
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
            commit_queue_threshold,
            commit_queue: BTreeMap::new(),
            prepare_queue_threshold: commit_queue_threshold * prepare_queue_threshold_multiplier,
            prepare_queue: BinaryHeap::new(),
            view_change_buffer: Default::default(),
        }
    }

    pub fn address(&self) -> SocketAddr {
        self.configuration[self.index]
    }

    pub fn poll(&mut self, envelope: Option<Envelope>, outbound: &mut impl Outbound) {
        let primary = self.view_table.primary_index(self.configuration.len());
        let is_primary = primary == self.index;

        if let Some(envelope) = envelope {
            self.process_message(outbound, envelope);
        }

        // Perform up-call for committed operations in order.
        if is_primary {
            self.start_view_change(outbound);
            self.update_primary(outbound);

            if self.idle_detector.detect() {
                self.broadcast(outbound, Ping {
                    v: self.view_table.view(),
                    c: self.committed
                })
            }
        } else {
            self.update_replica(outbound);

            if self.failure_detector.detect() {
                self.do_view_change(outbound);
            }
        }
    }

    fn process_message(&mut self, outbound: &mut impl Outbound, envelope: Envelope) {
        let primary = self.view_table.primary_index(self.configuration.len());
        let is_primary = primary == self.index;

        match self.status {
            Status::Normal if envelope.message.view() < self.view_table.view() => self.inform(outbound, envelope.from),
            Status::Normal if envelope.message.view() > self.view_table.view() => {
                let new_primary = envelope.message.view().primary_index(self.configuration.len());

                match envelope.message {
                    Message::DoViewChange(do_view_change) if self.index == new_primary => {
                        self.view_change_buffer.insert(do_view_change);
                    }
                    _ => todo!("Perform state transfer")
                }
            }
            Status::Normal if is_primary => self.process_primary(envelope, outbound),
            Status::Normal if envelope.from != self.configuration[primary] => self.inform(outbound, envelope.from),
            Status::Normal => {
                self.failure_detector.update(self.view_table.view(), envelope.from, self.configuration.len());
                self.process_replica(envelope, outbound)
            },
            Status::ViewChange if self.index == envelope.message.view().primary_index(self.configuration.len()) => {
                match envelope.message {
                    Message::DoViewChange(do_view_change) => {
                        self.view_change_buffer.insert(do_view_change);
                    }
                    _ => todo!("Figure out what to do with these messages")
                }
            },
            Status::ViewChange => todo!("Support recovering status"),
            Status::Recovering => todo!("Support recovering status"),
        }
    }

    fn inform(&mut self, outbound: &mut impl Outbound, to: SocketAddr) {
        outbound.send(
            Envelope::new(self.configuration[self.index], to, Inform {
                v: self.view_table.view(),
            })
        )
    }

    fn update_primary(&mut self, outbound: &mut impl Outbound) {
        if self.status != Status::Normal {
            return;
        }

        while let Some(entry) = self.commit_queue.first_entry() {
            let request = &self.log[(u128::from(*entry.key()) - 1) as usize];

            if !entry.get().is_committed(self.configuration.len()) {
                if self.commit_queue.len() > self.commit_queue_threshold {
                    // TODO: Reaching this threshold should cause the primary to re-send prepares only to replicas that have not responded yet.
                    self.broadcast(outbound, Prepare {
                        v: self.view_table.view(),
                        n: OpNumber::from(self.log.len()),
                        m: request.clone(),
                        c: self.committed
                    });
                }

                break;
            }

            let to = entry.get().address;
            let reply = Reply {
                v: self.view_table.view(),
                s: request.s,
                x: self.service.invoke(request.op.as_slice()),
            };

            self.client_table.set(&request, &reply);

            self.executed += 1;
            entry.remove();

            outbound.send(Envelope::new(self.configuration[self.index], to, reply));
        }
    }

    fn update_replica(&mut self, outbound: &mut impl Outbound) {
        if self.status != Status::Normal {
            return;
        }

        self.execute_committed();

        while let Some(Reverse(message)) = self.prepare_queue.pop() {
            // Ignore buffered prepares for older views.
            if message.v < self.view_table.view() {
                continue;
            }

            let next = self.view_table.op_number().increment();

            if message.n != next {
                self.prepare_queue.push(Reverse(message));

                if self.prepare_queue.len() > self.prepare_queue_threshold {
                    todo!("Perform state transfer to get missing information")
                }

                break;
            }

            let primary = self.configuration[self.view_table.primary_index(self.configuration.len())];

            self.prepare_ok(primary, message, outbound);
        }
    }

    fn process_primary(&mut self, envelope: Envelope, outbound: &mut impl Outbound) {
        match envelope.message {
            Message::Request(request) => {
                match self.client_table.partial_cmp(&request) {
                    None => {
                        // got a newer request or this is the first request from the client.
                        self.client_table.start(&request);
                        self.prepare(envelope.from, request, outbound)
                    }
                    Some(Ordering::Less) => {
                        todo!("handle concurrent requests from a single client.")
                    }
                    Some(Ordering::Equal) => {
                        match self.client_table.get(&request) {
                            None => {
                                // the client resent the latest request.
                                // we do not want to re-broadcast here to avoid the client being able to overwhelm the network.
                                todo!("handle resent in-progress requests.")
                            }
                            Some(reply) => {
                                // send back a cached response for latest request from the client.
                                outbound.send(Envelope::new(self.configuration[self.index], envelope.from, reply.clone()))
                            }
                        }

                    }
                    Some(Ordering::Greater) => {
                        todo!("handle discarding old requests resent by the client.")
                    }
                }
            }
            Message::PrepareOk(message) => {
                // Only committed ops are popped from the queue, so we can safely ignore prepare messages for anything not in the queue.
                if let Entry::Occupied(mut entry) = self.commit_queue.entry(message.n) {
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
        self.commit_queue
            .insert(OpNumber::from(self.log.len()), RequestState::new(client, request.s));

        self.broadcast(outbound, Prepare {
            v: self.view_table.view(),
            n: OpNumber::from(self.log.len()),
            m: request,
            c: self.committed
        })
    }

    fn process_replica(&mut self, envelope: Envelope, outbound: &mut impl Outbound) {
        match envelope.message {
            Message::Prepare(message) => {
                // TODO: If committed is higher than messages in the prepare buffer we need to enter recovery mode
                // to avoid buffering prepares that will never get processed.
                self.committed = self.committed.max(message.c);

                let next = self.view_table.op_number().increment();

                match next.cmp(&message.n) {
                    Ordering::Less => {
                        self.prepare_queue.push(Reverse(message));
                    },
                    Ordering::Equal => {
                        self.prepare_ok(envelope.from, message, outbound);
                    }
                    Ordering::Greater => ()
                }
            }
            _ => (),
        }
    }

    fn prepare_ok(&mut self, from: SocketAddr, message: Prepare, outbound: &mut impl Outbound) {
        self.log.push(message.m);
        self.view_table.next_op_number();

        let message = PrepareOk {
            v: self.view_table.view(),
            n: self.view_table.op_number(),
            i: self.index,
        };

        outbound.send(Envelope::new(self.configuration[self.index], from, message.clone()));
    }

    fn broadcast(&mut self, outbound: &mut impl Outbound, message: impl Into<Message>) {
        let message = message.into();

        for (i, replica) in self.configuration.iter().enumerate() {
            if i != self.index {
                outbound.send(Envelope::new(self.configuration[self.index], *replica, message.clone()));
            }
        }

        // we are not idle if we are sending messages to all replicas.
        self.idle_detector.tick();
    }

    fn do_view_change(&mut self, outbound: &mut impl Outbound) {
        self.view_table.next_view();
        self.status = Status::ViewChange;

        let primary = self.view_table.primary_index(self.configuration.len());
        let envelope = Envelope::new(self.configuration[self.index], self.configuration[primary],  DoViewChange {
            v: self.view_table.view(),
            t: self.view_table.clone(),
            l: self.log.clone(),
            k: self.view_table.op_number(),
            i: self.index,
        });

        outbound.send(envelope)
    }

    fn start_view_change(&mut self, outbound: &mut impl Outbound) {
        match self.view_change_buffer.start_view(self.index, self.configuration.len()) {
            None => (),
            Some(do_view_change) => {
                self.log = do_view_change.l;
                self.view_table.set_last_op_number(do_view_change.t.op_number());
                self.committed = self.committed.max(do_view_change.k);
                self.status = Status::Normal;

                self.broadcast(outbound, StartView {
                    v: self.view_table.view(),
                    l: self.log.clone(),
                    k: self.view_table.op_number(),
                });

                self.execute_committed();

                // TODO: Send replies to the clients.
                // We might not know the socket address of the client because we may not be the one that received the request.
                // The best we can do is wait for the client to query the system and detect that it's last request completed.
            }
        }
    }

    fn execute_committed(&mut self) {
        while u128::from(self.committed) < (self.executed as u128) && self.executed < self.log.len() {
            let request = &self.log[self.executed];
            let reply = Reply {
                v: self.view_table.view(),
                s: request.s,
                x: self.service.invoke(request.op.as_slice()),
            };

            self.client_table.set(&request, &reply);
            self.executed += 1;
        }
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
    use std::fmt::Debug;
    use super::*;
    use crate::model::{DoViewChange, Message};

    impl Service for usize {
        fn invoke(&mut self, payload: &[u8]) -> Vec<u8> {
            *self += payload.len();
            self.to_be_bytes().to_vec()
        }
    }

    impl FailureDetector for bool {
        fn detect(&self) -> bool {
            *self
        }

        fn update(&mut self, _: View, _: SocketAddr, _: usize) {
        }
    }

    impl IdleDetector for bool {
        fn detect(&self) -> bool {
            *self
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
        let mut replicas = build_replicas(false);
        let mut network = Network::default();
        for address in replicas.iter().map(Replica::address) {
            network.bind(address).unwrap();
        }

        let client_address = "127.0.0.1:4001".parse().unwrap();
        let mut client = Client::new(replicas[0].configuration.clone(), 1);

        network.bind(client_address).unwrap();

        let payload = b"Hello, World!".to_vec();
        let (primary, request) = client.new_request(payload.clone());

        network
            .send(Envelope::new(client_address, primary, request.clone()))
            .unwrap();

        poll(&mut network, &mut replicas[0]);
        poll(&mut network, &mut replicas[1]);
        poll(&mut network, &mut replicas[2]);
        poll(&mut network, &mut replicas[0]);
        poll(&mut network, &mut replicas[0]);

        let Envelope { from: sender, message, ..} = network.receive(client_address).unwrap();

        client.update(&message);

        assert_eq!(client.view, replicas[0].view_table.view());
        assert_eq!(sender, replicas[0].address());
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
        let mut replicas = build_replicas(false);
        let mut network = Network::default();
        for address in replicas.iter().map(Replica::address) {
            network.bind(address).unwrap();
        }

        let client_address = "127.0.0.1:4001".parse().unwrap();
        let mut client = Client::new(replicas[0].configuration.clone(), 1);

        network.bind(client_address).unwrap();

        let payload = b"Hello, World!".to_vec();
        let (primary, request) = client.new_request(payload.clone());

        replicas[2].failure_detector = true;

        network
            .send(Envelope::new(client_address, primary, request.clone()))
            .unwrap();

        poll(&mut network, &mut replicas[0]);
        poll(&mut network, &mut replicas[1]);
        poll(&mut network, &mut replicas[0]);
        poll(&mut network, &mut replicas[1]);

        let Envelope { message, ..} = network.receive(client_address).unwrap();

        client.update(&message);

        // skip prepare
        network.receive(replicas[2].address()).unwrap();
        // start view change
        poll(&mut network, &mut replicas[2]);

        let Envelope { from, message, ..} = network.receive(replicas[1].address()).unwrap();

        let mut table = ViewTable::default();
        table.next_view();

        assert_eq!(
            message,
            Message::DoViewChange(DoViewChange {
                v: View::from(1),
                t: table,
                l: vec![],
                k: OpNumber::from(0),
                i: 2
            })
        );
        assert_eq!(from, replicas[2].address());
    }

    fn build_replicas<FD: FailureDetector + Clone + Debug>(failure_detector: FD) -> Vec<Replica<impl Service, FD, impl IdleDetector>> {
        ReplicaBuilder::new()
            .with_replica("127.0.0.1:3001".parse().unwrap())
            .with_replica("127.0.0.1:3002".parse().unwrap())
            .with_replica("127.0.0.1:3003".parse().unwrap())
            .with_commit_queue_threshold(10)
            .with_prepare_multiplier(2)
            .with_service(0usize)
            .with_idle_detector(false)
            .with_failure_detector(failure_detector)
            .build()
            .unwrap()
    }

    fn poll(network: &mut Network, replica: &mut Replica<impl Service, impl FailureDetector, impl IdleDetector>) {
        replica.poll(network.receive(replica.address()).ok(), network)
    }
}
