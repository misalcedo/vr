use crate::client_table::ClientTable;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use crate::health::{HealthDetector, HealthStatus};
use crate::identifiers::ReplicaIdentifier;
use crate::mailbox::{Address, Mailbox};
use crate::model::{
    ConcurrentRequest, DoViewChange, Message, OutdatedRequest, Payload, Prepare, PrepareOk, Reply,
    Request, StartView,
};
use crate::service::Service;
use crate::stamps::{OpNumber, View};
use crate::state::State;

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
enum Status {
    #[default]
    Normal,
    ViewChange,
    Recovering,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NonVolatileState {
    identifier: ReplicaIdentifier,
    latest_view: Option<View>
}

impl From<ReplicaIdentifier> for NonVolatileState {
    fn from(identifier: ReplicaIdentifier) -> Self {
        Self { identifier, latest_view: None }
    }
}

impl NonVolatileState {
    pub fn new(identifier: ReplicaIdentifier, view: View) -> Self {
        Self { identifier, latest_view: Some(view) }
    }
}

#[derive(Debug)]
pub struct Replica<NS, S, HD> {
    /// Non-volatile state for the replica. When a replica crashes, it must maintain this state.
    state: NS,
    /// The service code for processing committed client requests.
    service: S,
    /// Detects whether the current primary is failed or idle.
    health_detector: HD,
    /// The identifier of this replica within the group.
    identifier: ReplicaIdentifier,
    /// The current view.
    view: View,
    /// The latest op-number received by this replica.
    op_number: OpNumber,
    /// This is an array containing op-number entries.
    /// The entries contain the requests that have been received so far in their assigned order.
    log: Vec<Request>,
    /// The last operation number committed in the current view.
    committed: OpNumber,
    /// The count of operations executed in the current view.
    executed: OpNumber,
    /// This records for each client the number of its most recent request, plus, if the request has been executed, the result sent for that request.
    client_table: ClientTable,
    /// The current status, either normal, view-change, or recovering.
    status: Status,
}

impl<NS, S, HD> Replica<NS, S, HD>
where
    NS: State<NonVolatileState>,
    S: Service,
    HD: HealthDetector,
{
    pub fn new(mut state: NS, service: S, health_detector: HD) -> Self {
        let saved_state = state.load();
        let identifier = saved_state.identifier;
        let view = saved_state.latest_view.unwrap_or_default();

        Self {
            state,
            service,
            health_detector,
            identifier,
            view,
            op_number: Default::default(),
            log: Default::default(),
            committed: Default::default(),
            executed: Default::default(),
            client_table: Default::default(),
            status: saved_state.latest_view.map(|_| Status::Recovering).unwrap_or_default(),
        }
    }

    pub fn state(self) -> NS {
        self.state
    }

    pub fn identifier(&self) -> ReplicaIdentifier {
        self.identifier
    }

    pub fn view(&self) -> View {
        self.view
    }

    pub fn poll(&mut self, mailbox: &mut Mailbox) {
        let primary = self.identifier.primary(self.view);

        self.inform_outdated(mailbox);

        if primary == self.identifier {
            self.poll_primary(mailbox);
        } else {
            self.poll_replica(mailbox);
        }
    }

    fn inform_outdated(&mut self, mailbox: &mut Mailbox) {
        mailbox.select_all(|sender, message| {
            if message.view < self.view {
                sender.send(message.from, self.view, Payload::OutdatedView);
                None
            } else {
                Some(message)
            }
        });
    }

    fn poll_primary(&mut self, mailbox: &mut Mailbox) {
        match self.status {
            Status::Normal => self.process_normal_primary(mailbox),
            Status::ViewChange => self.process_view_change_primary(mailbox),
            Status::Recovering => (),
        }
    }

    fn process_normal_primary(&mut self, mailbox: &mut Mailbox) {
        let mut prepared: HashMap<OpNumber, HashSet<Address>> = HashMap::new();

        mailbox.select(|sender, message| match message {
            Message {
                payload: Payload::Request(request),
                ..
            } => {
                let cached_request = self.client_table.get(&request);

                match cached_request {
                    None => {
                        // this is the first request from the client.
                        self.client_table.start(&request);
                        self.prepare_primary(sender, request);
                    }
                    Some(last_request) => {
                        match last_request.partial_cmp(&request) {
                            None => {
                                // got a newer request from the client.
                                self.client_table.start(&request);
                                self.prepare_primary(sender, request);
                            }
                            Some(Ordering::Less) => sender.send(
                                request.c,
                                self.view,
                                ConcurrentRequest {
                                    s: last_request.request(),
                                },
                            ),
                            Some(Ordering::Equal) => match last_request.reply() {
                                None => {
                                    // the client resent the latest request.
                                    // we do not want to re-broadcast here to avoid the client being able to overwhelm the network.
                                }
                                // send back a cached response for latest request from the client.
                                Some(reply) => sender.send(request.c, self.view, reply),
                            },
                            Some(Ordering::Greater) => sender.send(
                                request.c,
                                self.view,
                                OutdatedRequest {
                                    s: last_request.request(),
                                },
                            ),
                        }
                    }
                }

                None
            }
            ref message @ Message {
                from,
                payload: Payload::PrepareOk(prepare_ok),
                ..
            } => {
                if self.committed >= prepare_ok.n {
                    None
                } else {
                    let replication = prepared.entry(prepare_ok.n).or_insert_with(HashSet::new);

                    replication.insert(from);

                    if replication.len() >= self.identifier.sub_majority() {
                        self.committed = self.committed.max(prepare_ok.n);
                        self.execute_primary(sender);

                        None
                    } else {
                        Some(message.clone())
                    }
                }
            }
            _ => Some(message),
        });

        if self.health_detector.detect(self.view, self.identifier) >= HealthStatus::Suspect {
            mailbox.broadcast(self.view, Payload::Ping);
        }
    }

    fn process_view_change_primary(&mut self, mailbox: &mut Mailbox) {
        let mut replicas = HashSet::new();

        mailbox.visit(|message| {
            if let Message {
                from: Address::Replica(replica),
                payload: Payload::DoViewChange(_),
                ..
            } = message
            {
                replicas.insert(*replica);
            }
        });

        let quorum = self.identifier.sub_majority() + 1;

        if replicas.len() >= quorum {
            mailbox.select_all(|_, message| match message {
                Message {
                    payload: Payload::DoViewChange(do_view_change),
                    ..
                } => {
                    if do_view_change.l.len() > self.log.len() {
                        self.replace_log(do_view_change.k, do_view_change.l);
                    }

                    None
                }
                _ => Some(message),
            });

            self.status = Status::Normal;
            self.state.save(NonVolatileState::new(self.identifier, self.view));

            mailbox.broadcast(
                self.view,
                StartView {
                    l: self.log.clone(),
                    k: self.committed,
                },
            );

            self.execute_primary(mailbox);
        }
    }

    fn poll_replica(&mut self, mailbox: &mut Mailbox) {
        match self.status {
            Status::Normal => self.process_normal_replica(mailbox),
            Status::ViewChange => self.process_view_change_replica(mailbox),
            Status::Recovering => (),
        }
    }

    fn process_normal_replica(&mut self, mailbox: &mut Mailbox) {
        let next_op = self.op_number.next();

        mailbox.select(|sender, message| match message {
            Message {
                from: Address::Replica(_),
                payload: Payload::Ping,
                ..
            } => None,
            Message {
                from: Address::Replica(_),
                payload: Payload::Prepare(prepare),
                ..
            } if next_op == prepare.n => {
                self.client_table.start(&prepare.m);
                self.push_request(prepare.m);

                let primary = self.identifier.primary(self.view);

                sender.send(primary, self.view, PrepareOk { n: self.op_number });
                self.committed = self.committed.max(prepare.k);
                self.execute_replica();

                None
            }
            // TODO: perform state transfer if necessary to get missing information.
            _ => Some(message),
        });

        if self.health_detector.detect(self.view, self.identifier) >= HealthStatus::Unhealthy {
            self.view.increment();
            self.status = Status::ViewChange;
            self.state.save(NonVolatileState::new(self.identifier, self.view));

            mailbox.send(
                self.identifier.primary(self.view),
                self.view,
                DoViewChange {
                    l: self.log.clone(),
                    k: self.committed,
                },
            );
        }
    }

    fn process_view_change_replica(&mut self, mailbox: &mut Mailbox) {
        mailbox.select(|_, message| match message {
            Message {
                from: Address::Replica(_),
                view,
                payload: Payload::StartView(start_view),
                ..
            } => {
                self.view = view;
                self.status = Status::Normal;
                self.replace_log(start_view.k, start_view.l);

                None
            }
            _ => Some(message),
        });

        let mut current = self.committed.next();

        while current <= self.op_number {
            mailbox.send(
                self.identifier.primary(self.view),
                self.view,
                Payload::PrepareOk(PrepareOk { n: current }),
            );
            current.increment();
        }
    }

    // Push a request to the end of the log and increment the op-number.
    fn push_request(&mut self, request: Request) {
        self.op_number.increment();
        self.log.push(request);
    }

    fn prepare_primary(&mut self, sender: &mut Mailbox, request: Request) {
        self.push_request(request.clone());
        sender.broadcast(
            self.view,
            Prepare {
                n: self.op_number,
                m: request,
                k: self.committed,
            },
        );
    }

    fn execute_primary(&mut self, mailbox: &mut Mailbox) {
        let length = OpNumber::new(self.log.len());

        while self.committed > self.executed && self.executed < length {
            // executed must be incremented after indexing the log to avoid panicking.
            let request = &self.log[self.executed.as_usize()];
            let payload = self.service.invoke(request.op.as_slice());
            let reply = Reply {
                s: request.s,
                x: payload,
            };

            self.client_table.set(request, reply.clone());
            self.executed.increment();
            mailbox.send(request.c, self.view, reply);
        }
    }

    fn replace_log(&mut self, committed: OpNumber, log: Vec<Request>) {
        self.committed = committed;
        self.log = log;
        self.op_number = OpNumber::new(self.log.len());

        for in_progress in self.committed.as_usize()..self.op_number.as_usize() {
            self.client_table.start(&self.log[in_progress])
        }
    }

    fn execute_replica(&mut self) {
        let length = OpNumber::new(self.log.len());
        while self.committed > self.executed && self.executed < length {
            // executed must be incremented after indexing the log to avoid panicking.
            let request = &self.log[self.executed.as_usize()];
            let payload = self.service.invoke(request.op.as_slice());
            let reply = Reply {
                s: request.s,
                x: payload,
            };

            self.client_table.set(request, reply);
            self.executed.increment();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::client::Client;
    use crate::client_table::CachedRequest;
    use crate::driver::{Driver, LocalDriver};
    use crate::health::{HealthStatus, LocalHealthDetector, Suspect};
    use crate::identifiers::GroupIdentifier;
    use crate::model::OutdatedRequest;
    use crate::state::LocalState;

    use super::*;

    #[test]
    fn base_case() {
        let group = GroupIdentifier::new(3);
        let mut driver: LocalDriver<usize, HealthStatus> = LocalDriver::new(group);
        let mut client = Client::new(group);

        let operation = b"Hello, world!";
        let primary = group.primary(client.view());

        for i in 1..=3 {
            let request = client.new_message(operation);

            driver.deliver(request);
            driver.drive(Some(primary));
            driver.drive(group.replicas(client.view()));
            driver.drive(Some(primary));

            let messages = driver.fetch(client.identifier());

            // process 2nd prepareOk
            driver.drive(Some(primary));

            let reply = reply_message(&client, operation, i);

            assert_eq!(messages, vec![reply]);
        }

        let (_, mut mailbox) = driver.take(primary).unwrap();
        let inbound: Vec<Message> = mailbox.drain_inbound().collect();
        let outbound: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(inbound, vec![]);
        assert_eq!(outbound, vec![]);
    }

    #[test]
    fn committed_survive_view_change() {
        let requests = 3;
        let group = GroupIdentifier::new(requests);
        let mut health_detector = LocalHealthDetector::default();
        let mut driver: LocalDriver<usize, LocalHealthDetector> =
            LocalDriver::with_health_detector(group, &health_detector);
        let mut client = Client::new(group);
        let view = View::default();

        let operation = b"Hello, world!";
        let primary = group.primary(client.view());

        for _ in 1..=requests {
            let request = client.new_message(operation);

            driver.deliver(request);
            driver.drive_to_empty(group);
            driver.fetch(client.identifier());
        }

        // unprepared requests do not survive view changes.
        let unprepared_request = client.new_message(operation);
        driver.deliver(unprepared_request.clone());

        driver.crash(Some(primary));
        health_detector.set_status(primary, HealthStatus::Unhealthy);
        driver.drive_to_empty(group.replicas(view));

        let (primary, mut mailbox) = driver.take(group.primary(view.next())).unwrap();
        let inbound: Vec<Message> = mailbox.drain_inbound().collect();
        let outbound: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(primary.log.len(), requests);
        assert_eq!(primary.committed, OpNumber::new(requests - 1)); // replicas never learned of last commit
        assert_eq!(inbound, vec![]);
        assert_eq!(outbound, vec![]);
    }

    #[test]
    fn client_table_survives_view_change() {
        let requests = 3;
        let group = GroupIdentifier::new(requests);
        let mut health_detector = LocalHealthDetector::default();
        let mut driver: LocalDriver<usize, LocalHealthDetector> =
            LocalDriver::with_health_detector(group, &health_detector);
        let mut client = Client::new(group);
        let mut clone = client.clone();
        let view = View::default();

        let operation = b"Hello, world!";
        let primary = group.primary(client.view());

        for _ in 1..=requests {
            let request = client.new_message(operation);

            driver.deliver(request);
            driver.drive_to_empty(group);
            driver.fetch(client.identifier());
        }

        clone.set_view(view.next());
        let first_request = clone.new_message(operation);

        driver.crash(Some(primary));
        health_detector.set_status(primary, HealthStatus::Unhealthy);

        driver.drive_to_empty(group.replicas(view));
        driver.deliver(client.broadcast(operation));
        driver.drive_to_empty(group);

        let new_view = driver
            .fetch(client.identifier())
            .into_iter()
            .filter(|m| m.payload == Payload::OutdatedView)
            .map(|m| m.view)
            .max()
            .unwrap_or_default();
        client.set_view(new_view);

        driver.deliver(first_request);
        driver.drive_to_empty(group);

        let messages: Vec<OutdatedRequest> = driver
            .fetch(client.identifier())
            .into_iter()
            .map(Message::payload::<OutdatedRequest>)
            .map(Result::unwrap)
            .collect();

        assert_eq!(
            messages,
            vec![OutdatedRequest {
                s: client.last_request()
            }]
        );

        driver.deliver(client.message(operation));
        driver.drive_to_empty(group);

        // re-delivers latest reply.
        let messages: Vec<Message> = driver.fetch(client.identifier());
        assert_eq!(messages, vec![reply_message(&client, operation, requests)]);

        driver.deliver(client.new_message(operation));
        driver.drive_to_empty(group);

        // can still process new requests
        let messages: Vec<Message> = driver.fetch(client.identifier());
        assert_eq!(
            messages,
            vec![reply_message(&client, operation, requests + 1)]
        );

        let (primary, mut mailbox) = driver.take(group.primary(view.next())).unwrap();
        let inbound: Vec<Message> = mailbox.drain_inbound().collect();
        let outbound: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(primary.log.len(), requests + 1);
        assert_eq!(primary.committed, OpNumber::new(requests + 1));
        assert_eq!(inbound, vec![]);
        assert_eq!(outbound, vec![]);
    }

    #[test]
    fn base_case_f_crashed() {
        let f = 2;
        let group = GroupIdentifier::new((2 * f) + 1);
        let mut driver: LocalDriver<usize, HealthStatus> = LocalDriver::new(group);
        let mut client = Client::new(group);

        let operation = b"Hello, world!";
        let primary = group.primary(client.view());

        let request = client.new_message(operation);

        driver.crash(group.replicas(client.view()).take(f));
        driver.deliver(request);
        driver.drive_to_empty(group);

        let messages = driver.fetch(client.identifier());
        let reply = reply_message(&client, operation, 1);

        assert_eq!(messages, vec![reply]);

        let (_, mut mailbox) = driver.take(primary).unwrap();
        let inbound: Vec<Message> = mailbox.drain_inbound().collect();
        let outbound: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(inbound, vec![]);
        assert_eq!(outbound, vec![]);
    }

    #[test]
    fn base_case_f_plus_1_crashed() {
        let f = 2;
        let group = GroupIdentifier::new((2 * f) + 1);
        let mut driver: LocalDriver<usize, HealthStatus> = LocalDriver::new(group);
        let mut client = Client::new(group);

        let operation = b"Hello, world!";
        let primary = group.primary(client.view());

        let request = client.new_message(operation);

        driver.crash(group.replicas(client.view()).take(f + 1));
        driver.deliver(request);

        // A normal request take 4 messages to get a reply: request, prepare, prepareOk, reply.
        for _ in 0..=4 {
            driver.drive(group);
        }

        let messages = driver.fetch(client.identifier());

        assert_eq!(messages, vec![]);

        let (_, mut mailbox) = driver.take(primary).unwrap();
        let inbound = mailbox
            .drain_inbound()
            .map(Message::payload::<PrepareOk>)
            .map(Result::unwrap)
            .count();
        let outbound: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(inbound, f - 1);
        assert_eq!(outbound, vec![]);
    }

    #[test]
    fn single_client_concurrent_requests() {
        let group = GroupIdentifier::new(3);
        let mut driver: LocalDriver<usize, HealthStatus> = LocalDriver::new(group);
        let mut client = Client::new(group);

        let operation = b"Hello, world!";
        let primary = group.primary(client.view());
        let old_request = client.new_message(operation);
        let last_request = client.last_request();
        let new_request = client.new_message(operation);

        driver.deliver(old_request);
        driver.deliver(new_request);
        driver.drive(Some(primary));
        driver.drive(Some(primary));

        let messages = driver.fetch(client.identifier());
        let response = Message {
            from: primary.into(),
            to: client.address(),
            view: client.view(),
            payload: ConcurrentRequest { s: last_request }.into(),
        };

        assert_eq!(messages, vec![response]);
    }

    #[test]
    fn suspect_primary_sends_pings() {
        let group = GroupIdentifier::new(3);
        let view = View::default();
        let primary = group.primary(view);
        let mut driver: LocalDriver<usize, Suspect> = LocalDriver::new(group);

        driver.drive(Some(primary));

        let identifier = group.replicas(view).next().unwrap();
        let (replica, mut mailbox) = driver.take(identifier).unwrap();
        let messages: Vec<Message> = mailbox.drain_inbound().collect();

        assert_eq!(messages, vec![ping_message(&replica)]);
    }

    #[test]
    fn client_resends_are_outdated_request() {
        let group = GroupIdentifier::new(3);
        let mut driver: LocalDriver<usize, HealthStatus> = LocalDriver::new(group);
        let mut client = Client::new(group);
        let mut clone = client.clone();

        let operation = b"Hello, world!";
        let primary = group.primary(client.view());

        for _ in 1..=2 {
            let request = client.new_message(operation);

            driver.deliver(request);
            driver.drive_to_empty(group);
            driver.fetch(client.identifier());
        }

        let request = clone.new_message(operation);

        driver.deliver(request);
        driver.drive_to_empty(group);

        let message = driver.fetch(clone.identifier());
        let (primary, mut mailbox) = driver.take(primary).unwrap();
        let inbound: Vec<Message> = mailbox.drain_inbound().collect();
        let outbound: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(inbound, vec![]);
        assert_eq!(outbound, vec![]);
        assert_eq!(message, vec![outdated_request_message(&primary, &client)]);
    }

    #[test]
    fn replica_prepares_next_op() {
        let group = GroupIdentifier::new(3);
        let mut driver: LocalDriver<usize, HealthStatus> = LocalDriver::new(group);
        let mut client = Client::new(group);

        let operation = b"Hello, world!";
        let primary = group.primary(client.view());
        let request = client.new_message(operation);

        driver.deliver(request);
        driver.drive(Some(primary));
        driver.drive(group.replicas(client.view()));

        let (primary, mut mailbox) = driver.take(primary).unwrap();
        let inbound: Vec<PrepareOk> = mailbox
            .drain_inbound()
            .map(Message::payload::<PrepareOk>)
            .map(Result::unwrap)
            .collect();
        let outbound: Vec<Message> = mailbox.drain_outbound().collect();
        let prepare_ok = PrepareOk {
            n: primary.op_number,
        };

        assert_eq!(inbound, vec![prepare_ok, prepare_ok]);
        assert_eq!(outbound, vec![]);
    }

    #[test]
    fn replica_discards_ping() {
        let group = GroupIdentifier::new(3);
        let view = View::default();
        let primary = group.primary(view);
        let mut driver: LocalDriver<usize, Suspect> = LocalDriver::new(group);

        driver.drive(Some(primary));
        driver.drive(group.replicas(view));

        for replica in group.replicas(view) {
            assert!(driver.is_empty(replica));
        }
    }

    #[test]
    fn prepare_replica_outdated() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.into_iter().collect();

        let mut primary = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[0])),
            0,
            HealthStatus::Normal,
        );
        let mut client = Client::new(group);

        let mut replica = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[1])),
            0,
            HealthStatus::Normal,
        );
        let mut mailbox = Mailbox::from(replica.identifier);

        simulate_requests(&mut primary, vec![&mut client], operation);

        let message = prepare_message(&primary, &client, operation);

        replica.view.increment();
        replica.view.increment();

        mailbox.deliver(message);
        replica.poll(&mut mailbox);

        let messages: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(
            messages,
            vec![Message {
                from: replica.identifier.into(),
                to: primary.identifier.into(),
                view: replica.view,
                payload: Payload::OutdatedView,
            }]
        );
    }

    #[test]
    fn prepare_replica_committed() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.into_iter().collect();

        let mut primary = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[0])),
            0,
            HealthStatus::Normal,
        );
        let mut client = Client::new(group);
        let mut replica = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[1])),
            0,
            HealthStatus::Normal,
        );
        let mut mailbox = Mailbox::from(replica.identifier);

        simulate_reply(&mut primary, &mut replica, &mut client, 1);
        simulate_requests(&mut primary, vec![&mut client], operation);

        assert_eq!(replica.service, 0);

        mailbox.deliver(prepare_message(&primary, &client, operation));
        replica.poll(&mut mailbox);

        let messages: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(messages, vec![prepare_ok_message(&primary, &replica)]);
        assert_eq!(replica.service, operation.len());
    }

    #[test]
    fn prepare_replica_committed_not_in_log() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.into_iter().collect();

        let mut primary = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[0])),
            0,
            HealthStatus::Normal,
        );
        let mut client1 = Client::new(group);
        let mut client2 = Client::new(group);

        let mut replica = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[1])),
            0,
            HealthStatus::Normal,
        );
        let mut mailbox = Mailbox::from(replica.identifier);

        simulate_requests(&mut primary, vec![&mut client1, &mut client2], operation);
        primary.committed.increment();

        let message = prepare_message(&primary, &client2, operation);

        mailbox.deliver(message);
        replica.poll(&mut mailbox);

        assert_eq!(mailbox.drain_outbound().count(), 0);
        assert_eq!(replica.service, 0);
    }

    #[test]
    fn prepare_replica_buffered() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.into_iter().collect();

        let mut primary = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[0])),
            0,
            HealthStatus::Normal,
        );
        let mut client1 = Client::new(group);
        let mut client2 = Client::new(group);

        let mut replica = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[1])),
            0,
            HealthStatus::Normal,
        );
        let mut mailbox = Mailbox::from(replica.identifier);

        simulate_requests(&mut primary, vec![&mut client1, &mut client2], operation);

        let message = prepare_message(&primary, &client2, operation);

        mailbox.deliver(message);
        replica.poll(&mut mailbox);

        assert_eq!(mailbox.drain_outbound().count(), 0);
    }

    #[test]
    fn prepare_ok_primary() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(5);
        let replicas: Vec<ReplicaIdentifier> = group.into_iter().collect();

        let mut primary = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[0])),
            0,
            HealthStatus::Normal,
        );
        let mut replica1 = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[1])),
            0,
            HealthStatus::Normal,
        );
        let mut replica2 = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[2])),
            0,
            HealthStatus::Normal,
        );
        let mut client = Client::new(group);
        let mut mailbox = simulate_requests(&mut primary, vec![&mut client], operation);

        simulate_broadcast(&mut mailbox, vec![&mut replica1, &mut replica2]);

        let message1 = prepare_ok_message(&primary, &replica1);
        mailbox.deliver(message1);
        primary.poll(&mut mailbox);
        assert_eq!(mailbox.drain_outbound().count(), 0);

        let message2 = prepare_ok_message(&primary, &replica2);
        mailbox.deliver(message2);
        primary.poll(&mut mailbox);

        let messages: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(messages, vec![reply_message(&client, operation, 1)]);
        assert_eq!(primary.service, operation.len());
        assert_eq!(
            primary
                .client_table
                .get(&client.request(operation))
                .and_then(CachedRequest::reply),
            Some(reply_payload(&client, operation, 1))
        );
    }

    #[test]
    fn prepare_ok_primary_skipped() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(5);
        let replicas: Vec<ReplicaIdentifier> = group.into_iter().collect();

        let mut primary = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[0])),
            0,
            HealthStatus::Normal,
        );
        let mut replica1 = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[1])),
            0,
            HealthStatus::Normal,
        );
        let mut replica2 = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[2])),
            0,
            HealthStatus::Normal,
        );
        let mut client1 = Client::new(group);
        let mut client2 = Client::new(group);
        let mut mailbox = Mailbox::from(primary.identifier);

        mailbox.deliver(client1.new_message(operation));
        primary.poll(&mut mailbox);
        mailbox.deliver(client2.new_message(operation));
        primary.poll(&mut mailbox);

        simulate_broadcast(&mut mailbox, vec![&mut replica1, &mut replica2]);

        let message1 = prepare_ok_message(&primary, &replica1);
        let message2 = prepare_ok_message(&primary, &replica2);

        mailbox.deliver(message1);
        mailbox.deliver(message2);
        primary.poll(&mut mailbox);

        let messages: Vec<Message> = mailbox.drain_outbound().collect();

        // backtrack the request id to build the replies correctly.
        let replies = vec![
            reply_message(&client1, operation, 1),
            reply_message(&client2, operation, 2),
        ];

        assert_eq!(messages, replies);
        assert_eq!(primary.service, operation.len() * 2);
    }

    #[test]
    fn client_resend_in_progress() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.into_iter().collect();

        let mut primary = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[0])),
            0,
            HealthStatus::Normal,
        );
        let mut client = Client::new(group);

        let mut mailbox = simulate_requests(&mut primary, vec![&mut client], operation);

        assert_eq!(mailbox.drain_outbound().count(), 1);

        mailbox.deliver(client.message(operation));
        primary.poll(&mut mailbox);

        assert_eq!(mailbox.drain_outbound().count(), 0);
        assert_eq!(
            primary
                .client_table
                .get(&client.request(operation))
                .unwrap()
                .reply(),
            None
        );
    }

    #[test]
    fn client_resend_finished() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.into_iter().collect();

        let mut primary = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[0])),
            0,
            HealthStatus::Normal,
        );
        let mut replica = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[1])),
            0,
            HealthStatus::Normal,
        );
        let mut client = Client::new(group);
        let mut mailbox = simulate_requests(&mut primary, vec![&mut client], operation);

        simulate_broadcast(&mut mailbox, vec![&mut replica]);
        mailbox.deliver(prepare_ok_message(&primary, &replica));
        primary.poll(&mut mailbox);

        let messages: Vec<Message> = mailbox.drain_outbound().collect();
        assert_eq!(messages, vec![reply_message(&client, operation, 1)]);

        mailbox.deliver(client.message(operation));
        primary.poll(&mut mailbox);

        let messages: Vec<Message> = mailbox.drain_outbound().collect();
        assert_eq!(messages, vec![reply_message(&client, operation, 1)]);
    }

    #[test]
    fn client_resend_finished_not_cached() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.into_iter().collect();

        let mut primary = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[0])),
            0,
            HealthStatus::Normal,
        );
        let mut replica = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[1])),
            0,
            HealthStatus::Normal,
        );
        let mut client = Client::new(group);
        let mut clone = client.clone();

        let mut mailbox = simulate_reply(&mut primary, &mut replica, &mut client, 2);

        mailbox.deliver(clone.new_message(operation));
        primary.poll(&mut mailbox);

        let messages: Vec<Message> = mailbox.drain_outbound().collect();
        assert_eq!(messages, vec![outdated_request_message(&primary, &client)]);
    }

    #[test]
    fn do_view_change_replica() {
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.into_iter().collect();

        let mut replica = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[1])),
            0,
            HealthStatus::Normal,
        );
        let mut mailbox = Mailbox::from(replica.identifier);

        replica.push_request(Request {
            op: vec![],
            c: Default::default(),
            s: Default::default(),
        });
        replica.committed.increment();
        replica.health_detector = HealthStatus::Unhealthy;

        replica.poll(&mut mailbox);

        let messages: Vec<Message> = mailbox.drain_inbound().collect();

        assert_eq!(messages, vec![do_view_change_message(&replica)]);
    }

    #[test]
    fn start_view_primary() {
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.into_iter().collect();

        let mut client = Client::new(group);
        let mut replica = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[2])),
            0,
            HealthStatus::Normal,
        );
        let mut primary = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[1])),
            0,
            HealthStatus::Normal,
        );
        let mut mailbox = Mailbox::from(primary.identifier);

        // fake a request to increment the count
        client.new_request(b"");

        primary.view.increment();
        primary.status = Status::ViewChange;

        replica.view.increment();
        replica.status = Status::ViewChange;
        replica.committed.increment();
        replica.executed.increment();
        replica.push_request(client.request(b""));

        mailbox.deliver(do_view_change_message(&primary));
        mailbox.deliver(do_view_change_message(&replica));

        primary.poll(&mut mailbox);

        let messages: Vec<Message> = mailbox.drain_outbound().collect();

        // Inform the client of the new view so the reply message we assert is correct.
        client.set_view(primary.view);

        assert_eq!(mailbox.drain_inbound().count(), 0);
        assert_eq!(
            messages,
            vec![start_view_message(&primary), reply_message(&client, &[], 1),]
        );
        assert_eq!(primary.status, Status::Normal);
        assert_eq!(primary.log, replica.log);
        assert_eq!(primary.committed, replica.committed);
        assert_eq!(primary.op_number, replica.op_number);
        assert_eq!(primary.executed, replica.executed);
    }

    #[test]
    fn start_view_primary_no_quorum() {
        let group = GroupIdentifier::new(5);
        let replicas: Vec<ReplicaIdentifier> = group.into_iter().collect();

        let mut primary = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[1])),
            0,
            HealthStatus::Normal,
        );
        let mut mailbox = Mailbox::from(primary.identifier);

        primary.view.increment();
        primary.status = Status::ViewChange;
        mailbox.deliver(do_view_change_message(&primary));

        primary.poll(&mut mailbox);

        assert_eq!(mailbox.drain_inbound().count(), 1);
        assert_eq!(mailbox.drain_outbound().count(), 0);
        assert_eq!(primary.status, Status::ViewChange);
    }

    #[test]
    fn start_view_replica() {
        let operation = b"hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.into_iter().collect();

        let mut client = Client::new(group);
        let mut primary = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[1])),
            0,
            HealthStatus::Normal,
        );
        let mut replica = Replica::new(
            LocalState::new(NonVolatileState::from(replicas[2])),
            0,
            HealthStatus::Normal,
        );
        let mut mailbox = Mailbox::from(replica.identifier);

        primary.view.increment();
        primary.committed.increment();
        primary.executed.increment();
        primary.push_request(client.new_request(operation));
        primary.push_request(client.new_request(operation));

        replica.status = Status::ViewChange;
        mailbox.deliver(start_view_message(&primary));
        replica.poll(&mut mailbox);

        let messages: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(messages, vec![prepare_ok_message(&primary, &replica)]);
        assert_eq!(replica.status, Status::Normal);
        assert_eq!(replica.log, primary.log);
        assert_eq!(replica.op_number, primary.op_number);
        assert_eq!(replica.executed, OpNumber::default());
        assert_eq!(replica.committed, primary.committed);
    }

    fn do_view_change_message<NS, S, H>(replica: &Replica<NS, S, H>) -> Message {
        let payload = DoViewChange {
            l: replica.log.clone(),
            k: replica.committed,
        }
        .into();

        Message {
            from: replica.identifier.into(),
            to: replica.identifier.primary(replica.view).into(),
            view: replica.view,
            payload,
        }
    }

    fn simulate_broadcast<NS: State<NonVolatileState>, S: Service, H: HealthDetector>(
        source: &mut Mailbox,
        replicas: Vec<&mut Replica<NS, S, H>>,
    ) {
        let messages: Vec<Message> = source.drain_outbound().collect();

        for replica in replicas {
            let mut mailbox = Mailbox::from(replica.identifier);

            for message in messages.iter() {
                mailbox.deliver(message.clone());
                replica.poll(&mut mailbox);
            }
        }
    }

    fn simulate_requests<NS: State<NonVolatileState>, S: Service, H: HealthDetector>(
        primary: &mut Replica<NS, S, H>,
        clients: Vec<&mut Client>,
        operation: &[u8],
    ) -> Mailbox {
        let mut mailbox = Mailbox::from(primary.identifier);

        for client in clients {
            mailbox.deliver(client.new_message(operation));
            primary.poll(&mut mailbox);
        }

        mailbox
    }

    fn prepare_message<NS, S, H>(
        replica: &Replica<NS, S, H>,
        client: &Client,
        operation: &[u8],
    ) -> Message {
        Message {
            from: replica.identifier.into(),
            to: replica.identifier.group().into(),
            view: replica.view,
            payload: Prepare {
                n: replica.op_number,
                m: client.request(operation),
                k: replica.committed,
            }
            .into(),
        }
    }

    fn prepare_ok_message<NS, S, H>(
        primary: &Replica<NS, S, H>,
        replica: &Replica<NS, S, H>,
    ) -> Message {
        Message {
            from: replica.identifier.into(),
            to: primary.identifier.into(),
            view: replica.view,
            payload: PrepareOk {
                n: replica.op_number,
            }
            .into(),
        }
    }

    fn reply_message(client: &Client, operation: &[u8], times: usize) -> Message {
        Message {
            from: client.primary().into(),
            to: client.address(),
            view: client.view(),
            payload: reply_payload(client, operation, times).into(),
        }
    }

    fn reply_payload(client: &Client, operation: &[u8], times: usize) -> Reply {
        Reply {
            x: (operation.len() * times).to_be_bytes().to_vec(),
            s: client.last_request(),
        }
    }

    fn ping_message<NS, S, H>(replica: &Replica<NS, S, H>) -> Message {
        Message {
            from: replica.identifier.primary(replica.view).into(),
            to: replica.identifier.group().into(),
            view: replica.view,
            payload: Payload::Ping,
        }
    }

    fn start_view_message<NS, S, H>(primary: &Replica<NS, S, H>) -> Message {
        Message {
            from: primary.identifier.into(),
            to: primary.identifier.group().into(),
            view: primary.view,
            payload: Payload::StartView(StartView {
                l: primary.log.clone(),
                k: primary.committed,
            }),
        }
    }

    fn outdated_request_message<NS, S, H>(primary: &Replica<NS, S, H>, client: &Client) -> Message {
        Message {
            from: primary.identifier.into(),
            to: client.address(),
            view: primary.view,
            payload: Payload::OutdatedRequest(OutdatedRequest {
                s: client.last_request(),
            }),
        }
    }

    fn simulate_reply<NS: State<NonVolatileState>, S: Service, H: HealthDetector>(
        primary: &mut Replica<NS, S, H>,
        replica: &mut Replica<NS, S, H>,
        client: &mut Client,
        times: usize,
    ) -> Mailbox {
        let operation = b"Hi!";
        let mut mailbox = Mailbox::from(primary.identifier);

        for i in 1..=times {
            mailbox.deliver(client.new_message(operation));
            primary.poll(&mut mailbox);

            simulate_broadcast(&mut mailbox, vec![replica]);

            mailbox.deliver(prepare_ok_message(&primary, &replica));
            primary.poll(&mut mailbox);

            let messages: Vec<Message> = mailbox.drain_outbound().collect();

            assert_eq!(messages, vec![reply_message(&client, operation, i)]);
        }

        mailbox
    }
}
