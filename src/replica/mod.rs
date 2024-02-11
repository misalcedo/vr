use crate::client_table::ClientTable;

use crate::health::HealthDetector;
use crate::identifiers::ReplicaIdentifier;
use crate::mailbox::Mailbox;
use crate::model::{Payload, Prepare, PrepareOk, Reply, Request};
use crate::service::Service;
use crate::stamps::{OpNumber, View};
use crate::state::State;
use backup::Backup;
use primary::Primary;

mod backup;
mod primary;

trait Role {
    fn process_normal(&mut self, mailbox: &mut Mailbox);

    fn process_view_change(&mut self, mailbox: &mut Mailbox);

    fn process_recovering(&mut self, mailbox: &mut Mailbox);
}

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
    latest_view: Option<View>,
}

impl From<ReplicaIdentifier> for NonVolatileState {
    fn from(identifier: ReplicaIdentifier) -> Self {
        Self {
            identifier,
            latest_view: None,
        }
    }
}

impl NonVolatileState {
    pub fn new(identifier: ReplicaIdentifier, view: View) -> Self {
        Self {
            identifier,
            latest_view: Some(view),
        }
    }

    fn view(&self) -> View {
        match self.latest_view {
            None => View::default(),
            Some(view) if self.identifier.is_primary(view) => view.next(),
            Some(view) => view,
        }
    }

    fn status(&self) -> Status {
        match self.latest_view {
            None => Status::Normal,
            Some(_) => Status::Recovering,
        }
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
        let view = saved_state.view();
        let status = saved_state.status();

        state.save(NonVolatileState::new(identifier, view));

        Self {
            state,
            service,
            health_detector,
            identifier,
            view,
            op_number: Default::default(),
            log: Default::default(),
            committed: Default::default(),
            client_table: Default::default(),
            status,
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
        let status = self.status;

        self.inform_outdated(mailbox);

        let mut role: Box<dyn Role> = if primary == self.identifier {
            Box::new(Primary(self))
        } else {
            Box::new(Backup(self))
        };

        match status {
            Status::Normal => role.process_normal(mailbox),
            Status::ViewChange => role.process_view_change(mailbox),
            Status::Recovering => role.process_recovering(mailbox),
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

    fn execute_committed(&mut self, committed: OpNumber, mut mailbox: Option<&mut Mailbox>) {
        while self.committed < committed {
            match self.log.get(self.committed.as_usize()) {
                None => break,
                Some(request) => {
                    let payload = self.service.invoke(request.op.as_slice());
                    let reply = Reply {
                        s: request.s,
                        x: payload,
                    };

                    if let Some(mailbox) = mailbox.as_mut() {
                        mailbox.send(request.c, self.view, reply.clone());
                    }

                    self.client_table.set(request, reply);
                    self.committed.increment();
                }
            }
        }
    }

    fn replace_log(&mut self, log: Vec<Request>) {
        self.log = log;
        self.op_number = OpNumber::new(self.log.len());
    }

    fn prepare_ok_uncommitted(&mut self, mailbox: &mut Mailbox) {
        let mut current = self.committed;

        while current < self.op_number {
            current.increment();
            mailbox.send(
                self.identifier.primary(self.view),
                self.view,
                Payload::PrepareOk(PrepareOk { n: current }),
            );
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
    use crate::model::{Commit, ConcurrentRequest, DoViewChange, Message, PrepareOk, StartView};
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
        assert_eq!(primary.committed, OpNumber::new(requests));
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

        for i in 1..=requests {
            let request = client.new_message(operation);

            driver.deliver(request);
            driver.drive_to_empty(group);

            assert_eq!(
                driver.fetch(client.identifier()),
                vec![reply_message(&client, operation, i)]
            );
        }

        clone.set_view(view.next());
        let first_request = clone.new_message(operation);

        driver.crash(Some(primary));
        health_detector.set_status(primary, HealthStatus::Unhealthy);

        // trigger view change without a message
        driver.drive(group.replicas(view));
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

        assert_eq!(driver.fetch(client.identifier()), vec![]);

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
    fn old_primaries_can_recover() {
        let requests = 5;
        let group = GroupIdentifier::new(3);
        let mut health_detector = LocalHealthDetector::default();
        let mut driver: LocalDriver<usize, LocalHealthDetector> =
            LocalDriver::with_health_detector(group, &health_detector);
        let mut client = Client::new(group);

        let operation = b"Hello, world!";
        let old_primary = group.primary(client.view());

        driver.crash(Some(old_primary));
        health_detector.set_status(old_primary, HealthStatus::Unhealthy);

        // view change
        driver.drive_to_empty(group);

        client.set_view(client.view().next());

        for i in 1..=requests {
            driver.deliver(client.new_message(operation));
            driver.drive_to_empty(group);

            assert_eq!(
                driver.fetch(client.identifier()),
                vec![reply_message(&client, operation, i)]
            );
        }

        driver.recover_with_detector(Some(old_primary), &health_detector);
        health_detector.set_status(old_primary, HealthStatus::Normal);

        // old primary recovery
        driver.drive(Some(old_primary));
        driver.drive_to_empty(group);

        driver.crash(group.replicas(client.view()).last());
        health_detector.set_status(old_primary, HealthStatus::Unhealthy);

        // old primary now functions as a backup
        driver.deliver(client.new_message(operation));
        driver.drive_to_empty(group);

        assert_eq!(
            driver.fetch(client.identifier()),
            vec![reply_message(&client, operation, requests + 1)]
        );
    }

    #[test]
    fn replicas_can_recover() {
        let requests = 5;
        let group = GroupIdentifier::new(3);
        let mut driver: LocalDriver<usize, HealthStatus> = LocalDriver::new(group);
        let mut client = Client::new(group);

        let operation = b"Hello, world!";

        for i in 1..=requests {
            driver.deliver(client.new_message(operation));
            driver.drive_to_empty(group);

            assert_eq!(
                driver.fetch(client.identifier()),
                vec![reply_message(&client, operation, i)]
            );
        }

        driver.crash(group.replicas(client.view()));
        driver.recover(group.replicas(client.view()));
        driver.drive_to_empty(group);

        driver.deliver(client.new_message(operation));
        driver.drive_to_empty(group);

        assert_eq!(
            driver.fetch(client.identifier()),
            vec![reply_message(&client, operation, requests + 1)]
        );
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

        assert_eq!(messages, vec![commit_message(&replica)]);
    }

    #[test]
    fn resent_client_requests_are_dropped() {
        let group = GroupIdentifier::new(3);
        let mut driver: LocalDriver<usize, HealthStatus> = LocalDriver::new(group);
        let mut client = Client::new(group);
        let mut clone = client.clone();

        let operation = b"Hello, world!";
        let primary = group.primary(client.view());

        for _ in 1..=2 {
            driver.deliver(client.new_message(operation));
            driver.drive_to_empty(group);
            driver.fetch(client.identifier());
        }

        driver.deliver(clone.new_message(operation));
        driver.drive_to_empty(group);

        let messages = driver.fetch(client.identifier());
        let (_, mut mailbox) = driver.take(primary).unwrap();
        let inbound: Vec<Message> = mailbox.drain_inbound().collect();
        let outbound: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(inbound, vec![]);
        assert_eq!(outbound, vec![]);
        assert_eq!(messages, vec![]);
    }

    #[test]
    fn client_requests_dropped_on_backups() {
        let group = GroupIdentifier::new(3);
        let mut driver: LocalDriver<usize, HealthStatus> = LocalDriver::new(group);
        let mut client = Client::new(group);

        let operation = b"Hello, world!";
        let replica = group.replicas(client.view()).next().unwrap();

        let mut message = client.new_message(operation);

        message.to = replica.into();

        driver.deliver(message);
        driver.drive_to_empty(group);

        let messages = driver.fetch(client.identifier());
        let (_, mut mailbox) = driver.take(replica).unwrap();
        let inbound: Vec<Message> = mailbox.drain_inbound().collect();
        let outbound: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(inbound, vec![]);
        assert_eq!(outbound, vec![]);
        assert_eq!(messages, vec![]);
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

    fn commit_message<NS, S, H>(replica: &Replica<NS, S, H>) -> Message {
        Message {
            from: replica.identifier.primary(replica.view).into(),
            to: replica.identifier.group().into(),
            view: replica.view,
            payload: Commit {
                k: replica.committed,
            }
            .into(),
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
