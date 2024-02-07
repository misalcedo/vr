use crate::client_table::ClientTable;
use std::collections::{HashMap, HashSet};

use crate::health::{HealthDetector, HealthStatus};
use crate::mailbox::Mailbox;
use crate::model::{
    Address, ClientIdentifier, DoViewChange, GroupIdentifier, Message, OpNumber, Payload, Prepare,
    PrepareOk, ReplicaIdentifier, Reply, Request, RequestIdentifier, StartView, View,
};
use crate::service::Service;

// TODO: Update replica tests so the client can be moved to its own file.
pub struct Client {
    identifier: ClientIdentifier,
    view: View,
    request: RequestIdentifier,
    group: GroupIdentifier,
}

impl Client {
    pub fn new(group: GroupIdentifier) -> Self {
        Self {
            identifier: Default::default(),
            view: Default::default(),
            request: Default::default(),
            group,
        }
    }

    pub fn new_message(&mut self, payload: &[u8]) -> Message {
        self.request.increment();

        Message {
            from: self.identifier.into(),
            to: self.group.primary(self.view).into(),
            view: self.view,
            payload: self.request(payload).into(),
        }
    }

    fn request(&mut self, payload: &[u8]) -> Request {
        Request {
            op: Vec::from(payload),
            c: self.identifier,
            s: self.request,
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub enum Status {
    #[default]
    Normal,
    ViewChange,
    Recovering,
}

pub struct Replica<S, H> {
    /// The service code for processing committed client requests.
    service: S,
    /// Detects whether the current primary is failed or idle.
    health_detector: H,
    /// The identifier of this replica within the group.
    identifier: ReplicaIdentifier,
    /// The current view.
    view: View,
    /// The latest op-number received by this replica.
    op_number: OpNumber,
    /// The current status, either normal, view-change, or recovering.
    status: Status,
    /// This is an array containing op-number entries.
    /// The entries contain the requests that have been received so far in their assigned order.
    log: Vec<Request>,
    /// The last operation number committed in the current view.
    committed: OpNumber,
    /// The count of operations executed in the current view.
    executed: OpNumber,
    /// This records for each client the number of its most recent request, plus, if the request has been executed, the result sent for that request.
    client_table: ClientTable,
}

impl<S, H> Replica<S, H>
where
    S: Service,
    H: HealthDetector,
{
    pub fn new(service: S, health_detector: H, identifier: ReplicaIdentifier) -> Self {
        Self {
            service,
            health_detector,
            identifier,
            view: Default::default(),
            op_number: Default::default(),
            status: Default::default(),
            log: Default::default(),
            committed: Default::default(),
            executed: Default::default(),
            client_table: Default::default(),
        }
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
                sender.send(message.from, self.view, Payload::Outdated);
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
                self.push_request(request.clone());
                self.health_detector.notify(self.view, self.identifier);
                sender.broadcast(
                    self.view,
                    Prepare {
                        n: self.op_number,
                        m: request,
                        k: self.committed,
                    },
                );

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
            self.health_detector.notify(self.view, self.identifier);
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
                    self.committed = self.committed.max(do_view_change.k);

                    if do_view_change.l.len() > self.log.len() {
                        self.replace_log(do_view_change.l);
                    }

                    None
                }
                _ => Some(message),
            });

            self.status = Status::Normal;
            self.health_detector.notify(self.view, self.identifier);
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

    fn execute_primary(&mut self, mailbox: &mut Mailbox) {
        let length = OpNumber::from(self.log.len());

        while self.committed > self.executed && self.executed < length {
            // executed must be incremented after indexing the log to avoid panicking.
            let request = &self.log[usize::from(self.executed)];
            let payload = self.service.invoke(request.op.as_slice());
            let reply = Reply {
                s: request.s,
                x: payload,
            };

            self.client_table.set(request, &reply);
            self.executed.increment();
            mailbox.send(request.c, self.view, reply);
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
                from: Address::Replica(replica),
                payload: Payload::Ping,
                ..
            } => {
                self.health_detector.notify(self.view, replica);
                None
            }
            Message {
                from: Address::Replica(replica),
                payload: Payload::Prepare(prepare),
                ..
            } if next_op == prepare.n => {
                self.health_detector.notify(self.view, replica);
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
                from: Address::Replica(replica),
                view,
                payload: Payload::StartView(start_view),
                ..
            } => {
                self.replace_log(start_view.l);
                self.view = view;
                self.status = Status::Normal;
                self.committed = start_view.k;
                self.health_detector.notify(self.view, replica);

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

    fn replace_log(&mut self, log: Vec<Request>) {
        self.log = log;
        self.op_number = OpNumber::from(self.log.len());
    }

    fn execute_replica(&mut self) {
        let length = OpNumber::from(self.log.len());
        while self.committed > self.executed && self.executed < length {
            // executed must be incremented after indexing the log to avoid panicking.
            let request = &self.log[usize::from(self.executed)];
            self.service.invoke(request.op.as_slice());
            self.executed.increment();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::health::HealthStatus;
    use crate::model::GroupIdentifier;

    use super::*;

    #[test]
    fn request_primary() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, HealthStatus::Normal, replicas[0]);
        let mut client = Client::new(group);

        primary.health_detector = HealthStatus::Unhealthy;

        let mut mailbox = simulate_request(&mut primary, &mut client, operation, 1);

        let messages: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(
            messages,
            vec![prepare_message(&primary, &client, operation)]
        );
        assert_eq!(primary.health_detector, HealthStatus::Normal);
        assert_eq!(primary.client_table.get(&client.request(operation)), None);
    }

    #[test]
    fn request_primary_suspect() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, HealthStatus::Normal, replicas[0]);
        let mut mailbox = Mailbox::from(primary.identifier);

        primary.health_detector = HealthStatus::Suspect;
        primary.poll(&mut mailbox);

        let messages: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(messages, vec![ping_message(&primary)]);
        assert_eq!(primary.health_detector, HealthStatus::Normal);
    }

    #[test]
    fn request_primary_outdated() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, HealthStatus::Normal, replicas[0]);
        let mut client = Client::new(group);

        for _ in 1..=replicas.len() {
            primary.view.increment();
        }

        let mut mailbox = simulate_request(&mut primary, &mut client, operation, 1);

        let messages: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(
            messages,
            vec![Message {
                from: primary.identifier.into(),
                to: client.identifier.into(),
                view: primary.view,
                payload: Payload::Outdated,
            }]
        );
    }

    #[test]
    fn prepare_replica() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, HealthStatus::Normal, replicas[0]);
        let mut client = Client::new(group);

        let mut replica = Replica::new(0, HealthStatus::Normal, replicas[1]);
        let mut mailbox = Mailbox::from(replicas[1]);

        simulate_request(&mut primary, &mut client, operation, 1);

        let message = prepare_message(&primary, &client, operation);

        mailbox.deliver(message);
        replica.poll(&mut mailbox);

        let messages: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(messages, vec![prepare_ok_message(&primary, &replica)]);
        assert_eq!(replica.op_number, OpNumber::from(1));
    }

    #[test]
    fn ping_replica() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let primary = Replica::new(0, HealthStatus::Normal, replicas[0]);
        let mut replica = Replica::new(0, HealthStatus::Normal, replicas[1]);
        let mut mailbox = Mailbox::from(replicas[1]);

        let message = ping_message(&primary);

        mailbox.deliver(message);
        replica.poll(&mut mailbox);

        assert_eq!(mailbox.drain_outbound().count(), 0);
        assert_eq!(replica.health_detector, HealthStatus::Normal);
    }

    #[test]
    fn prepare_replica_outdated() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, HealthStatus::Normal, replicas[0]);
        let mut client = Client::new(group);

        let mut replica = Replica::new(0, HealthStatus::Normal, replicas[1]);
        let mut mailbox = Mailbox::from(replicas[1]);

        simulate_request(&mut primary, &mut client, operation, 1);

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
                payload: Payload::Outdated,
            }]
        );
    }

    #[test]
    fn prepare_replica_committed() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, HealthStatus::Normal, replicas[0]);
        let mut client = Client::new(group);

        let mut replica = Replica::new(0, HealthStatus::Normal, replicas[1]);
        let mut mailbox = Mailbox::from(replicas[1]);

        simulate_request(&mut primary, &mut client, operation, 1);
        let message = prepare_message(&primary, &client, operation);
        mailbox.deliver(message);
        replica.poll(&mut mailbox);
        assert_eq!(mailbox.drain_outbound().count(), 1);

        simulate_request(&mut primary, &mut client, operation, 1);
        primary.committed.increment();
        let message = prepare_message(&primary, &client, operation);
        mailbox.deliver(message);
        replica.poll(&mut mailbox);

        let messages: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(messages, vec![prepare_ok_message(&primary, &replica)]);
        assert_eq!(replica.service, operation.len());
    }

    #[test]
    fn prepare_replica_committed_not_in_log() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, HealthStatus::Normal, replicas[0]);
        let mut client = Client::new(group);

        let mut replica = Replica::new(0, HealthStatus::Normal, replicas[1]);
        let mut mailbox = Mailbox::from(replicas[1]);

        simulate_request(&mut primary, &mut client, operation, 2);
        primary.committed.increment();

        let message = prepare_message(&primary, &client, operation);

        mailbox.deliver(message);
        replica.poll(&mut mailbox);

        assert_eq!(mailbox.drain_outbound().count(), 0);
        assert_eq!(replica.service, 0);
    }

    #[test]
    fn prepare_replica_buffered() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, HealthStatus::Normal, replicas[0]);
        let mut client = Client::new(group);

        let mut replica = Replica::new(0, HealthStatus::Normal, replicas[1]);
        let mut mailbox = Mailbox::from(replicas[1]);

        simulate_request(&mut primary, &mut client, operation, 2);

        let message = prepare_message(&primary, &client, operation);

        mailbox.deliver(message);
        replica.poll(&mut mailbox);

        assert_eq!(mailbox.drain_outbound().count(), 0);
    }

    #[test]
    fn prepare_ok_primary() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(5);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, HealthStatus::Normal, replicas[0]);
        let mut replica1 = Replica::new(0, HealthStatus::Normal, replicas[1]);
        let mut replica2 = Replica::new(0, HealthStatus::Normal, replicas[2]);
        let mut client = Client::new(group);
        let mut mailbox = simulate_request(&mut primary, &mut client, operation, 1);

        simulate_broadcast(&mut mailbox, vec![&mut replica1, &mut replica2]);

        let message1 = prepare_ok_message(&primary, &replica1);
        mailbox.deliver(message1);
        primary.poll(&mut mailbox);
        assert_eq!(mailbox.drain_outbound().count(), 0);

        let message2 = prepare_ok_message(&primary, &replica2);
        mailbox.deliver(message2);
        primary.poll(&mut mailbox);

        let messages: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(
            messages,
            vec![reply_message(&primary, &client, operation, 1)]
        );
        assert_eq!(primary.service, operation.len());
        assert_eq!(
            primary.client_table.get(&client.request(operation)),
            Some(new_reply(&client, operation, 1))
        );
    }

    #[test]
    fn prepare_ok_primary_skipped() {
        let times = 2;
        let operation = b"Hi!";
        let group = GroupIdentifier::new(5);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, HealthStatus::Normal, replicas[0]);
        let mut replica1 = Replica::new(0, HealthStatus::Normal, replicas[1]);
        let mut replica2 = Replica::new(0, HealthStatus::Normal, replicas[2]);
        let mut client = Client::new(group);
        let mut mailbox = simulate_request(&mut primary, &mut client, operation, times);

        simulate_broadcast(&mut mailbox, vec![&mut replica1, &mut replica2]);

        let message1 = prepare_ok_message(&primary, &replica1);
        let message2 = prepare_ok_message(&primary, &replica2);

        mailbox.deliver(message1);
        mailbox.deliver(message2);
        primary.poll(&mut mailbox);

        let messages: Vec<Message> = mailbox.drain_outbound().collect();

        // backtrack the request id to build the replies correctly.
        client.request = RequestIdentifier::default();
        let mut replies = Vec::with_capacity(times);

        for i in 1..=times {
            client.request.increment();
            replies.push(reply_message(&primary, &client, operation, i));
        }

        assert_eq!(messages, replies);
        assert_eq!(primary.service, operation.len() * times);
    }

    #[test]
    fn do_view_change_replica() {
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut replica = Replica::new(0, HealthStatus::Normal, replicas[1]);
        let mut mailbox = Mailbox::from(replicas[1]);

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
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut client = Client::new(group);
        let mut replica = Replica::new(0, HealthStatus::Normal, replicas[2]);
        let mut primary = Replica::new(0, HealthStatus::Normal, replicas[1]);
        let mut mailbox = Mailbox::from(replicas[1]);

        client.request.increment();

        primary.view.increment();
        primary.status = Status::ViewChange;

        replica.view.increment();
        replica.status = Status::ViewChange;
        replica.committed.increment();
        replica.executed.increment();
        replica.push_request(Request {
            op: vec![],
            c: client.identifier,
            s: client.request,
        });

        mailbox.deliver(do_view_change_message(&primary));
        mailbox.deliver(do_view_change_message(&replica));

        primary.poll(&mut mailbox);

        let messages: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(mailbox.drain_inbound().count(), 0);
        assert_eq!(
            messages,
            vec![
                start_view_message(&primary),
                reply_message(&primary, &client, &[], 1),
            ]
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
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, HealthStatus::Normal, replicas[1]);
        let mut mailbox = Mailbox::from(replicas[1]);

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
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut client = Client::new(group);
        let mut primary = Replica::new(0, HealthStatus::Normal, replicas[1]);
        let mut replica = Replica::new(0, HealthStatus::Normal, replicas[2]);
        let mut mailbox = Mailbox::from(replica.identifier);

        primary.view.increment();
        primary.committed.increment();
        primary.executed.increment();
        primary.push_request(Request {
            op: operation.to_vec(),
            c: client.identifier,
            s: client.request.increment(),
        });
        primary.push_request(Request {
            op: operation.to_vec(),
            c: client.identifier,
            s: client.request.increment(),
        });

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

    fn do_view_change_message<S, H>(replica: &Replica<S, H>) -> Message {
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

    fn simulate_broadcast<S: Service, H: HealthDetector>(
        source: &mut Mailbox,
        replicas: Vec<&mut Replica<S, H>>,
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

    fn simulate_request<S: Service, H: HealthDetector>(
        primary: &mut Replica<S, H>,
        client: &mut Client,
        operation: &[u8],
        times: usize,
    ) -> Mailbox {
        let mut mailbox = Mailbox::from(primary.identifier);

        for _ in 1..=times {
            mailbox.deliver(client.new_message(operation));
            primary.poll(&mut mailbox);
        }

        mailbox
    }

    fn prepare_message<S, H>(
        replica: &Replica<S, H>,
        client: &Client,
        operation: &[u8],
    ) -> Message {
        Message {
            from: replica.identifier.into(),
            to: replica.identifier.group().into(),
            view: replica.view,
            payload: Prepare {
                n: replica.op_number,
                m: Request {
                    op: Vec::from(operation),
                    c: client.identifier,
                    s: client.request,
                },
                k: replica.committed,
            }
            .into(),
        }
    }

    fn prepare_ok_message<S, H>(primary: &Replica<S, H>, replica: &Replica<S, H>) -> Message {
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

    fn reply_message<S, H>(
        primary: &Replica<S, H>,
        client: &Client,
        operation: &[u8],
        times: usize,
    ) -> Message {
        Message {
            from: primary.identifier.into(),
            to: client.identifier.into(),
            view: primary.view,
            payload: new_reply(client, operation, times).into(),
        }
    }

    fn new_reply(client: &Client, operation: &[u8], times: usize) -> Reply {
        Reply {
            x: (operation.len() * times).to_be_bytes().to_vec(),
            s: client.request,
        }
    }

    fn ping_message<S, H>(primary: &Replica<S, H>) -> Message {
        Message {
            from: primary.identifier.into(),
            to: primary.identifier.group().into(),
            view: primary.view,
            payload: Payload::Ping,
        }
    }

    fn start_view_message<S, H>(primary: &Replica<S, H>) -> Message {
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
}
