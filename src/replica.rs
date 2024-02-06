use crate::health::{HealthDetector, HealthStatus};
use crate::mailbox::Mailbox;
use crate::new_model::{
    Address, ClientIdentifier, DoViewChange, GroupIdentifier, Message, OpNumber, Payload, Prepare,
    PrepareOk, ReplicaIdentifier, Reply, Request, RequestIdentifier, View,
};
use crate::service::Service;
use std::collections::{HashMap, HashSet};

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

    pub fn envelope(&mut self, payload: &[u8]) -> Message {
        Message {
            from: self.identifier.into(),
            to: self.group.primary(self.view).into(),
            view: self.view,
            payload: Request {
                op: Vec::from(payload),
                c: self.identifier,
                s: self.request.increment(),
            }
            .into(),
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
            view: View::default(),
            op_number: OpNumber::default(),
            status: Default::default(),
            log: vec![],
            committed: Default::default(),
            executed: Default::default(),
        }
    }

    pub fn poll(&mut self, mailbox: &mut Mailbox) {
        let primary = self.identifier.primary(self.view);

        if primary == self.identifier {
            self.poll_primary(mailbox);
        } else {
            self.poll_replica(mailbox);
        }
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

        mailbox.select(|sender, message| {
            match message {
                _ if message.view > self.view => {
                    todo!("Perform state transfer")
                }
                _ if message.view < self.view => {
                    sender.send(message.from, self.view, Payload::Outdated);
                    None
                }
                Message {
                    payload: Payload::Request(request),
                    ..
                } => {
                    self.push_request(request.clone());

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
                ref envelope @ Message {
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

                            let length = OpNumber::from(self.log.len());
                            while self.committed > self.executed && self.executed < length {
                                // executed must be incremented after indexing the log to avoid panicking.
                                let request = &self.log[usize::from(self.executed)];
                                let reply = self.service.invoke(request.op.as_slice());

                                sender.send(
                                    request.c,
                                    self.view,
                                    Reply {
                                        s: request.s,
                                        x: reply,
                                    },
                                );
                                self.executed.increment();
                            }

                            None
                        } else {
                            Some(envelope.clone())
                        }
                    }
                }
                _ => Some(message),
            }
        })
    }

    fn process_view_change_primary(&mut self, mailbox: &mut Mailbox) {}

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
            _ if message.view > self.view => {
                todo!("Perform state transfer")
            }
            _ if message.view < self.view => {
                sender.send(message.from, self.view, Payload::Outdated);
                None
            }
            Message {
                payload: Payload::Prepare(prepare),
                ..
            } if next_op == prepare.n => {
                self.push_request(prepare.m);

                let primary = self.identifier.primary(self.view);
                sender.send(primary, self.view, PrepareOk { n: self.op_number });

                self.committed = self.committed.max(prepare.k);

                let length = OpNumber::from(self.log.len());
                while self.committed > self.executed && self.executed < length {
                    // executed must be incremented after indexing the log to avoid panicking.
                    let request = &self.log[usize::from(self.executed)];
                    self.service.invoke(request.op.as_slice());
                    self.executed.increment();
                }

                None
            }
            // TODO: perform state transfer if necessary to get missing information.
            _ => Some(message),
        });

        if self.health_detector.detect(self.view, self.identifier) == HealthStatus::Unhealthy {
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

    fn process_view_change_replica(&mut self, mailbox: &mut Mailbox) {}

    // Push a request to the end of the log and increment the op-number.
    fn push_request(&mut self, request: Request) {
        self.op_number.increment();
        self.log.push(request);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::HealthStatus;
    use crate::new_model::GroupIdentifier;

    #[test]
    fn request_primary() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, HealthStatus::Normal, replicas[0]);
        let mut client = Client::new(group);
        let mut mailbox = simulate_request(&mut primary, &mut client, operation, 1);

        let envelopes: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(
            envelopes,
            vec![prepare_message(&primary, &client, operation)]
        );
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

        let envelopes: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(
            envelopes,
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

        let envelope = prepare_message(&primary, &client, operation);

        mailbox.deliver(envelope);
        replica.poll(&mut mailbox);

        let envelopes: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(envelopes, vec![prepare_ok_message(&primary, &replica)]);
        assert_eq!(replica.op_number, OpNumber::from(1));
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

        let envelope = prepare_message(&primary, &client, operation);

        replica.view.increment();
        replica.view.increment();

        mailbox.deliver(envelope);
        replica.poll(&mut mailbox);

        let envelopes: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(
            envelopes,
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
        let envelope = prepare_message(&primary, &client, operation);
        mailbox.deliver(envelope);
        replica.poll(&mut mailbox);
        assert_eq!(mailbox.drain_outbound().count(), 1);

        simulate_request(&mut primary, &mut client, operation, 1);
        primary.committed.increment();
        let envelope = prepare_message(&primary, &client, operation);
        mailbox.deliver(envelope);
        replica.poll(&mut mailbox);

        let envelopes: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(envelopes, vec![prepare_ok_message(&primary, &replica)]);
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

        let envelope = prepare_message(&primary, &client, operation);

        mailbox.deliver(envelope);
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

        let envelope = prepare_message(&primary, &client, operation);

        mailbox.deliver(envelope);
        replica.poll(&mut mailbox);

        let envelopes: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(envelopes, vec![]);
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

        let envelope1 = prepare_ok_message(&primary, &replica1);
        mailbox.deliver(envelope1);
        primary.poll(&mut mailbox);
        assert_eq!(mailbox.drain_outbound().count(), 0);

        let envelope2 = prepare_ok_message(&primary, &replica2);
        mailbox.deliver(envelope2);
        primary.poll(&mut mailbox);

        let envelopes: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(
            envelopes,
            vec![reply_message(&primary, &client, operation, 1)]
        );
        assert_eq!(primary.service, operation.len());
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

        let envelope1 = prepare_ok_message(&primary, &replica1);
        let envelope2 = prepare_ok_message(&primary, &replica2);

        mailbox.deliver(envelope1);
        mailbox.deliver(envelope2);
        primary.poll(&mut mailbox);

        let envelopes: Vec<Message> = mailbox.drain_outbound().collect();

        // backtrack the request id to build the replies correctly.
        client.request = RequestIdentifier::default();
        let mut replies = Vec::with_capacity(times);

        for i in 1..=times {
            client.request.increment();
            replies.push(reply_message(&primary, &client, operation, i));
        }

        assert_eq!(envelopes, replies);
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

        let envelopes: Vec<Message> = mailbox.drain_outbound().collect();

        assert_eq!(
            envelopes,
            vec![do_view_change_message(&replicas, &replica)]
        );
    }

    fn do_view_change_message<S, H>(
        replicas: &[ReplicaIdentifier],
        replica: &Replica<S, H>,
    ) -> Message {
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
        let envelopes: Vec<Message> = source.drain_outbound().collect();

        for replica in replicas {
            let mut mailbox = Mailbox::from(replica.identifier);

            for envelope in envelopes.iter() {
                mailbox.deliver(envelope.clone());
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
            mailbox.deliver(client.envelope(operation));
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
            payload: Reply {
                x: (operation.len() * times).to_be_bytes().to_vec(),
                s: client.request,
            }
            .into(),
        }
    }
}
