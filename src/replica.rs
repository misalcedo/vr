use crate::mailbox::Mailbox;
use crate::new_model::{
    Address, ClientIdentifier, Envelope, GroupIdentifier, Message, OpNumber, Payload, Prepare,
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

    pub fn envelope(&mut self, payload: &[u8]) -> Envelope {
        Envelope {
            from: self.identifier.into(),
            to: self.group.primary(self.view).into(),
            message: Message {
                view: self.view,
                payload: Request {
                    op: Vec::from(payload),
                    c: self.identifier,
                    s: self.request.increment(),
                }
                .into(),
            },
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

pub struct Replica<S> {
    /// The service code for processing committed client requests.
    service: S,
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

impl<S> Replica<S>
where
    S: Service,
{
    pub fn new(service: S, identifier: ReplicaIdentifier) -> Self {
        Self {
            service,
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

        mailbox.select(|sender, envelope| {
            match envelope {
                Envelope {
                    message:
                        Message {
                            payload: Payload::Request(request),
                            ..
                        },
                    ..
                } => {
                    self.push_request(request.clone());

                    sender.broadcast(Message::new(
                        self.view,
                        Prepare {
                            n: self.op_number,
                            m: request,
                            k: self.committed,
                        },
                    ));

                    None
                }
                ref envelope @ Envelope {
                    from,
                    message:
                        Message {
                            payload: Payload::PrepareOk(prepare_ok),
                            ..
                        },
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
                                    Message::new(
                                        self.view,
                                        Reply {
                                            s: request.s,
                                            x: reply,
                                        },
                                    ),
                                );
                                self.executed.increment();
                            }

                            None
                        } else {
                            Some(envelope.clone())
                        }
                    }
                }
                _ => Some(envelope),
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

        mailbox.select(|sender, envelope| match envelope {
            Envelope {
                message:
                    Message {
                        payload: Payload::Prepare(prepare),
                        ..
                    },
                ..
            } if next_op == prepare.n => {
                self.push_request(prepare.m);

                let primary = self.identifier.primary(self.view);
                sender.send(
                    primary,
                    Message::new(self.view, PrepareOk { n: self.op_number }),
                );

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
            _ => Some(envelope),
        })
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
    use crate::new_model::GroupIdentifier;

    #[test]
    fn request_primary() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, replicas[0]);
        let mut client = Client::new(group);
        let mut mailbox = simulate_request(&mut primary, &mut client, operation, 1);

        let envelopes: Vec<Envelope> = mailbox.drain_outbound().collect();

        assert_eq!(
            envelopes,
            vec![prepare_envelope(&primary, &client, operation)]
        );
    }

    #[test]
    fn prepare_replica() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, replicas[0]);
        let mut client = Client::new(group);

        let mut replica = Replica::new(0, replicas[1]);
        let mut mailbox = Mailbox::from(replicas[1]);

        simulate_request(&mut primary, &mut client, operation, 1);

        let envelope = prepare_envelope(&primary, &client, operation);

        mailbox.deliver(envelope);
        replica.poll(&mut mailbox);

        let envelopes: Vec<Envelope> = mailbox.drain_outbound().collect();

        assert_eq!(envelopes, vec![prepare_ok_envelope(&primary, &replica)]);
        assert_eq!(replica.op_number, OpNumber::from(1));
    }

    #[test]
    fn prepare_replica_committed() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, replicas[0]);
        let mut client = Client::new(group);

        let mut replica = Replica::new(0, replicas[1]);
        let mut mailbox = Mailbox::from(replicas[1]);

        simulate_request(&mut primary, &mut client, operation, 1);
        let envelope = prepare_envelope(&primary, &client, operation);
        mailbox.deliver(envelope);
        replica.poll(&mut mailbox);
        assert_eq!(mailbox.drain_outbound().count(), 1);

        simulate_request(&mut primary, &mut client, operation, 1);
        primary.committed.increment();
        let envelope = prepare_envelope(&primary, &client, operation);
        mailbox.deliver(envelope);
        replica.poll(&mut mailbox);

        let envelopes: Vec<Envelope> = mailbox.drain_outbound().collect();

        assert_eq!(envelopes, vec![prepare_ok_envelope(&primary, &replica)]);
        assert_eq!(replica.service, operation.len());
    }

    #[test]
    fn prepare_replica_committed_not_in_log() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, replicas[0]);
        let mut client = Client::new(group);

        let mut replica = Replica::new(0, replicas[1]);
        let mut mailbox = Mailbox::from(replicas[1]);

        simulate_request(&mut primary, &mut client, operation, 2);
        primary.committed.increment();

        let envelope = prepare_envelope(&primary, &client, operation);

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

        let mut primary = Replica::new(0, replicas[0]);
        let mut client = Client::new(group);

        let mut replica = Replica::new(0, replicas[1]);
        let mut mailbox = Mailbox::from(replicas[1]);

        simulate_request(&mut primary, &mut client, operation, 2);

        let envelope = prepare_envelope(&primary, &client, operation);

        mailbox.deliver(envelope);
        replica.poll(&mut mailbox);

        let envelopes: Vec<Envelope> = mailbox.drain_outbound().collect();

        assert_eq!(envelopes, vec![]);
    }

    #[test]
    fn prepare_ok_primary() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(5);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, replicas[0]);
        let mut replica1 = Replica::new(0, replicas[1]);
        let mut replica2 = Replica::new(0, replicas[2]);
        let mut client = Client::new(group);
        let mut mailbox = simulate_request(&mut primary, &mut client, operation, 1);

        simulate_broadcast(&mut mailbox, vec![&mut replica1, &mut replica2]);

        let envelope1 = prepare_ok_envelope(&primary, &replica1);
        mailbox.deliver(envelope1);
        primary.poll(&mut mailbox);
        assert_eq!(mailbox.drain_outbound().count(), 0);

        let envelope2 = prepare_ok_envelope(&primary, &replica2);
        mailbox.deliver(envelope2);
        primary.poll(&mut mailbox);

        let envelopes: Vec<Envelope> = mailbox.drain_outbound().collect();

        assert_eq!(
            envelopes,
            vec![reply_envelope(&primary, &client, operation, 1)]
        );
        assert_eq!(primary.service, operation.len());
    }

    #[test]
    fn prepare_ok_primary_skipped() {
        let times = 2;
        let operation = b"Hi!";
        let group = GroupIdentifier::new(5);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, replicas[0]);
        let mut replica1 = Replica::new(0, replicas[1]);
        let mut replica2 = Replica::new(0, replicas[2]);
        let mut client = Client::new(group);
        let mut mailbox = simulate_request(&mut primary, &mut client, operation, times);

        simulate_broadcast(&mut mailbox, vec![&mut replica1, &mut replica2]);

        let envelope1 = prepare_ok_envelope(&primary, &replica1);
        let envelope2 = prepare_ok_envelope(&primary, &replica2);

        mailbox.deliver(envelope1);
        mailbox.deliver(envelope2);
        primary.poll(&mut mailbox);

        let envelopes: Vec<Envelope> = mailbox.drain_outbound().collect();

        // backtrack the request id to build the replies correctly.
        client.request = RequestIdentifier::default();
        let mut replies = Vec::with_capacity(times);

        for i in 1..=times {
            client.request.increment();
            replies.push(reply_envelope(&primary, &client, operation, i));
        }

        assert_eq!(envelopes, replies);
        assert_eq!(primary.service, operation.len() * times);
    }

    fn simulate_broadcast(source: &mut Mailbox, replicas: Vec<&mut Replica<usize>>) {
        let envelopes: Vec<Envelope> = source.drain_outbound().collect();

        for replica in replicas {
            let mut mailbox = Mailbox::from(replica.identifier);

            for envelope in envelopes.iter() {
                mailbox.deliver(envelope.clone());
                replica.poll(&mut mailbox);
            }
        }
    }

    fn simulate_request(
        primary: &mut Replica<usize>,
        client: &mut Client,
        operation: &[u8],
        times: usize,
    ) -> Mailbox {
        let mut mailbox = Mailbox::from(primary.identifier);

        for _ in 0..times {
            mailbox.deliver(client.envelope(operation));
            primary.poll(&mut mailbox);
        }

        mailbox
    }

    fn prepare_envelope<S>(replica: &Replica<S>, client: &Client, operation: &[u8]) -> Envelope {
        Envelope {
            from: replica.identifier.into(),
            to: replica.identifier.group().into(),
            message: Message {
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
            },
        }
    }

    fn prepare_ok_envelope<S>(primary: &Replica<S>, replica: &Replica<S>) -> Envelope {
        Envelope {
            from: replica.identifier.into(),
            to: primary.identifier.into(),
            message: Message {
                view: replica.view,
                payload: PrepareOk {
                    n: replica.op_number,
                }
                .into(),
            },
        }
    }

    fn reply_envelope<S>(
        primary: &Replica<S>,
        client: &Client,
        operation: &[u8],
        times: usize,
    ) -> Envelope {
        Envelope {
            from: primary.identifier.into(),
            to: client.identifier.into(),
            message: Message {
                view: primary.view,
                payload: Reply {
                    x: (operation.len() * times).to_be_bytes().to_vec(),
                    s: client.request,
                }
                .into(),
            },
        }
    }
}
