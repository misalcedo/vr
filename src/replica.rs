use crate::mailbox::{Mailbox, Sender};
use crate::new_model::{Envelope, Message, ReplicaIdentifier, Request, View, OpNumber, Payload, Prepare, ClientIdentifier, GroupIdentifier, RequestIdentifier, PrepareOk};
use crate::service::Service;

pub struct Client {
    identifier: ClientIdentifier,
    view: View,
    request: RequestIdentifier,
    group: GroupIdentifier,
}

impl Client {
    pub fn new(group: GroupIdentifier) -> Self {
        Self { identifier: Default::default(), view: Default::default(), request: Default::default(), group }
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
                }.into()
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
    pub fn new(
        service: S,
        identifier: ReplicaIdentifier,
    ) -> Self {
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
            Status::Recovering => ()
        }
    }

    fn process_normal_primary(&mut self, mailbox: &mut Mailbox) {
        mailbox.select(|sender, envelope| match envelope {
            Envelope { message: Message { payload: Payload::Request(request), ..}, .. } => {
                self.push_request(request.clone());

                sender.broadcast(Message::new(
                    self.view,
                    Prepare {
                        n: self.op_number,
                        m: request,
                        k: self.committed,
                    }
                ));

                None
            }
            _ => Some(envelope)
        })
    }

    fn process_view_change_primary(&mut self, mailbox: &mut Mailbox) {
    }

    fn poll_replica(&mut self, mailbox: &mut Mailbox) {
        match self.status {
            Status::Normal => self.process_normal_replica(mailbox),
            Status::ViewChange => self.process_view_change_replica(mailbox),
            Status::Recovering => ()
        }
    }

    fn process_normal_replica(&mut self, mailbox: &mut Mailbox) {
        let next_op = self.op_number.next();

        mailbox.select(|sender, envelope| match envelope {
            Envelope { message: Message { payload: Payload::Prepare(prepare), ..}, .. } if next_op == prepare.n => {
                self.push_request(prepare.m);

                let primary = self.identifier.primary(self.view);

                sender.send(primary, Message::new(self.view, PrepareOk { n: self.op_number, }));

                None
            }
            // TODO: perform state transfer if necessary to get missing information.
            _ => Some(envelope)
        })
    }

    fn process_view_change_replica(&mut self, mailbox: &mut Mailbox) {
    }

    // Push a request to the end of the log and increment the op-number.
    fn push_request(&mut self, request: Request) {
        self.op_number.increment();
        self.log.push(request);
    }
}


#[cfg(test)]
mod tests {
    use crate::new_model::GroupIdentifier;
    use super::*;

    #[test]
    fn request_primary() {
        let operation = b"Hi!";
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut primary = Replica::new(0, replicas[0]);
        let mut client = Client::new(group);
        let mut mailbox = simulate_prepare(&mut primary, &mut client, operation, 1);

        let envelopes: Vec<Envelope> = mailbox.drain_outbound().collect();

        assert_eq!(envelopes, vec![prepare_envelope(&primary, &client, operation)]);
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

        simulate_prepare(&mut primary, &mut client, operation, 1);

        let envelope = prepare_envelope(&primary, &client, operation);

        mailbox.deliver(envelope);
        replica.poll(&mut mailbox);

        let envelopes: Vec<Envelope> = mailbox.drain_outbound().collect();

        assert_eq!(envelopes, vec![prepare_ok_envelope(&primary, &replica)]);
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

        simulate_prepare(&mut primary, &mut client, operation, 2);

        let envelope = prepare_envelope(&primary, &client, operation);

        mailbox.deliver(envelope);
        replica.poll(&mut mailbox);

        let envelopes: Vec<Envelope> = mailbox.drain_outbound().collect();

        assert_eq!(envelopes, vec![]);
    }

    fn simulate_prepare(primary: &mut Replica<usize>, client: &mut Client, operation: &[u8], times: usize) -> Mailbox {
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
                }.into()
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
                }.into()
            },
        }
    }
}