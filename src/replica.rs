use crate::mailbox::{Mailbox, Sender};
use crate::new_model::{Envelope, Message, ReplicaIdentifier, Request, View, OpNumber, Payload, Prepare, ClientIdentifier, GroupIdentifier, RequestIdentifier};
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
        mailbox.select(|s, e| self.select_normal_primary(s, e))
    }

    fn select_normal_primary(&mut self, sender: &mut Sender, envelope: Envelope) -> Option<Envelope> {
        match envelope {
            Envelope { message: Message { payload: Payload::Request(request), ..}, .. } => {
                self.op_number.increment();
                self.log.push(request.clone());
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
        }
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
    }

    fn process_view_change_replica(&mut self, mailbox: &mut Mailbox) {
    }
}


#[cfg(test)]
mod tests {
    use crate::new_model::GroupIdentifier;
    use super::*;

    #[test]
    fn request_primary() {
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.replicas().collect();

        let mut replica = Replica::new(0, replicas[0]);
        let mut client = Client::new(group);
        let mut mailbox = Mailbox::from(replicas[0]);

        let operation = b"Hi!";
        let envelope = client.envelope(operation);

        mailbox.deliver(envelope);
        replica.poll(&mut mailbox);

        let envelopes: Vec<Envelope> = mailbox.drain_outbound().collect();

        assert_eq!(envelopes, vec![prepare_envelope(&replica, &client, operation)]);
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
}