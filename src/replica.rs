use crate::mailbox::{Mailbox, Sender};
use crate::new_model::{Envelope, Message, ReplicaIdentifier, Request, View, OpNumber, Payload, Prepare};
use crate::service::Service;

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub enum Status {
    #[default]
    Normal,
    ViewChange,
    Recovering,
}

#[derive(Debug)]
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
