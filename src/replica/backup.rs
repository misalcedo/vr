use crate::health::{HealthDetector, HealthStatus};
use crate::mailbox::{Address, Mailbox};
use crate::model::{DoViewChange, Message, Payload, PrepareOk};
use crate::replica::{NonVolatileState, Replica, Role, Status};
use crate::service::Service;
use crate::state::State;

pub struct Backup<'a, NS, S, HD>(pub &'a mut Replica<NS, S, HD>);

impl<'a, NS, S, HD> Role for Backup<'a, NS, S, HD>
where
    NS: State<NonVolatileState>,
    S: Service,
    HD: HealthDetector,
{
    fn process_normal(&mut self, mailbox: &mut Mailbox) {
        let next_op = self.0.op_number.next();

        mailbox.select(|sender, message| match message {
            Message {
                from: Address::Replica(_),
                payload: Payload::Request(_),
                ..
            } => None,
            Message {
                from: Address::Replica(_),
                payload: Payload::Commit(_),
                ..
            } => None,
            Message {
                from: Address::Replica(_),
                payload: Payload::Prepare(prepare),
                ..
            } if next_op == prepare.n => {
                self.0.client_table.start(&prepare.m);
                self.0.push_request(prepare.m);

                let primary = self.0.identifier.primary(self.0.view);

                sender.send(
                    primary,
                    self.0.view,
                    PrepareOk {
                        n: self.0.op_number,
                    },
                );
                self.0.execute_committed(prepare.k, None);

                None
            }
            // TODO: perform state transfer if necessary to get missing information.
            _ => Some(message),
        });

        if self
            .0
            .health_detector
            .detect(self.0.view, self.0.identifier)
            >= HealthStatus::Unhealthy
        {
            self.0.view.increment();
            self.0.status = Status::ViewChange;
            self.0
                .state
                .save(NonVolatileState::new(self.0.identifier, self.0.view));

            mailbox.send(
                self.0.identifier.primary(self.0.view),
                self.0.view,
                DoViewChange {
                    l: self.0.log.clone(),
                    k: self.0.committed,
                },
            );
        }
    }

    fn process_view_change(&mut self, mailbox: &mut Mailbox) {
        mailbox.select(|_, message| match message {
            Message {
                from: Address::Replica(_),
                view,
                payload: Payload::StartView(start_view),
                ..
            } => {
                self.0.replace_log(start_view.l);
                self.0.view = view;
                self.0.status = Status::Normal;
                self.0.execute_committed(start_view.k, None);

                None
            }
            _ => Some(message),
        });

        let mut current = self.0.committed;

        while current < self.0.op_number {
            current.increment();
            mailbox.send(
                self.0.identifier.primary(self.0.view),
                self.0.view,
                Payload::PrepareOk(PrepareOk { n: current }),
            );
        }
    }

    fn process_recovering(&mut self, _mailbox: &mut Mailbox) {
        todo!()
    }
}
