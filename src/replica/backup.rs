use crate::health::{HealthDetector, HealthStatus};
use crate::mailbox::{Address, Mailbox};
use crate::model::{DoViewChange, Message, Payload, PrepareOk};
use crate::replica::{NonVolatileState, Replica, Status};
use crate::service::Service;
use crate::state::State;

pub trait Backup {
    fn process_normal(&mut self, mailbox: &mut Mailbox);

    fn process_view_change(&mut self, mailbox: &mut Mailbox);

    fn process_recovering(&mut self, mailbox: &mut Mailbox);
}

impl<NS, S, HD> Backup for Replica<NS, S, HD>
where
    NS: State<NonVolatileState>,
    S: Service,
    HD: HealthDetector,
{
    fn process_normal(&mut self, mailbox: &mut Mailbox) {
        let next_op = self.op_number.next();

        mailbox.select(|sender, message| match message {
            Message {
                payload: Payload::Commit(commit),
                ..
            } => {
                self.execute_committed(commit.k, None);
                None
            }
            Message {
                payload: Payload::Prepare(prepare),
                ..
            } if next_op == prepare.n => {
                self.client_table.start(&prepare.m);
                self.push_request(prepare.m);

                let primary = self.identifier.primary(self.view);

                sender.send(primary, self.view, PrepareOk { n: self.op_number });
                self.execute_committed(prepare.k, None);

                None
            }
            // TODO: perform state transfer if necessary to get missing information.
            Message {
                payload: Payload::Prepare(_),
                ..
            } => Some(message),
            _ => None,
        });

        if self.health_detector.detect(self.view, self.identifier) >= HealthStatus::Unhealthy {
            self.view.increment();
            self.status = Status::ViewChange;
            self.save_non_volatile_state();

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

    fn process_view_change(&mut self, mailbox: &mut Mailbox) {
        mailbox.select(|_, message| match message {
            Message {
                from: Address::Replica(_),
                view,
                payload: Payload::StartView(start_view),
                ..
            } => {
                self.replace_log(start_view.l);
                self.view = view;
                self.status = Status::Normal;
                self.execute_committed(start_view.k, None);

                None
            }
            _ => Some(message),
        });

        self.prepare_ok_uncommitted(mailbox);
    }

    fn process_recovering(&mut self, mailbox: &mut Mailbox) {
        // TODO: Only send this once and then wait for a response (with timeout).
        mailbox.broadcast(self.view, Payload::Recovery);
        mailbox.select(|sender, message| match message {
            Message {
                from: Address::Replica(_),
                view,
                payload: Payload::RecoveryResponse(recovery_response),
                ..
            } => {
                if view > self.view {
                    self.save_non_volatile_state()
                }

                self.view = view;
                self.replace_log(recovery_response.l);
                self.execute_committed(recovery_response.k, None);

                self.status = Status::Normal;
                self.prepare_ok_uncommitted(sender);

                None
            }
            _ => Some(message),
        });
    }
}
