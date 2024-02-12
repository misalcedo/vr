use crate::health::{HealthDetector, HealthStatus};
use crate::mailbox::{Address, Mailbox};
use crate::model::{DoViewChange, Message, Payload, PrepareOk};
use crate::replica::{NonVolatileState, Replica, Status};
use crate::service::Service;
use crate::state::State;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

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
        let mut committed = self.committed;
        let mut operations = BinaryHeap::new();

        mailbox.visit(|message| match message {
            Message {
                from: Address::Replica(_),
                payload: Payload::Prepare(prepare),
                ..
            } => {
                operations.push(Reverse(prepare.n));
                committed = committed.max(prepare.k);
            }
            Message {
                from: Address::Replica(_),
                payload: Payload::Commit(commit),
                ..
            } => {
                committed = committed.max(commit.k);
            }
            _ => {}
        });

        let mut preparable = self.op_number;
        while let Some(Reverse(operation)) = operations.pop() {
            if operation == preparable.next() {
                preparable.increment();
            } else {
                // TODO: perform state transfer if necessary to get missing information.
                break;
            }
        }

        let primary = self.identifier.primary(self.view);

        mailbox.select_all(|sender, message| match message {
            Message {
                from: Address::Replica(_),
                payload: Payload::Commit(_),
                ..
            } => None,
            Message {
                from: Address::Replica(_),
                payload: Payload::Prepare(prepare),
                ..
            } if prepare.n <= preparable => {
                self.client_table.start(&prepare.m.request);
                self.push_log_entry(prepare.m);
                sender.send(primary, self.view, PrepareOk { n: self.op_number });

                None
            }
            Message {
                from: Address::Replica(_),
                payload: Payload::Prepare(_),
                ..
            } => Some(message),
            message @ Message {
                from: Address::Replica(_),
                view,
                payload: Payload::DoViewChange(_),
                ..
            } if self.identifier == self.identifier.primary(view) => Some(message),
            _ => None,
        });

        self.execute_committed(committed, None);

        // We process unhealthy primary after consuming as many prepare messages as possible.
        // This ensures we keep the longest log possible into the next view.
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
        mailbox.select(|sender, message| match message {
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
                self.prepare_ok_uncommitted(sender);

                None
            }
            _ => None,
        });
    }

    fn process_recovering(&mut self, mailbox: &mut Mailbox) {
        // TODO: Only send this once and then wait for a response (with timeout).
        mailbox.broadcast(self.view, Payload::Recovery);
        mailbox.select(|sender, message| match message {
            Message {
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
