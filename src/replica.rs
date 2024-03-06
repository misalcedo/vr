use crate::client_table::ClientTable;
use crate::configuration::Configuration;
use crate::log::Log;
use crate::mail::{Mailbox, Outbox};
use crate::nonce::Nonce;
use crate::protocol::{
    Checkpoint, Commit, DoViewChange, GetState, NewState, Prepare, PrepareOk, Recovery,
    RecoveryResponse, StartView, StartViewChange,
};
use crate::request::{Reply, Request};
use crate::service::Service;
use crate::status::Status;
use crate::viewstamp::{OpNumber, View};
use rand::Rng;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;

/// A replica may perform the role of a primary or backup depending on the configuration and the current view.
/// Implements a message-based viewstamped replication revisited protocol that does not wait for messages to arrive.
/// Instead, the protocol requires returned messages to be re-queued for later (i.e. after a new message comes in).
pub struct Replica<S>
where
    S: Service,
{
    configuration: Configuration,
    index: usize,
    service: S,
    status: Status,
    view: View,
    log: Log<S::Request, S::Prediction>,
    committed: OpNumber,
    client_table: ClientTable<S::Reply>,
    prepared: BTreeMap<OpNumber, HashSet<usize>>,
    start_view_changes: HashSet<usize>,
    do_view_changes: HashMap<usize, DoViewChange<S::Request, S::Prediction>>,
    recovery_responses: HashMap<usize, RecoveryResponse<S::Request, S::Prediction>>,
    nonce: Nonce,
    checkpoints: VecDeque<OpNumber>,
}

impl<S> Replica<S>
where
    S: Service,
{
    /// Creates a new instance of a replica.
    pub fn new(configuration: Configuration, index: usize, service: S) -> Self {
        Self {
            configuration,
            index,
            service,
            status: Status::Normal,
            view: Default::default(),
            log: Default::default(),
            committed: Default::default(),
            client_table: Default::default(),
            prepared: Default::default(),
            start_view_changes: Default::default(),
            do_view_changes: Default::default(),
            recovery_responses: Default::default(),
            nonce: Default::default(),
            checkpoints: Default::default(),
        }
    }

    /// Creates a new instance of a replica running the recovery protocol.
    /// The caller is responsible for determining when a replica needs to recover.
    pub fn recovering<O>(
        configuration: Configuration,
        index: usize,
        checkpoint: Checkpoint<S::Checkpoint>,
        outbox: &mut O,
    ) -> Self
    where
        O: Outbox<S>,
    {
        let mut replica = Self::new(configuration, index, checkpoint.state.into());

        replica.committed = checkpoint.committed;
        replica.status = Status::Recovering;

        outbox.recovery(Recovery {
            index,
            committed: replica.committed,
            nonce: replica.nonce,
        });

        replica
    }

    pub fn configuration(&self) -> Configuration {
        self.configuration
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn view(&self) -> View {
        self.view
    }

    pub fn checkpoint(&mut self, suffix: Option<NonZeroUsize>) -> Checkpoint<S::Checkpoint> {
        if let Some(suffix) = suffix.map(NonZeroUsize::get) {
            if self.checkpoints.len() >= suffix {
                let cutoff = self.checkpoints.len() - suffix;
                let checkpoint = self.checkpoints.drain(..=cutoff).last();
                let start = checkpoint.unwrap_or_default().next();

                self.log.compact(start);
            }
        }

        self.checkpoints.push_back(self.committed);

        Checkpoint {
            committed: self.committed,
            state: self.service.checkpoint(),
        }
    }

    pub fn idle<O>(&mut self, outbox: &mut O)
    where
        O: Outbox<S>,
    {
        match self.status {
            Status::Normal => {
                if self.is_primary() {
                    self.idle_primary(outbox);
                } else {
                    self.start_view_change(self.view.next(), outbox);
                }
            }
            Status::Recovering => {
                outbox.recovery(Recovery {
                    index: self.index,
                    committed: self.committed,
                    nonce: self.nonce,
                });
            }
            Status::ViewChange => {
                if self.is_backup() && self.should_do_view_change() {
                    self.start_view_change(self.view.next(), outbox);
                } else {
                    outbox.start_view_change(StartViewChange {
                        view: self.view,
                        index: self.index,
                    });
                }
            }
        }
    }

    pub fn handle_request<O>(&mut self, request: Request<S::Request>, outbox: &mut O)
    where
        O: Outbox<S>,
    {
        if self.is_backup() {
            return;
        }

        match self.client_table.compare(&request) {
            Ok(Ordering::Greater) => {
                let prediction = self.service.predict(&request.payload);
                let (entry, op_number) = self.log.push(self.view, request, prediction);

                self.client_table.start(entry.request());

                outbox.prepare(Prepare {
                    view: self.view,
                    op_number,
                    request: entry.request().clone(),
                    prediction: entry.prediction().clone(),
                    committed: self.committed,
                });
            }
            Ok(Ordering::Equal) => {
                if let Some(reply) = self.client_table.reply(&request) {
                    outbox.reply(request.client, reply);
                }
            }
            Ok(Ordering::Less) => (),
            Err(_) => (),
        }
    }

    pub fn handle_prepare<M>(
        &mut self,
        message: Prepare<S::Request, S::Prediction>,
        mailbox: &mut M,
    ) where
        M: Mailbox<S>,
    {
        if self.need_state_transfer(message.view) {
            self.state_transfer(message.view, mailbox);
            mailbox.push_prepare(message);
            return;
        }

        if self.should_ignore_normal(message.view) || self.log.contains(&message.op_number) {
            return;
        }

        let next = self.log.next_op_number();
        if next < message.op_number || next < message.committed {
            self.state_transfer(message.view, mailbox);
            mailbox.push_prepare(message);
            return;
        }

        self.client_table.start(&message.request);
        self.log
            .push(self.view, message.request, message.prediction);
        mailbox.prepare_ok(
            self.configuration % self.view,
            PrepareOk {
                view: self.view,
                op_number: message.op_number,
                index: self.index,
            },
        );
        self.commit_operations(message.committed, mailbox);
    }

    pub fn handle_prepare_ok<M>(&mut self, message: PrepareOk, mailbox: &mut M)
    where
        M: Mailbox<S>,
    {
        if self.need_state_transfer(message.view) {
            self.state_transfer(message.view, mailbox);
            mailbox.push_prepare_ok(message);
            return;
        }

        if self.should_ignore_normal(message.view) || message.op_number <= self.committed {
            return;
        }

        let prepared = self.prepared.entry(message.op_number).or_default();

        prepared.insert(message.index);

        if prepared.len() >= self.configuration.sub_majority() {
            self.prepared.retain(|&o, _| o > message.op_number);
            self.commit_operations(message.op_number, mailbox);
        }
    }

    pub fn handle_commit<M>(&mut self, message: Commit, mailbox: &mut M)
    where
        M: Mailbox<S>,
    {
        if self.need_state_transfer(message.view) {
            self.state_transfer(message.view, mailbox);
            mailbox.push_commit(message);
            return;
        }

        if self.should_ignore_normal(message.view) || message.committed <= self.committed {
            return;
        }

        if !self.log.contains(&message.committed) {
            self.state_transfer(message.view, mailbox);
            mailbox.push_commit(message);
            return;
        }

        self.commit_operations(message.committed, mailbox);
    }

    pub fn handle_get_state<M>(&mut self, message: GetState, mailbox: &mut M)
    where
        M: Mailbox<S>,
    {
        if self.need_state_transfer(message.view) {
            self.state_transfer(message.view, mailbox);
            mailbox.push_get_state(message);
            return;
        }

        if self.should_ignore_normal(message.view) {
            return;
        }

        if !self.log.contains(&message.op_number) {
            return;
        }

        mailbox.new_state(
            message.index,
            NewState {
                view: self.view,
                log: self.log.after(message.op_number),
                committed: self.committed,
            },
        );
    }

    pub fn handle_recovery<O>(&mut self, message: Recovery, outbox: &mut O)
    where
        O: Outbox<S>,
    {
        if self.status != Status::Normal {
            return;
        }

        let mut response = RecoveryResponse {
            view: self.view,
            nonce: message.nonce,
            log: Default::default(),
            committed: Default::default(),
            index: self.index,
        };

        if self.is_primary() {
            response.log = self.log.clone();
            response.committed = self.committed;
        }

        outbox.recovery_response(message.index, response);
    }

    pub fn handle_recovery_response<O>(
        &mut self,
        message: RecoveryResponse<S::Request, S::Prediction>,
        outbox: &mut O,
    ) where
        O: Outbox<S>,
    {
        if self.status != Status::Recovering || self.nonce != message.nonce {
            return;
        }

        self.recovery_responses.insert(message.index, message);

        if self.recovery_responses.len() >= self.configuration.quorum() {
            let view = self
                .recovery_responses
                .values()
                .map(|r| r.view)
                .max()
                .unwrap_or_default();
            let primary = self.configuration % view;

            if let Some(primary_response) = self.recovery_responses.remove(&primary) {
                self.view = primary_response.view;
                self.log = primary_response.log;
                self.set_status(Status::Normal);
                self.commit_operations(primary_response.committed, outbox);
                self.start_preparing_operations(outbox);
            }
        }
    }

    pub fn handle_new_state<O>(
        &mut self,
        message: NewState<S::Request, S::Prediction>,
        outbox: &mut O,
    ) where
        O: Outbox<S>,
    {
        if message.view < self.view
            || self.status != Status::Normal
            || message.log.first_op_number() != self.log.next_op_number()
        {
            return;
        }

        self.view = message.view;
        self.log.extend(message.log);
        self.commit_operations(message.committed, outbox);
        self.start_preparing_operations(outbox);
    }

    pub fn handle_start_view_change<O>(&mut self, message: StartViewChange, outbox: &mut O)
    where
        O: Outbox<S>,
    {
        if self.need_view_change(message.view) {
            self.start_view_change(message.view, outbox);
        }

        if self.should_ignore_view_change(message.view) {
            return;
        }

        self.start_view_changes.insert(message.index);

        if self.should_do_view_change() {
            outbox.do_view_change(
                self.configuration % self.view,
                DoViewChange {
                    view: self.view,
                    log: self.log.clone(),
                    committed: self.committed,
                    index: self.index,
                },
            )
        }
    }

    pub fn handle_do_view_change<O>(
        &mut self,
        message: DoViewChange<S::Request, S::Prediction>,
        outbox: &mut O,
    ) where
        O: Outbox<S>,
    {
        if self.need_view_change(message.view) {
            self.start_view_change(message.view, outbox);
        }

        if self.should_ignore_view_change(message.view) {
            return;
        }

        self.do_view_changes.insert(message.index, message);

        if self.do_view_changes.contains_key(&self.index)
            && self.do_view_changes.len() >= self.configuration.quorum()
        {
            let committed = self
                .do_view_changes
                .values()
                .map(|v| v.committed)
                .max()
                .unwrap_or(self.committed);
            if let Some(do_view_change) = self
                .do_view_changes
                .drain()
                .map(|(_, v)| v)
                .max_by(|x, y| x.log.cmp(&y.log))
            {
                self.log = do_view_change.log;
                self.view = do_view_change.view;
                self.set_status(Status::Normal);

                outbox.start_view(StartView {
                    view: self.view,
                    log: self.log.clone(),
                    committed,
                });

                self.commit_operations(committed, outbox);
                self.start_preparing_operations(outbox);
            }
        }
    }

    pub fn handle_start_view<O>(
        &mut self,
        message: StartView<S::Request, S::Prediction>,
        outbox: &mut O,
    ) where
        O: Outbox<S>,
    {
        if message.view < self.view {
            return;
        }

        if message.view == self.view && self.status == Status::Normal {
            return;
        }

        self.view = message.view;
        self.log = message.log;

        self.set_status(Status::Normal);
        self.commit_operations(message.committed, outbox);
        self.start_preparing_operations(outbox);
    }

    fn start_view_change<O>(&mut self, view: View, outbox: &mut O)
    where
        O: Outbox<S>,
    {
        self.view = view;

        self.set_status(Status::ViewChange);

        outbox.start_view_change(StartViewChange {
            view: self.view,
            index: self.index,
        });
    }

    fn state_transfer<O>(&mut self, view: View, outbox: &mut O)
    where
        O: Outbox<S>,
    {
        if self.view < view {
            self.log.truncate(self.committed);
        }

        let replicas = self.configuration.replicas();

        let mut replica = self.index;
        while replica == self.index {
            replica = rand::thread_rng().gen_range(0..replicas);
        }

        outbox.get_state(
            replica,
            GetState {
                view: self.view,
                op_number: self.log.last_op_number(),
                index: self.index,
            },
        );
    }

    fn commit_operations<O>(&mut self, committed: OpNumber, outbox: &mut O)
    where
        O: Outbox<S>,
    {
        while self.committed < committed {
            self.committed.increment();

            let entry = &self.log[self.committed];
            let request = entry.request();
            let reply = Reply {
                view: self.view,
                id: request.id,
                payload: self.service.invoke(&request.payload, entry.prediction()),
            };

            if self.is_primary() {
                outbox.reply(request.client, &reply);
            }

            self.client_table.finish(request, reply);
        }
    }

    fn start_preparing_operations<O>(&mut self, outbox: &mut O)
    where
        O: Outbox<S>,
    {
        let mut current = self.committed.next();

        while self.log.contains(&current) {
            let entry = &self.log[current];
            let request = entry.request();

            self.client_table.start(request);

            if self.is_backup() {
                outbox.prepare_ok(
                    self.configuration % self.view,
                    PrepareOk {
                        view: self.view,
                        op_number: current,
                        index: self.index,
                    },
                );
            }

            current.increment();
        }
    }

    fn set_status(&mut self, status: Status) {
        self.status = status;
        self.prepared = Default::default();

        // We only need this on a new replica. Therefore, we can deallocate on any status change.
        self.recovery_responses = Default::default();

        // Avoid allocating unless we need it for the current protocol.
        match self.status {
            Status::ViewChange => {
                self.start_view_changes = HashSet::with_capacity(self.configuration.sub_majority());
                self.do_view_changes = HashMap::with_capacity(self.configuration.quorum());
            }
            _ => {
                self.start_view_changes = Default::default();
                self.do_view_changes = Default::default();
            }
        }
    }

    fn idle_primary<O>(&mut self, outbox: &mut O)
    where
        O: Outbox<S>,
    {
        if self.committed == self.log.last_op_number() {
            outbox.commit(Commit {
                view: self.view,
                committed: self.committed,
            });
        } else {
            let mut op_number = self.committed.next();

            while self.log.contains(&op_number) {
                let entry = &self.log[op_number];

                outbox.prepare(Prepare {
                    view: self.view,
                    op_number,
                    request: entry.request().clone(),
                    prediction: entry.prediction().clone(),
                    committed: self.committed,
                });

                op_number.increment();
            }
        }
    }

    pub fn is_primary(&self) -> bool {
        (self.configuration % self.view) == self.index
    }

    pub fn is_backup(&self) -> bool {
        !self.is_primary()
    }

    fn should_ignore_normal(&self, view: View) -> bool {
        self.view != view || self.status != Status::Normal
    }

    fn need_state_transfer(&self, view: View) -> bool {
        self.status == Status::Normal && view > self.view
    }

    fn should_ignore_view_change(&self, view: View) -> bool {
        self.view != view || self.status != Status::ViewChange
    }

    fn need_view_change(&self, view: View) -> bool {
        self.status != Status::Recovering && view > self.view
    }

    fn should_do_view_change(&self) -> bool {
        self.start_view_changes.len() >= self.configuration.sub_majority()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BufferedMailbox, ProtocolPayload};

    #[test]
    fn sender_behind_prepare() {
        let configuration = Configuration::from(3);
        let mut replica = Replica::new(configuration, 0, 0);
        let mut mailbox = BufferedMailbox::default();

        replica.view.increment();
        replica.view.increment();

        let message = Prepare {
            view: View::default().next(),
            op_number: OpNumber::default().next(),
            request: Request {
                payload: 2,
                client: Default::default(),
                id: Default::default(),
            },
            prediction: (),
            committed: OpNumber::default(),
        };

        replica.handle_prepare(message, &mut mailbox);

        assert_eq!(Vec::from_iter(mailbox.drain_inbound()), vec![]);
        assert!(mailbox.is_empty());
    }

    #[test]
    fn sender_ahead_prepare() {
        let configuration = Configuration::from(3);
        let mut replica = Replica::new(configuration, 1, 0);
        let mut mailbox = BufferedMailbox::default();

        let message = Prepare {
            view: View::default().next(),
            op_number: OpNumber::default().next(),
            request: Request {
                payload: 2,
                client: Default::default(),
                id: Default::default(),
            },
            prediction: (),
            committed: OpNumber::default(),
        };

        replica.handle_prepare(message.clone(), &mut mailbox);

        assert_eq!(
            mailbox.pop_inbound().map(ProtocolPayload::unwrap_prepare),
            Some(message)
        );

        let mut messages = Vec::from_iter(mailbox.drain_send());
        let outbound = GetState {
            view: replica.view,
            op_number: replica.log.last_op_number(),
            index: replica.index,
        };
        let envelope = messages.pop().unwrap();

        assert_ne!(envelope.destination, replica.index);
        assert_eq!(envelope.payload.unwrap_get_state(), outbound);
        assert!(messages.is_empty());
        assert!(mailbox.is_empty());
    }

    #[test]
    fn sender_behind_prepare_ok() {
        let configuration = Configuration::from(3);
        let mut replica = Replica::new(configuration, 2, 0);
        let mut mailbox = BufferedMailbox::default();

        replica.view.increment();
        replica.view.increment();

        let message = PrepareOk {
            view: View::default().next(),
            op_number: OpNumber::default().next(),
            index: 0,
        };

        replica.handle_prepare_ok(message, &mut mailbox);

        assert_eq!(Vec::from_iter(mailbox.drain_inbound()), vec![]);
        assert!(mailbox.is_empty());
    }

    #[test]
    fn sender_ahead_prepare_ok() {
        let configuration = Configuration::from(3);
        let mut replica = Replica::new(configuration, 1, 0);
        let mut mailbox = BufferedMailbox::default();

        let message = PrepareOk {
            view: View::default().next(),
            op_number: OpNumber::default().next(),
            index: 0,
        };

        replica.handle_prepare_ok(message.clone(), &mut mailbox);

        assert_eq!(
            mailbox
                .pop_inbound()
                .map(ProtocolPayload::unwrap_prepare_ok),
            Some(message)
        );

        let mut messages = Vec::from_iter(mailbox.drain_send());
        let outbound = GetState {
            view: replica.view,
            op_number: replica.log.last_op_number(),
            index: replica.index,
        };
        let envelope = messages.pop().unwrap();

        assert_ne!(envelope.destination, replica.index);
        assert_eq!(envelope.payload.unwrap_get_state(), outbound);
        assert!(messages.is_empty());
        assert!(mailbox.is_empty());
    }

    #[test]
    fn sender_behind_commit() {
        let configuration = Configuration::from(3);
        let mut replica = Replica::new(configuration, 0, 0);
        let mut mailbox = BufferedMailbox::default();

        replica.view.increment();
        replica.view.increment();

        let message = Commit {
            view: View::default().next(),
            committed: OpNumber::default().next(),
        };

        replica.handle_commit(message, &mut mailbox);

        assert_eq!(Vec::from_iter(mailbox.drain_inbound()), vec![]);
        assert!(mailbox.is_empty());
    }

    #[test]
    fn sender_ahead_commit() {
        let configuration = Configuration::from(3);
        let mut replica = Replica::new(configuration, 0, 0);
        let mut mailbox = BufferedMailbox::default();

        let message = Commit {
            view: View::default().next(),
            committed: OpNumber::default().next(),
        };

        replica.handle_commit(message.clone(), &mut mailbox);

        assert_eq!(
            mailbox.pop_inbound().map(ProtocolPayload::unwrap_commit),
            Some(message)
        );

        let mut messages = Vec::from_iter(mailbox.drain_send());
        let outbound = GetState {
            view: replica.view,
            op_number: replica.log.last_op_number(),
            index: replica.index,
        };
        let envelope = messages.pop().unwrap();

        assert_ne!(envelope.destination, replica.index);
        assert_eq!(envelope.payload.unwrap_get_state(), outbound);
        assert!(messages.is_empty());
        assert!(mailbox.is_empty());
    }

    #[test]
    fn sender_behind_get_state() {
        let configuration = Configuration::from(3);
        let mut replica = Replica::new(configuration, 0, 0);
        let mut mailbox = BufferedMailbox::default();

        replica.view.increment();
        replica.view.increment();

        let message = GetState {
            view: View::default().next(),
            op_number: OpNumber::default(),
            index: 1,
        };

        replica.handle_get_state(message, &mut mailbox);

        assert_eq!(Vec::from_iter(mailbox.drain_inbound()), vec![]);
        assert!(mailbox.is_empty());
    }

    #[test]
    fn sender_ahead_get_state() {
        let configuration = Configuration::from(3);
        let mut replica = Replica::new(configuration, 0, 0);
        let mut mailbox = BufferedMailbox::default();

        let message = GetState {
            view: View::default().next(),
            op_number: OpNumber::default().next(),
            index: 1,
        };

        replica.handle_get_state(message.clone(), &mut mailbox);

        assert_eq!(
            mailbox.pop_inbound().map(ProtocolPayload::unwrap_get_state),
            Some(message)
        );

        let mut messages = Vec::from_iter(mailbox.drain_send());
        let outbound = GetState {
            view: replica.view,
            op_number: replica.log.last_op_number(),
            index: replica.index,
        };
        let envelope = messages.pop().unwrap();

        assert_ne!(envelope.destination, replica.index);
        assert_eq!(envelope.payload.unwrap_get_state(), outbound);
        assert!(messages.is_empty());
        assert!(mailbox.is_empty());
    }

    #[test]
    fn sender_behind_new_state() {
        let configuration = Configuration::from(3);
        let mut replica = Replica::new(configuration, 0, 0);
        let mut outbox = BufferedMailbox::default();

        replica.view.increment();
        replica.view.increment();
        replica.log.push(
            replica.view,
            Request {
                payload: 2,
                client: Default::default(),
                id: Default::default(),
            },
            (),
        );

        let message = NewState {
            view: View::default().next(),
            log: Log::default(),
            committed: OpNumber::default().next(),
        };

        replica.handle_new_state(message.clone(), &mut outbox);

        assert_ne!(replica.log, message.log);
        assert_ne!(replica.committed, message.committed);
        assert!(outbox.is_empty());
    }
}
