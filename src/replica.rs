use crate::client_table::ClientTable;
use crate::configuration::Configuration;
use crate::log::Log;
use crate::mail::Outbox;
use crate::nonce::Nonce;
use crate::protocol::{
    Checkpoint, Commit, DoViewChange, GetState, NewState, Prepare, PrepareOk, Protocol, Recovery,
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
pub struct Replica<S, P>
where
    S: Service<P>,
    P: Protocol,
{
    configuration: Configuration,
    index: usize,
    service: S,
    status: Status,
    view: View,
    log: Log<P::Request, P::Prediction>,
    committed: OpNumber,
    client_table: ClientTable<P::Reply>,
    prepared: BTreeMap<OpNumber, HashSet<usize>>,
    start_view_changes: HashSet<usize>,
    do_view_changes: HashMap<usize, DoViewChange<P::Request, P::Prediction>>,
    recovery_responses: HashMap<usize, RecoveryResponse<P::Request, P::Prediction>>,
    nonce: Nonce,
    checkpoints: VecDeque<OpNumber>,
}

impl<S, P> Replica<S, P>
where
    S: Service<P>,
    P: Protocol,
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
        checkpoint: Checkpoint<P::Checkpoint>,
        outbox: &mut O,
    ) -> Self
    where
        O: Outbox<P>,
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

    pub fn checkpoint(&mut self, suffix: Option<NonZeroUsize>) -> Checkpoint<P::Checkpoint> {
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
        O: Outbox<P>,
    {
        match self.status {
            Status::Normal => {
                if self.is_primary() {
                    outbox.commit(Commit {
                        view: self.view,
                        committed: self.committed,
                    });
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

    pub fn handle_request<O>(&mut self, request: Request<P::Request>, outbox: &mut O)
    where
        O: Outbox<P>,
    {
        if self.is_backup() {
            return;
        }

        let (cached_request, comparison) = self.client_table.get_mut(&request);

        match comparison {
            Ordering::Greater => {
                let prediction = self.service.predict(&request.payload);
                let (entry, op_number) = self.log.push(self.view, request, prediction);

                outbox.prepare(Prepare {
                    view: self.view,
                    op_number,
                    request: entry.request().clone(),
                    prediction: entry.prediction().clone(),
                    committed: self.committed,
                });
            }
            Ordering::Equal => {
                if let Some(reply) = cached_request.reply() {
                    outbox.reply(request.client, reply);
                }
            }
            Ordering::Less => (),
        }
    }

    pub fn handle_prepare<O>(
        &mut self,
        message: Prepare<P::Request, P::Prediction>,
        outbox: &mut O,
    ) -> Option<Prepare<P::Request, P::Prediction>>
    where
        O: Outbox<P>,
    {
        if self.need_state_transfer(message.view) {
            self.state_transfer(message.view, outbox);
            return Some(message);
        }

        if self.should_ignore_normal(message.view) || self.log.contains(&message.op_number) {
            return None;
        }

        let next = self.log.next_op_number();
        if next < message.op_number || next < message.committed {
            self.state_transfer(message.view, outbox);
            return Some(message);
        }

        self.client_table.start(&message.request);
        self.log
            .push(self.view, message.request, message.prediction);
        outbox.prepare_ok(
            self.configuration % self.view,
            PrepareOk {
                view: self.view,
                op_number: message.op_number,
                index: self.index,
            },
        );
        self.commit_operations(message.committed, outbox);

        None
    }

    pub fn handle_prepare_ok<O>(&mut self, message: PrepareOk, outbox: &mut O) -> Option<PrepareOk>
    where
        O: Outbox<P>,
    {
        if self.need_state_transfer(message.view) {
            self.state_transfer(message.view, outbox);
            return Some(message);
        }

        if self.should_ignore_normal(message.view) || message.op_number <= self.committed {
            return None;
        }

        let prepared = self.prepared.entry(message.op_number).or_default();

        prepared.insert(message.index);

        let committed = prepared.len() >= self.configuration.sub_majority();

        if committed {
            self.prepared.retain(|&o, _| o > message.op_number);
            self.commit_operations(message.op_number, outbox);
        }

        None
    }

    pub fn handle_commit<O>(&mut self, message: Commit, outbox: &mut O) -> Option<Commit>
    where
        O: Outbox<P>,
    {
        if self.need_state_transfer(message.view) {
            self.state_transfer(message.view, outbox);
            return Some(message);
        }

        if self.should_ignore_normal(message.view) || message.committed <= self.committed {
            return None;
        }

        if !self.log.contains(&message.committed) {
            self.state_transfer(message.view, outbox);
            return Some(message);
        }

        self.commit_operations(message.committed, outbox);

        None
    }

    pub fn handle_get_state<O>(&mut self, message: GetState, outbox: &mut O) -> Option<GetState>
    where
        O: Outbox<P>,
    {
        if self.need_state_transfer(message.view) {
            self.state_transfer(message.view, outbox);
            return Some(message);
        }

        if self.should_ignore_normal(message.view) {
            return None;
        }

        outbox.new_state(
            message.index,
            NewState {
                view: self.view,
                log: self.log.after(message.op_number),
                committed: self.committed,
            },
        );

        None
    }

    pub fn handle_recovery<O>(&mut self, message: Recovery, outbox: &mut O)
    where
        O: Outbox<P>,
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
        message: RecoveryResponse<P::Request, P::Prediction>,
        outbox: &mut O,
    ) where
        O: Outbox<P>,
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
        message: NewState<P::Request, P::Prediction>,
        outbox: &mut O,
    ) where
        O: Outbox<P>,
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
        O: Outbox<P>,
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
        message: DoViewChange<P::Request, P::Prediction>,
        outbox: &mut O,
    ) where
        O: Outbox<P>,
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
        message: StartView<P::Request, P::Prediction>,
        outbox: &mut O,
    ) where
        O: Outbox<P>,
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
        O: Outbox<P>,
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
        O: Outbox<P>,
    {
        if self.view < view {
            self.log.truncate(self.committed);
        }

        let replicas = self.configuration.replicas();
        let mut replica = rand::thread_rng().gen_range(0..replicas);

        if replica == self.index {
            replica += (replica + 1) % replicas;
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
        O: Outbox<P>,
    {
        while self.committed < committed {
            let entry = &self.log[self.committed];
            let request = entry.request();
            let reply = Reply {
                view: self.view,
                id: request.id,
                payload: self.service.invoke(&request.payload, entry.prediction()),
            };

            self.committed.increment();

            if self.is_primary() {
                outbox.reply(request.client, &reply);
            }

            self.client_table.finish(request, reply);
        }
    }

    fn start_preparing_operations<O>(&mut self, outbox: &mut O)
    where
        O: Outbox<P>,
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

    fn is_primary(&self) -> bool {
        (self.configuration % self.view) == self.index
    }

    fn is_backup(&self) -> bool {
        !self.is_primary()
    }

    fn should_ignore_normal(&self, view: View) -> bool {
        self.view != view && self.status != Status::Normal
    }

    fn need_state_transfer(&self, view: View) -> bool {
        self.status == Status::Normal && view > self.view
    }

    fn should_ignore_view_change(&self, view: View) -> bool {
        self.view != view && self.status != Status::ViewChange
    }

    fn need_view_change(&self, view: View) -> bool {
        self.status != Status::Recovering && view > self.view
    }

    fn should_do_view_change(&self) -> bool {
        self.start_view_changes.len() >= self.configuration.sub_majority()
    }
}
