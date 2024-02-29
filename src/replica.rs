use crate::client_table::ClientTable;
use crate::configuration::Configuration;
use crate::log::Log;
use crate::mail::{Mailbox, Outbox};
use crate::protocol::{
    Commit, DoViewChange, GetState, Message, NewState, Prepare, PrepareOk, Protocol, Recovery,
    RecoveryResponse, StartView, StartViewChange,
};
use crate::request::{Reply, Request, RequestIdentifier};
use crate::service::Service;
use crate::status::Status;
use crate::viewstamp::{OpNumber, View};
use rand::Rng;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

pub struct Replica<'a, S>
where
    S: Service<'a>,
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
    nonce: RequestIdentifier,
    checkpoints: VecDeque<OpNumber>,
}

impl<'a, S> Replica<'a, S>
where
    S: Service<'a>,
{
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

    pub fn recovering<O>(
        configuration: Configuration,
        index: usize,
        service: S,
        outbox: &mut O,
    ) -> Self
    where
        O: Outbox,
    {
        let mut replica = Self::new(configuration, index, service);

        replica.status = Status::Recovering;

        outbox.broadcast(&Recovery {
            index,
            nonce: replica.nonce,
        });

        replica
    }

    pub fn checkpoint(&mut self) -> S::Checkpoint {
        self.checkpoints.push_back(self.log.last_op_number());
        self.service.checkpoint()
    }

    pub fn compact(&mut self, suffix: usize) {
        if self.checkpoints.len() <= suffix {
            return;
        }

        let index = self.checkpoints.len() - suffix;
        let checkpoint = self.checkpoints.drain(..index).last();
        let cutoff = checkpoint.unwrap_or_default().next();

        self.log.compact(cutoff);
    }

    pub fn idle<O>(&mut self, outbox: &mut O)
    where
        O: Outbox,
    {
        if self.is_primary() {
            outbox.broadcast(&Commit {
                view: self.view,
                committed: self.committed,
            });
        } else {
            self.start_view_change(self.view.next(), outbox);
        }
    }

    pub fn handle_request<O>(&mut self, request: Request<S::Request>, outbox: &mut O)
    where
        O: Outbox,
    {
        let (cached_request, comparison) = self.client_table.get_mut(&request);

        match comparison {
            Ordering::Greater => {
                let prediction = self.service.predict(&request.payload);
                let (entry, op_number) = self.log.push(self.view, request, prediction);

                outbox.broadcast(&Prepare {
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

    pub fn handle_prepare<O>(&mut self, message: Prepare<S::Request, S::Prediction>, outbox: &mut O)
    where
        O: Outbox,
    {
        if self.need_state_transfer(message.view) {
            self.start_state_transfer(message, outbox);
            return;
        }

        if self.should_ignore_normal(message.view) || self.log.contains(&message.op_number) {
            return;
        }

        let next = self.log.next_op_number();
        if next < message.op_number || next < message.committed {
            self.state_transfer(self.view, outbox);
            outbox.send(self.index, &message);
        }

        self.client_table.start(&message.request);
        self.log
            .push(self.view, message.request, message.prediction);
        outbox.send(
            self.configuration % self.view,
            &PrepareOk {
                view: self.view,
                op_number: message.op_number,
                index: self.index,
            },
        );
        self.commit_operations(message.committed, outbox);
    }

    pub fn handle_prepare_ok<O>(&mut self, message: PrepareOk, outbox: &mut O)
    where
        O: Outbox,
    {
        if self.need_state_transfer(message.view) {
            self.start_state_transfer(message, outbox);
            return;
        }

        if self.should_ignore_normal(message.view) || message.op_number <= self.committed {
            return;
        }

        let prepared = self.prepared.entry(message.op_number).or_default();

        prepared.insert(message.index);

        let committed = prepared.len() >= self.configuration.sub_majority();

        if committed {
            self.prepared.retain(|&o, _| o > message.op_number);
            self.commit_operations(message.op_number, outbox);
        }
    }

    pub fn handle_commit<O>(&mut self, message: Commit, outbox: &mut O)
    where
        O: Outbox,
    {
        if self.need_state_transfer(message.view) {
            self.start_state_transfer(message, outbox);
            return;
        }

        if self.should_ignore_normal(message.view) || message.committed <= self.committed {
            return;
        }

        if !self.log.contains(&message.committed) {
            self.state_transfer(self.view, outbox);
            outbox.send(self.index, &message);
        }

        self.commit_operations(message.committed, outbox);
    }

    pub fn handle_get_state<O>(&mut self, message: GetState, outbox: &mut O)
    where
        O: Outbox,
    {
        if self.need_state_transfer(message.view) {
            self.start_state_transfer(message, outbox);
            return;
        }

        if self.should_ignore_normal(message.view) {
            return;
        }

        outbox.send(
            message.index,
            &NewState {
                view: self.view,
                log: self.log.after(message.op_number),
                committed: self.committed,
            },
        );
    }

    pub fn handle_recovery<O>(&mut self, message: Recovery, outbox: &mut O)
    where
        O: Outbox,
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

        outbox.send(message.index, &response);
    }

    pub fn handle_recovery_response<O>(
        &mut self,
        message: RecoveryResponse<S::Request, S::Prediction>,
        outbox: &mut O,
    ) where
        O: Outbox,
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
        O: Outbox,
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
        O: Outbox,
    {
        if self.need_view_change(message.view) {
            self.start_view_change(message.view, outbox);
        }

        if self.should_ignore_view_change(message.view) {
            return;
        }

        self.start_view_changes.insert(message.index);

        if self.start_view_changes.len() >= self.configuration.sub_majority() {
            outbox.send(
                self.configuration % self.view,
                &DoViewChange {
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
        O: Outbox,
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

                outbox.broadcast(&StartView {
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
        O: Outbox,
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
        O: Outbox,
    {
        self.view = view;

        self.set_status(Status::ViewChange);

        outbox.broadcast(&StartViewChange {
            view: self.view,
            index: self.index,
        });
    }

    fn start_state_transfer<'m, M>(&mut self, message: M, outbox: &mut impl Outbox)
    where
        M: Message<'m>,
    {
        self.state_transfer(message.view(), outbox);
        outbox.send(self.index, &message);
    }

    fn state_transfer<O>(&mut self, view: View, outbox: &mut O)
    where
        O: Outbox,
    {
        if self.view < view {
            self.log.truncate(self.committed);
        }

        let replicas = self.configuration.replicas();
        let mut replica = rand::thread_rng().gen_range(0..replicas);

        if replica == self.index {
            replica += (replica + 1) % replicas;
        }

        outbox.send(
            replica,
            &GetState {
                view: self.view,
                op_number: self.log.last_op_number(),
                index: self.index,
            },
        );
    }

    fn commit_operations(&mut self, committed: OpNumber, outbox: &mut impl Outbox) {
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

    fn start_preparing_operations(&mut self, outbox: &mut impl Outbox) {
        let mut current = self.committed.next();

        while self.log.contains(&current) {
            let entry = &self.log[current];
            let request = entry.request();

            self.client_table.start(request);

            if self.is_backup() {
                outbox.send(
                    self.configuration % self.view,
                    &PrepareOk {
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
}
