use crate::client_table::ClientTable;
use crate::configuration::Configuration;
use crate::log::Log;
use crate::mail::{Mailbox, Outbox};
use crate::protocol::{
    Commit, DoViewChange, GetState, Message, NewState, Prepare, PrepareOk, Protocol, StartView,
    StartViewChange,
};
use crate::request::{Reply, Request};
use crate::service::Service;
use crate::status::Status;
use crate::viewstamp::{OpNumber, View};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

pub struct Replica<S>
where
    S: Service,
{
    configuration: Configuration,
    index: usize,
    view: View,
    status: Status,
    log: Log<S::Request, S::Prediction>,
    committed: OpNumber,
    client_table: ClientTable<S::Reply>,
    service: S,
    prepared: BTreeMap<OpNumber, HashSet<usize>>,
    start_view_changes: (View, HashSet<usize>),
    do_view_changes: HashMap<usize, DoViewChange<S::Request, S::Prediction>>,
}

impl<'a, S> Replica<S>
where
    S: Service,
    S::Request: Clone + Serialize + Deserialize<'a>,
    S::Prediction: Clone + Serialize + Deserialize<'a>,
    S::Reply: Serialize + Deserialize<'a>,
{
    pub fn handle_request<O>(&mut self, request: Request<S::Request>, outbox: &mut O)
    where
        O: Outbox<Reply = S::Reply>,
    {
        if self.is_backup() {
            return;
        }

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

    /// Assumes all participating replicas are in the same view.
    /// If the sender is behind, the receiver drops the message.
    /// If the sender is ahead, the replica performs a state transfer.
    pub fn handle_protocol<M>(
        &mut self,
        protocol: Protocol<S::Request, S::Prediction>,
        mailbox: &mut M,
    ) where
        M: Mailbox<Reply = S::Reply>,
    {
        match (self.status, protocol) {
            (_, p) if p.view() < self.view => {}
            (Status::Normal, Protocol::NewState(message)) => {
                self.handle_new_state(message, mailbox)
            }
            (Status::Normal | Status::ViewChange, Protocol::StartViewChange(message)) => {
                self.handle_start_view_change(message, mailbox)
            }
            (Status::Normal | Status::ViewChange, Protocol::DoViewChange(message)) => {
                self.handle_do_view_change(message, mailbox)
            }
            (_, p) if p.view() > self.view => {
                self.state_transfer(p.view(), mailbox);
                mailbox.send(self.index, &p);
            }
            (Status::Normal, Protocol::Prepare(message)) => self.handle_prepare(message, mailbox),
            (Status::Normal, Protocol::PrepareOk(message)) => {
                self.handle_prepare_ok(message, mailbox)
            }
            (Status::Normal, Protocol::Commit(message)) => self.handle_commit(message, mailbox),
            (Status::Normal, Protocol::GetState(message)) => {
                self.handle_get_state(message, mailbox)
            }
            _ => (),
        }
    }

    pub fn idle<O>(&mut self, outbox: &mut O)
    where
        O: Outbox<Reply = S::Reply>,
    {
        if self.is_primary() {
            outbox.broadcast(&Commit {
                view: self.view,
                committed: self.committed,
            });
        } else {
            self.view.increment();
            self.start_view_change(outbox);
        }
    }

    fn handle_prepare<M>(&mut self, prepare: Prepare<S::Request, S::Prediction>, mailbox: &mut M)
    where
        M: Mailbox<Reply = S::Reply>,
    {
        if self.is_primary() || self.log.contains(&prepare.op_number) {
            return;
        }

        let next = self.log.next_op_number();
        if next < prepare.op_number || next < prepare.committed {
            self.state_transfer(self.view, mailbox);
            mailbox.send(self.index, &prepare);
        }

        self.client_table.start(&prepare.request);
        self.log
            .push(self.view, prepare.request, prepare.prediction);
        mailbox.send(
            self.configuration % self.view,
            &PrepareOk {
                view: self.view,
                op_number: prepare.op_number,
                index: self.index,
            },
        );
        self.commit_operations(prepare.committed, mailbox);
    }

    fn handle_prepare_ok<O>(&mut self, prepare_ok: PrepareOk, outbox: &mut O)
    where
        O: Outbox<Reply = S::Reply>,
    {
        if self.is_backup() || prepare_ok.op_number <= self.committed {
            return;
        }

        let prepared = self.prepared.entry(prepare_ok.op_number).or_default();

        prepared.insert(prepare_ok.index);

        let committed = prepared.len() >= self.configuration.sub_majority();

        if committed {
            self.prepared.retain(|&o, _| o > prepare_ok.op_number);
            self.commit_operations(prepare_ok.op_number, outbox);
        }
    }

    fn handle_commit<M>(&mut self, commit: Commit, mailbox: &mut M)
    where
        M: Mailbox<Reply = S::Reply>,
    {
        if self.is_primary() || commit.committed <= self.committed {
            return;
        }

        if !self.log.contains(&commit.committed) {
            self.state_transfer(self.view, mailbox);
            mailbox.send(self.index, &commit);
        }

        self.commit_operations(commit.committed, mailbox);
    }

    fn handle_get_state<O>(&mut self, get_state: GetState, outbox: &mut O)
    where
        O: Outbox<Reply = S::Reply>,
    {
        outbox.send(
            get_state.index,
            &NewState {
                view: self.view,
                log: self.log.after(get_state.op_number),
                committed: self.committed,
            },
        )
    }

    fn handle_new_state<O>(
        &mut self,
        new_state: NewState<S::Request, S::Prediction>,
        outbox: &mut O,
    ) where
        O: Outbox<Reply = S::Reply>,
    {
        if new_state.log.first_op_number() == self.log.next_op_number() {
            self.view = new_state.view;
            self.log.extend(new_state.log);
            self.commit_operations(new_state.committed, outbox);
        }
    }

    fn handle_start_view_change<O>(&mut self, start_view_change: StartViewChange, outbox: &mut O)
    where
        O: Outbox<Reply = S::Reply>,
    {
        if start_view_change.view > self.start_view_changes.0 {
            self.start_view_change(outbox);
            self.start_view_changes.0 = start_view_change.view
        }

        self.start_view_changes.1.insert(start_view_change.index);
        if self.start_view_changes.1.len() >= self.configuration.sub_majority() {
            outbox.send(
                self.configuration % self.view,
                &DoViewChange {
                    view: self.view,
                    last_normal_view: self.start_view_changes.0,
                    log: self.log.clone(),
                    committed: self.committed,
                    index: self.index,
                },
            )
        }
    }

    fn handle_do_view_change<O>(
        &mut self,
        do_view_change: DoViewChange<S::Request, S::Prediction>,
        outbox: &mut O,
    ) where
        O: Outbox<Reply = S::Reply>,
    {
        if do_view_change.view > self.start_view_changes.0 {
            self.start_view_change(outbox);
        }

        if self.is_backup() {
            return;
        }

        self.do_view_changes
            .insert(do_view_change.index, do_view_change);

        if self.do_view_changes.contains_key(&self.index)
            && self.do_view_changes.len() >= self.configuration.quorum()
        {
            let committed = self
                .do_view_changes
                .values()
                .map(|v| v.committed)
                .max()
                .unwrap_or(self.committed);
            let max = self
                .do_view_changes
                .drain()
                .map(|(k, v)| v)
                .max_by_key(|m| (m.last_normal_view, m.log.last_op_number()));

            if let Some(do_view_change) = max {
                self.log = do_view_change.log;
                self.view = do_view_change.view;
                self.set_status(Status::Normal);

                outbox.broadcast(&StartView {
                    view: self.view,
                    log: self.log.clone(),
                    committed,
                });

                self.commit_operations(committed, outbox);
            }
        }
    }

    fn start_view_change<O>(&mut self, outbox: &mut O)
    where
        O: Outbox<Reply = S::Reply>,
    {
        self.set_status(Status::ViewChange);

        outbox.broadcast(&StartViewChange {
            view: self.view,
            index: self.index,
        });
    }

    fn state_transfer<M>(&mut self, view: View, mailbox: &mut M)
    where
        M: Mailbox<Reply = S::Reply>,
    {
        if self.view < view {
            self.log.truncate(self.committed);
        }

        let replicas = self.configuration.replicas();
        let mut replica = rand::thread_rng().gen_range(0..replicas);

        if replica == self.index {
            replica += (replica + 1) % replicas;
        }

        mailbox.send(
            replica,
            &GetState {
                view: self.view,
                op_number: self.log.last_op_number(),
                index: self.index,
            },
        );
    }

    fn is_primary(&self) -> bool {
        (self.configuration % self.view) == self.index
    }

    fn is_backup(&self) -> bool {
        !self.is_primary()
    }

    fn is_normal(&self) -> bool {
        self.status == Status::Normal
    }

    fn is_recovering(&self) -> bool {
        self.status == Status::Recovering
    }

    fn is_view_change(&self) -> bool {
        self.status == Status::ViewChange
    }

    pub fn is_quorum(&self, value: usize) -> bool {
        self.configuration.quorum() <= value
    }

    fn commit_operations(
        &mut self,
        committed: OpNumber,
        outbox: &mut impl Outbox<Reply = S::Reply>,
    ) {
        if self.committed < committed {
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

    fn set_status(&mut self, status: Status) {
        self.status = status;
        self.prepared.clear();
        self.start_view_changes = (self.view, Default::default());
        self.do_view_changes = Default::default();
    }
}
