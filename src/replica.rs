use crate::backup::Backup;
use crate::client_table::ClientTable;
use crate::configuration::Configuration;
use crate::log::Log;
use crate::mail::{Mailbox, Outbox};
use crate::protocol::{
    Commit, GetState, Message, NewState, Prepare, PrepareOk, Protocol, StartViewChange,
};
use crate::request::{Reply, Request};
use crate::role::Role;
use crate::service::Service;
use crate::status::Status;
use crate::viewstamp::{OpNumber, View};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

struct Primary {
    prepared: BTreeMap<OpNumber, HashSet<usize>>,
}

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
    primary: Primary,
    backup: Backup,
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
                let entry = self.log.push(self.view, request, prediction);

                outbox.broadcast(&Prepare {
                    view: self.view,
                    op_number: entry.viewstamp().op_number(),
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
        if protocol.view() < self.view {
            return;
        }

        if protocol.view() > self.view {
            self.state_transfer(protocol.view(), mailbox);
        }

        match protocol {
            Protocol::Prepare(message) => self.handle_prepare(message, mailbox),
            Protocol::PrepareOk(message) => self.handle_prepare_ok(message, mailbox),
            Protocol::Commit(message) => self.handle_commit(message, mailbox),
            Protocol::GetState(_) if self.is_normal() => {}
            Protocol::StartViewChange(_) => {}
            Protocol::DoViewChange(_) => {}
            _ => (),
        }
    }

    pub async fn idle<O>(&mut self, outbox: &mut O)
    where
        O: Outbox<Reply = S::Reply>,
    {
        if self.is_primary() {
            outbox.broadcast(&Commit {
                view: self.view,
                committed: self.committed,
            })
        } else {
            outbox.broadcast(&StartViewChange {
                view: self.view,
                index: self.index,
            });
        }
    }

    fn handle_prepare<M>(&mut self, prepare: Prepare<S::Request, S::Prediction>, mailbox: &mut M)
    where
        M: Mailbox<Reply = S::Reply>,
    {
        if self.is_primary() || self.log.contains(prepare.op_number) {
            return;
        }

        let next = self.log.next_op_number();
        if next < prepare.op_number || next < prepare.committed {
            self.state_transfer(self.view, mailbox);
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

        let prepared = self
            .primary
            .prepared
            .entry(prepare_ok.op_number)
            .or_default();

        prepared.insert(prepare_ok.index);

        let committed = prepared.len() >= self.configuration.sub_majority();

        if committed {
            self.primary
                .prepared
                .retain(|&o, _| o > prepare_ok.op_number);
            self.commit_operations(prepare_ok.op_number, outbox);
        }
    }

    fn handle_commit<M>(&mut self, commit: Commit, mailbox: &mut M)
    where
        M: Mailbox<Reply = S::Reply>,
    {
        if self.is_primary() {
            return;
        }

        if self.log.last_op_number() < commit.committed {
            self.state_transfer(self.view, mailbox);
        }

        self.commit_operations(self.committed, mailbox);
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

        let new_state: NewState<S::Request, S::Prediction> = mailbox.receive();

        self.log.extend(new_state.log);
        self.commit_operations(new_state.committed, mailbox);
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
}
