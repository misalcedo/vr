use crate::backup::Backup;
use crate::client_table::ClientTable;
use crate::configuration::Configuration;
use crate::log::Log;
use crate::mail::{Either, Mailbox, Outbox};
use crate::primary::Primary;
use crate::protocol::{
    Commit, DoViewChange, GetState, Message, NewState, Prepare, PrepareOk, Protocol,
    StartViewChange,
};
use crate::request::{Reply, Request};
use crate::role::Role;
use crate::service::Service;
use crate::status::Status;
use crate::viewstamp::{OpNumber, View};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

enum AdditionalState {
    Primary(Primary),
    StartViewChange,
    DoViewChange,
    Backup(Backup),
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
    additional_state: AdditionalState,
}

impl<'a, S> Replica<S>
where
    S: Service,
    S::Request: Clone + Serialize + Deserialize<'a>,
    S::Prediction: Clone + Serialize + Deserialize<'a>,
    S::Reply: Serialize + Deserialize<'a>,
{
    /// Assumes all participating replicas are in the same view.
    /// If the sender is behind, the receiver drops the message.
    /// If the sender is ahead, the replica performs a state transfer.
    pub async fn normal<M>(&mut self, mailbox: &mut M)
    where
        M: Mailbox<Request = S::Request, Prediction = S::Prediction, Reply = S::Reply>,
    {
        match mailbox.receive().await {
            Either::Left(request) => {
                self.handle_request(request, mailbox);
            }
            Either::Right(protocol) if protocol.view() < self.view => {}
            Either::Right(protocol) if protocol.view() > self.view => {
                self.view = protocol.view();
                self.state_transfer(self.committed, mailbox).await;
                self.handle_protocol(protocol, mailbox).await;
            }
            Either::Right(protocol) => {
                self.handle_protocol(protocol, mailbox).await;
            }
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

    async fn state_transfer<M>(&mut self, op_number: OpNumber, mailbox: &mut M)
    where
        M: Mailbox<Request = S::Request, Prediction = S::Prediction, Reply = S::Reply>,
    {
        self.log.truncate(op_number);

        let replicas = self.configuration.replicas();
        let mut replica = rand::thread_rng().gen_range(0..replicas);

        if replica == self.index {
            replica += (replica + 1) % replicas;
        }

        mailbox.send(
            self.configuration % self.view,
            &GetState {
                view: self.view,
                op_number: self.log.last_op_number(),
                index: self.index,
            },
        );

        let new_state = mailbox
            .receive_response(|m: &NewState<S::Request, S::Prediction>| {
                m.view == self.view && m.log.first_op_number() == self.log.next_op_number()
            })
            .await;

        self.log.extend(new_state.log);
        self.commit_operations(new_state.committed, mailbox);
    }

    fn handle_request<O>(&mut self, request: Request<S::Request>, outbox: &mut O)
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

    fn commit_operations(
        &mut self,
        committed: OpNumber,
        outbox: &mut impl Outbox<Reply = S::Reply>,
    ) {
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

    async fn handle_protocol<M>(
        &mut self,
        protocol: Protocol<S::Request, S::Prediction>,
        mailbox: &mut M,
    ) where
        M: Mailbox<Request = S::Request, Prediction = S::Prediction, Reply = S::Reply>,
    {
        match protocol {
            Protocol::Prepare(prepare) if self.is_backup() => {
                self.handle_prepare(prepare, mailbox).await
            }
            Protocol::PrepareOk(_) => {}
            Protocol::Commit(_) => {}
            Protocol::GetState(_) => {}
            Protocol::NewState(_) => {}
            Protocol::StartViewChange(_) => {}
            Protocol::DoViewChange(_) => {}
            _ => (),
        }
    }

    async fn handle_prepare<M>(
        &mut self,
        prepare: Prepare<S::Request, S::Prediction>,
        mailbox: &mut M,
    ) where
        M: Mailbox<Request = S::Request, Prediction = S::Prediction, Reply = S::Reply>,
    {
        let next = self.log.next_op_number();

        if prepare.op_number < next {
            return;
        }

        if next < prepare.op_number {
            self.state_transfer(self.log.last_op_number(), mailbox)
                .await;
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
}

impl<'a, S> Replica<S>
where
    S: Service,
    S::Request: Clone + Serialize + Deserialize<'a>,
    S::Prediction: Clone + Serialize + Deserialize<'a>,
    S::Reply: Serialize + Deserialize<'a>,
{
    pub fn is_primary(&self) -> bool {
        (self.configuration % self.view) == self.index
    }

    pub fn is_backup(&self) -> bool {
        !self.is_primary()
    }

    pub fn is_sub_majority(&self, value: usize) -> bool {
        self.configuration.sub_majority() <= value
    }

    pub fn is_quorum(&self, value: usize) -> bool {
        self.configuration.quorum() <= value
    }

    pub fn get_new_state(&self, get_state: GetState) -> NewState<S::Request, S::Prediction> {
        NewState {
            view: self.view,
            log: self.log.after(get_state.op_number),
            committed: self.committed,
        }
    }

    pub fn new_do_view_change(&self) -> DoViewChange<S::Request, S::Prediction> {
        todo!()
    }
}
