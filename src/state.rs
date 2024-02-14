use crate::client_table::ClientTable;
use crate::configuration::Configuration;
use crate::log::Log;
use crate::mail::Outbox;
use crate::protocol::{
    Commit, DoViewChange, GetState, NewState, Prepare, PrepareOk, StartViewChange,
};
use crate::request::{ClientIdentifier, Reply, Request};
use crate::status::Status;
use crate::viewstamp::{OpNumber, View};
use crate::Service;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

pub struct State<S>
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
}

impl<'a, S> State<S>
where
    S: Service,
    S::Request: Clone + Serialize + Deserialize<'a>,
    S::Prediction: Clone + Serialize + Deserialize<'a>,
    S::Reply: Serialize + Deserialize<'a>,
{
    pub fn primary(&self) -> usize {
        self.configuration % self.view
    }

    pub fn is_sub_majority(&self, value: usize) -> bool {
        self.configuration.sub_majority() <= value
    }

    pub fn is_quorum(&self, value: usize) -> bool {
        self.configuration.quorum() <= value
    }

    pub fn prepare_request(
        &mut self,
        request: Request<S::Request>,
    ) -> Option<Result<Prepare<S::Request, S::Prediction>, (ClientIdentifier, &Reply<S::Reply>)>>
    {
        let (cached_request, comparison) = self.client_table.get_mut(&request);

        match comparison {
            Ordering::Greater => {
                let prediction = self.service.predict(&request.payload);
                let entry = self.log.push(self.view, request, prediction);

                Some(Ok(Prepare {
                    view: self.view,
                    op_number: entry.viewstamp().op_number(),
                    request: entry.request().clone(),
                    prediction: entry.prediction().clone(),
                    committed: self.committed,
                }))
            }
            Ordering::Equal => Some(Err((request.client, cached_request.reply()?))),
            Ordering::Less => None,
        }
    }

    pub fn prepare_operation(
        &mut self,
        prepare: Prepare<S::Request, S::Prediction>,
        outbox: &mut impl Outbox<Reply = S::Reply>,
    ) -> Result<OpNumber, OpNumber> {
        if self.log.next_op_number() != prepare.op_number {
            let latest = self.log.last_op_number();
            self.start_state_transfer(latest, outbox);
            return Err(latest);
        }

        self.client_table.start(&prepare.request);
        self.log
            .push(self.view, prepare.request, prepare.prediction);
        outbox.send(
            self.configuration % self.view,
            &PrepareOk {
                view: self.view,
                op_number: prepare.op_number,
                index: self.index,
            },
        );
        self.commit_operations_with_state_transfer(prepare.committed, outbox)
    }

    pub fn commit_operations_with_reply(
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
            outbox.reply(request.client, &reply);
            self.client_table.finish(request, reply);
        }
    }

    pub fn commit_operations_with_state_transfer<O: Outbox<Reply = S::Reply>>(
        &mut self,
        committed: OpNumber,
        outbox: &mut O,
    ) -> Result<OpNumber, OpNumber> {
        while self.committed < committed {
            match self.log.get(self.committed) {
                None => {
                    let latest = self.log.last_op_number();
                    self.start_state_transfer(latest, outbox);
                    return Err(latest);
                }
                Some(entry) => {
                    let request = entry.request();
                    let reply = Reply {
                        view: self.view,
                        id: request.id,
                        payload: self.service.invoke(&request.payload, entry.prediction()),
                    };

                    self.committed.increment();
                    self.client_table.finish(request, reply);
                }
            }
        }

        Ok(self.committed)
    }

    pub fn get_commit(&self) -> Commit {
        Commit {
            view: self.view,
            committed: self.committed,
        }
    }

    pub fn get_new_state(&self, get_state: GetState) -> NewState<S::Request, S::Prediction> {
        NewState {
            view: self.view,
            log: self.log.after(get_state.op_number),
            committed: self.committed,
        }
    }

    fn start_state_transfer(
        &mut self,
        latest: OpNumber,
        outbox: &mut impl Outbox<Reply = S::Reply>,
    ) {
        self.log.truncate(latest);

        let replicas = self.configuration.replicas();
        let mut replica = rand::thread_rng().gen_range(0..replicas);

        if replica == self.index {
            replica += (replica + 1) % replicas;
        }

        outbox.send(
            self.configuration % self.view,
            &GetState {
                view: self.view,
                op_number: self.log.last_op_number(),
                index: self.index,
            },
        );
    }

    pub fn update_state(
        &mut self,
        new_state: NewState<S::Request, S::Prediction>,
        outbox: &mut impl Outbox<Reply = S::Reply>,
    ) {
        if self.log.next_op_number() == new_state.log.first_op_number() {
            self.log.extend(new_state.log);
            self.commit_operations_with_state_transfer(new_state.committed, outbox);
        }
    }

    pub fn start_view_change(&mut self) -> StartViewChange {
        self.view.increment();
        self.status = Status::ViewChange;

        StartViewChange {
            view: self.view,
            index: self.index,
        }
    }

    pub fn new_do_view_change(&self) -> DoViewChange<S::Request, S::Prediction> {
        todo!()
    }
}
