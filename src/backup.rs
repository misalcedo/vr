use crate::client_table::ClientTable;
use crate::configuration::Configuration;
use crate::log::{Entry, Log};
use crate::mail::Outbox;
use crate::protocol::{Commit, GetState, NewState, Prepare, PrepareOk};
use crate::request::{Reply, Request};
use crate::role::Role;
use crate::status::Status;
use crate::viewstamp::View;
use crate::Service;
use rand::Rng;
use serde::{Deserialize, Serialize};

pub struct Backup<S>
where
    S: Service,
{
    configuration: Configuration,
    index: usize,
    view: View,
    status: Status,
    log: Log<S::Request, S::Prediction>,
    committed: usize,
    client_table: ClientTable<S::Reply>,
    service: S,
}

impl<'a, S> Backup<S>
where
    S: Service,
    S::Request: Clone + Serialize + Deserialize<'a>,
    S::Prediction: Clone + Serialize + Deserialize<'a>,
    S::Reply: Serialize + Deserialize<'a>,
{
    fn state_transfer(&mut self, latest: usize, outbox: &mut impl Outbox<Reply = S::Reply>) {
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
                op_number: self.log.len(),
                index: self.index,
            },
        );
    }
    fn commit_operations(&mut self, committed: usize, outbox: &mut impl Outbox<Reply = S::Reply>) {
        while self.committed < committed {
            match self.log.get(self.committed) {
                None => self.state_transfer(self.log.len(), outbox),
                Some(entry) => {
                    let request = entry.request();
                    let reply = Reply {
                        view: self.view,
                        id: request.id,
                        payload: self.service.invoke(&request.payload, entry.prediction()),
                    };

                    self.committed += 1;
                    self.client_table.finish(request, reply);
                }
            }
        }
    }
}

impl<'a, S> Role<S> for Backup<S>
where
    S: Service,
    S::Request: Clone + Serialize + Deserialize<'a>,
    S::Prediction: Clone + Serialize + Deserialize<'a>,
    S::Reply: Serialize + Deserialize<'a>,
{
    fn request(&mut self, _: Request<S::Request>, _: &mut impl Outbox<Reply = S::Reply>) {}

    fn prepare(
        &mut self,
        prepare: Prepare<S::Request, S::Prediction>,
        outbox: &mut impl Outbox<Reply = S::Reply>,
    ) {
        if (self.log.len() + 1) != prepare.op_number {
            self.state_transfer(self.log.len(), outbox);
            return;
        }

        self.client_table.start(&prepare.request);
        self.log
            .push(Entry::new(prepare.request, prepare.prediction));
        outbox.send(
            self.configuration % self.view,
            &PrepareOk {
                view: self.view,
                op_number: prepare.op_number,
                index: self.index,
            },
        );
        self.commit_operations(prepare.committed, outbox);
    }

    fn prepare_ok(&mut self, _: PrepareOk, _: &mut impl Outbox<Reply = S::Reply>) {}

    fn idle(&mut self, _: &mut impl Outbox<Reply = S::Reply>) {}

    fn commit(&mut self, commit: Commit, outbox: &mut impl Outbox<Reply = S::Reply>) {
        self.commit_operations(commit.committed, outbox);
    }

    fn get_state(&mut self, get_state: GetState, outbox: &mut impl Outbox<Reply = S::Reply>) {
        outbox.send(
            get_state.index,
            &NewState {
                view: self.view,
                log: self.log.after(get_state.op_number),
                op_number: self.log.len(),
                committed: self.committed,
            },
        )
    }

    fn new_state(
        &mut self,
        new_state: NewState<S::Request, S::Prediction>,
        outbox: &mut impl Outbox<Reply = S::Reply>,
    ) {
        if self.log.len() + self.log.len() == new_state.op_number {
            self.log.extend(new_state.log);
            self.commit_operations(new_state.committed, outbox);
        }
    }
}
