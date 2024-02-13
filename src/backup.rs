use crate::client_table::ClientTable;
use crate::configuration::Configuration;
use crate::log::{Entry, Log};
use crate::mail::Outbox;
use crate::protocol::{Commit, Prepare, PrepareOk};
use crate::request::{Reply, Request};
use crate::role::Role;
use crate::status::Status;
use crate::viewstamp::View;
use crate::Service;
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
    fn commit_operations(&mut self, committed: usize) {
        while self.committed < committed {
            match self.log.get(self.committed) {
                None => {
                    todo!("Perform state transfer")
                }
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
        if (self.log.op_number() + 1) != prepare.op_number {
            // TODO: Start state transfer
            return;
        }

        self.client_table.start(&prepare.request);
        self.log
            .push(Entry::new(prepare.request, prepare.prediction));
        outbox.send(&PrepareOk {
            view: self.view,
            op_number: prepare.op_number,
            index: self.index,
        });
        self.commit_operations(prepare.committed);
    }

    fn prepare_ok(&mut self, _: PrepareOk, _: &mut impl Outbox<Reply = S::Reply>) {}

    fn idle(&mut self, _: &mut impl Outbox<Reply = S::Reply>) {}

    fn commit(&mut self, commit: Commit, _: &mut impl Outbox<Reply = S::Reply>) {
        self.commit_operations(commit.committed);
    }
}
