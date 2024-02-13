use crate::client_table::ClientTable;
use crate::configuration::Configuration;
use crate::log::{Entry, Log};
use crate::mail::Outbox;
use crate::protocol::{Prepare, PrepareOk};
use crate::replica::Replica;
use crate::request::Request;
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
}

impl<'a, S> Replica<S> for Backup<S>
where
    S: Service,
    S::Request: Clone + Serialize + Deserialize<'a>,
    S::Prediction: Clone + Serialize + Deserialize<'a>,
    S::Reply: Serialize + Deserialize<'a>,
{
    fn invoke(&mut self, _: Request<S::Request>, _: &mut impl Outbox<Reply = S::Reply>) {}

    fn prepare(
        &mut self,
        prepare: Prepare<S::Request, S::Prediction>,
        outbox: &mut impl Outbox<Reply = S::Reply>,
    ) -> Option<Prepare<S::Request, S::Prediction>> {
        if (self.log.op_number() + 1) != prepare.op_number {
            // TODO: Start state transfer
            return Some(prepare);
        }

        self.client_table.start(&prepare.request);
        self.log
            .push(Entry::new(prepare.request, prepare.prediction));
        outbox.send(&PrepareOk {
            view: self.view,
            op_number: prepare.op_number,
            index: self.index,
        });

        None
    }

    fn prepare_ok(&mut self, _: PrepareOk, _: &mut impl Outbox<Reply = S::Reply>) {}
}
