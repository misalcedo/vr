use crate::mail::Outbox;
use crate::protocol::{Prepare, PrepareOk};
use crate::request::Request;
use crate::Service;

pub trait Replica<S: Service> {
    fn invoke(&mut self, request: Request<S::Request>, outbox: &mut impl Outbox<Reply = S::Reply>);

    fn prepare(
        &mut self,
        prepare: Prepare<S::Request, S::Prediction>,
        outbox: &mut impl Outbox<Reply = S::Reply>,
    ) -> Option<Prepare<S::Request, S::Prediction>>;

    fn prepare_ok(&mut self, prepare_ok: PrepareOk, outbox: &mut impl Outbox<Reply = S::Reply>);
}
