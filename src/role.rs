use crate::mail::Outbox;
use crate::protocol::{Commit, GetState, NewState, Prepare, PrepareOk};
use crate::request::Request;
use crate::Service;

pub trait Role<S: Service> {
    fn request(&mut self, request: Request<S::Request>, outbox: &mut impl Outbox<Reply = S::Reply>);

    fn prepare(
        &mut self,
        prepare: Prepare<S::Request, S::Prediction>,
        outbox: &mut impl Outbox<Reply = S::Reply>,
    );

    fn prepare_ok(&mut self, prepare_ok: PrepareOk, outbox: &mut impl Outbox<Reply = S::Reply>);

    fn idle(&mut self, outbox: &mut impl Outbox<Reply = S::Reply>);

    fn commit(&mut self, commit: Commit, outbox: &mut impl Outbox<Reply = S::Reply>);

    fn get_state(&mut self, get_state: GetState, outbox: &mut impl Outbox<Reply = S::Reply>);

    fn new_state(
        &mut self,
        new_state: NewState<S::Request, S::Prediction>,
        outbox: &mut impl Outbox<Reply = S::Reply>,
    );
}
