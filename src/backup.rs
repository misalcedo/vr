use crate::mail::Outbox;
use crate::protocol::{
    Commit, DoViewChange, GetState, NewState, Prepare, PrepareOk, StartViewChange,
};
use crate::request::Request;
use crate::role::Role;
use crate::state::State;
use crate::Service;
use serde::{Deserialize, Serialize};

pub struct Backup<S>
where
    S: Service,
{
    state: State<S>,
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
        match self.state.prepare_operation(prepare, outbox) {
            Ok(_) => (),
            Err(_) => {
                todo!("ensure we can properly wait for a state transfer message")
            }
        }
    }

    fn prepare_ok(&mut self, _: PrepareOk, _: &mut impl Outbox<Reply = S::Reply>) {}

    fn idle(&mut self, outbox: &mut impl Outbox<Reply = S::Reply>) {
        outbox.broadcast(&self.state.start_view_change());
    }

    fn commit(&mut self, commit: Commit, outbox: &mut impl Outbox<Reply = S::Reply>) {
        match self
            .state
            .commit_operations_with_state_transfer(commit.committed, outbox)
        {
            Ok(_) => (),
            Err(_) => {
                todo!("ensure we can properly wait for a state transfer message")
            }
        }
    }

    fn get_state(&mut self, get_state: GetState, outbox: &mut impl Outbox<Reply = S::Reply>) {
        outbox.send(get_state.index, &self.state.get_new_state(get_state))
    }

    fn new_state(
        &mut self,
        new_state: NewState<S::Request, S::Prediction>,
        outbox: &mut impl Outbox<Reply = S::Reply>,
    ) {
        self.state.update_state(new_state, outbox);
    }

    fn start_view_change(
        &mut self,
        start_view_change: StartViewChange,
        outbox: &mut impl Outbox<Reply = S::Reply>,
    ) {
        todo!()
    }

    fn do_view_change(
        &mut self,
        do_view_change: DoViewChange<S::Request, S::Prediction>,
        outbox: &mut impl Outbox<Reply = S::Reply>,
    ) {
        todo!()
    }
}
