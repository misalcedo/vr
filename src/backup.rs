use crate::mail::Outbox;
use crate::protocol::{
    Commit, DoViewChange, GetState, NewState, Prepare, PrepareOk, StartViewChange,
};
use crate::request::Request;
use crate::role::Role;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub struct Backup {
    start_view_changes: HashSet<usize>,
}
//
// impl<'a, S> Role<S> for Backup<S>
// where
//     S: Service,
//     S::Request: Clone + Serialize + Deserialize<'a>,
//     S::Prediction: Clone + Serialize + Deserialize<'a>,
//     S::Reply: Serialize + Deserialize<'a>,
// {
//
//     fn prepare_ok(&mut self, _: PrepareOk, _: &mut impl Outbox<Reply = S::Reply>) {}
//
//     fn commit(&mut self, commit: Commit, outbox: &mut impl Outbox<Reply = S::Reply>) {
//         match self
//             .state
//             .commit_operations_with_state_transfer(commit.committed, outbox)
//         {
//             Ok(_) => (),
//             Err(_) => {
//                 todo!("ensure we can properly wait for a state transfer message")
//             }
//         }
//     }
//
//     fn get_state(&mut self, get_state: GetState, outbox: &mut impl Outbox<Reply = S::Reply>) {
//         outbox.send(get_state.index, &self.state.get_new_state(get_state))
//     }
//
//     fn new_state(
//         &mut self,
//         new_state: NewState<S::Request, S::Prediction>,
//         outbox: &mut impl Outbox<Reply = S::Reply>,
//     ) {
//         self.state.update_state(new_state, outbox);
//     }
//
//     fn start_view_change(
//         &mut self,
//         start_view_change: StartViewChange,
//         outbox: &mut impl Outbox<Reply = S::Reply>,
//     ) {
//         self.start_view_changes.insert(start_view_change.index);
//         if self.state.is_sub_majority(self.start_view_changes.len()) {
//             outbox.send(self.state.primary(), &self.state.new_do_view_change())
//         }
//     }
//
//     fn do_view_change(
//         &mut self,
//         do_view_change: DoViewChange<S::Request, S::Prediction>,
//         outbox: &mut impl Outbox<Reply = S::Reply>,
//     ) {
//         todo!()
//     }
// }
