use crate::mail::{Either, Inbox, Mailbox, Outbox};
use crate::protocol::{
    Commit, DoViewChange, GetState, Message, NewState, Prepare, PrepareOk, Protocol,
    StartViewChange,
};
use crate::replica::Replica;
use crate::request::Request;
use crate::role::Role;
use crate::viewstamp::OpNumber;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::time::Instant;

pub struct Primary {
    prepared: BTreeMap<OpNumber, HashSet<usize>>,
}
//
// impl<'a, S> Role<S> for Primary<S>
// where
//     S: Service,
//     S::Request: Clone + Serialize + Deserialize<'a>,
//     S::Prediction: Clone + Serialize + Deserialize<'a>,
//     S::Reply: Serialize + Deserialize<'a>,
// {

//
//     fn prepare(
//         &mut self,
//         _: Prepare<S::Request, S::Prediction>,
//         _: &mut impl Outbox<Reply = S::Reply>,
//     ) {
//     }
//
//
//     fn idle(&mut self, outbox: &mut impl Outbox<Reply = S::Reply>) {
//         outbox.broadcast(&self.state.get_commit());
//     }
//
//     fn commit(&mut self, _: Commit, _: &mut impl Outbox<Reply = S::Reply>) {}
//
//     fn new_state(
//         &mut self,
//         _: NewState<S::Request, S::Prediction>,
//         _: &mut impl Outbox<Reply = S::Reply>,
//     ) {
//     }
//
//     fn get_state(&mut self, get_state: GetState, outbox: &mut impl Outbox<Reply = S::Reply>) {
//         outbox.send(self.state.primary(), &self.state.get_new_state(get_state));
//     }
//
//     fn start_view_change(
//         &mut self,
//         start_view_change: StartViewChange,
//         outbox: &mut impl Outbox<Reply = S::Reply>,
//     ) {
//         todo!()
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
