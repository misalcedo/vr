use crate::mail::{Either, Inbox, Mailbox, Outbox};
use crate::protocol::{
    Commit, DoViewChange, GetState, Message, NewState, Prepare, PrepareOk, Protocol,
    StartViewChange,
};
use crate::request::Request;
use crate::role::Role;
use crate::state::State;
use crate::viewstamp::OpNumber;
use crate::Service;
use futures::future;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::future::Future;

pub struct Primary<S>
where
    S: Service,
{
    state: State<S>,
    prepared: BTreeMap<OpNumber, HashSet<usize>>,
}

impl<'a, S> Primary<S>
where
    S: Service,
    S::Request: Clone + Serialize + Deserialize<'a>,
    S::Prediction: Clone + Serialize + Deserialize<'a>,
    S::Reply: Serialize + Deserialize<'a>,
{
    /// Assumes all participating replicas are in the same view.
    /// If the sender is behind, the receiver drops the message.
    /// If the sender is ahead, the replica performs a state transfer.
    pub async fn normal<M, F>(&mut self, mailbox: &mut M, idle_timeout: &mut F)
    where
        M: Mailbox<Request = S::Request, Prediction = S::Prediction, Reply = S::Reply>,
        F: Future + Unpin,
    {
        let result = match future::select(mailbox.receive(), idle_timeout).await {
            future::Either::Left((message, _)) => Ok(message),
            future::Either::Right((_, _)) => Err(()),
        };

        match result {
            Ok(Either::Left(request)) => self.request(request, mailbox),
            Ok(Either::Right(protocol)) if protocol.view() < self.state.view() => {}
            Ok(Either::Right(protocol)) if protocol.view() > self.state.view() => {
                self.state.state_transfer(mailbox).await;
            }
            Ok(Either::Right(protocol)) => {}
            Err(_) => mailbox.broadcast(&self.state.get_commit()),
        }
    }
}

impl<'a, S> Role<S> for Primary<S>
where
    S: Service,
    S::Request: Clone + Serialize + Deserialize<'a>,
    S::Prediction: Clone + Serialize + Deserialize<'a>,
    S::Reply: Serialize + Deserialize<'a>,
{
    fn request(
        &mut self,
        request: Request<S::Request>,
        outbox: &mut impl Outbox<Reply = S::Reply>,
    ) {
        match self.state.prepare_request(request) {
            Some(Ok(prepare)) => outbox.broadcast(&prepare),
            Some(Err((client, reply))) => outbox.reply(client, reply),
            None => (),
        }
    }

    fn prepare(
        &mut self,
        _: Prepare<S::Request, S::Prediction>,
        _: &mut impl Outbox<Reply = S::Reply>,
    ) {
    }

    fn prepare_ok(&mut self, prepare_ok: PrepareOk, outbox: &mut impl Outbox<Reply = S::Reply>) {
        let prepared = self.prepared.entry(prepare_ok.op_number).or_default();

        prepared.insert(prepare_ok.index);

        if self.state.is_sub_majority(prepared.len()) {
            self.prepared.retain(|&o, _| o > prepare_ok.op_number);
            self.state
                .commit_operations_with_reply(prepare_ok.op_number, outbox);
        }
    }

    fn idle(&mut self, outbox: &mut impl Outbox<Reply = S::Reply>) {
        outbox.broadcast(&self.state.get_commit());
    }

    fn commit(&mut self, _: Commit, _: &mut impl Outbox<Reply = S::Reply>) {}

    fn new_state(
        &mut self,
        _: NewState<S::Request, S::Prediction>,
        _: &mut impl Outbox<Reply = S::Reply>,
    ) {
    }

    fn get_state(&mut self, get_state: GetState, outbox: &mut impl Outbox<Reply = S::Reply>) {
        outbox.send(self.state.primary(), &self.state.get_new_state(get_state));
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
