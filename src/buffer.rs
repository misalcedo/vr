use crate::mail::{Inbox, Mailbox, Outbox};
use crate::protocol::{
    Commit, DoViewChange, GetState, NewState, Prepare, PrepareOk, Recovery, RecoveryResponse,
    StartView, StartViewChange,
};
use crate::request::{ClientIdentifier, Reply};
use crate::service::Protocol;
use std::collections::VecDeque;
use std::iter::FusedIterator;

pub struct Envelope<D, P> {
    pub destination: D,
    pub payload: P,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ProtocolPayload<P>
where
    P: Protocol,
{
    Prepare(Prepare<P::Request, P::Prediction>),
    PrepareOk(PrepareOk),
    Commit(Commit),
    GetState(GetState),
    NewState(NewState<P::Request, P::Prediction>),
    StartViewChange(StartViewChange),
    DoViewChange(DoViewChange<P::Request, P::Prediction>),
    StartView(StartView<P::Request, P::Prediction>),
    Recovery(Recovery),
    RecoveryResponse(RecoveryResponse<P::Request, P::Prediction>),
}

impl<P> ProtocolPayload<P>
where
    P: Protocol,
{
    pub fn unwrap_prepare(self) -> Prepare<P::Request, P::Prediction> {
        let Self::Prepare(message) = self else {
            panic!("called `ProtocolPayload::unwrap_prepare` on a unsupported variant",)
        };
        message
    }

    pub fn unwrap_prepare_ok(self) -> PrepareOk {
        let Self::PrepareOk(message) = self else {
            panic!("called `ProtocolPayload::unwrap_prepare_ok` on a unsupported variant",)
        };
        message
    }

    pub fn unwrap_commit(self) -> Commit {
        let Self::Commit(message) = self else {
            panic!("called `ProtocolPayload::unwrap_commit` on a unsupported variant",)
        };
        message
    }

    pub fn unwrap_get_state(self) -> GetState {
        let Self::GetState(message) = self else {
            panic!("called `ProtocolPayload::unwrap_get_state` on a unsupported variant",)
        };
        message
    }
}

pub struct BufferedMailbox<P>
where
    P: Protocol,
{
    inbound: VecDeque<ProtocolPayload<P>>,
    replies: VecDeque<Envelope<ClientIdentifier, Reply<P::Reply>>>,
    send: VecDeque<Envelope<usize, ProtocolPayload<P>>>,
    broadcast: VecDeque<ProtocolPayload<P>>,
}

impl<P> Default for BufferedMailbox<P>
where
    P: Protocol,
{
    fn default() -> Self {
        Self {
            inbound: Default::default(),
            replies: Default::default(),
            send: Default::default(),
            broadcast: Default::default(),
        }
    }
}

impl<P> BufferedMailbox<P>
where
    P: Protocol,
{
    pub fn is_empty(&self) -> bool {
        self.inbound.is_empty()
            && self.replies.is_empty()
            && self.send.is_empty()
            && self.broadcast.is_empty()
    }

    pub fn pop_inbound(&mut self) -> Option<ProtocolPayload<P>> {
        self.inbound.pop_front()
    }

    pub fn drain_inbound(
        &mut self,
    ) -> impl Iterator<Item = ProtocolPayload<P>>
           + DoubleEndedIterator
           + ExactSizeIterator
           + FusedIterator
           + '_ {
        self.inbound.drain(..)
    }

    pub fn drain_replies(
        &mut self,
    ) -> impl Iterator<Item = Envelope<ClientIdentifier, Reply<P::Reply>>>
           + DoubleEndedIterator
           + ExactSizeIterator
           + FusedIterator
           + '_ {
        self.replies.drain(..)
    }

    pub fn drain_send(
        &mut self,
    ) -> impl Iterator<Item = Envelope<usize, ProtocolPayload<P>>>
           + DoubleEndedIterator
           + ExactSizeIterator
           + FusedIterator
           + '_ {
        self.send.drain(..)
    }

    pub fn drain_broadcast(
        &mut self,
    ) -> impl Iterator<Item = ProtocolPayload<P>>
           + DoubleEndedIterator
           + ExactSizeIterator
           + FusedIterator
           + '_ {
        self.broadcast.drain(..)
    }
}

impl<P> Outbox<P> for BufferedMailbox<P>
where
    P: Protocol,
{
    fn prepare(&mut self, message: Prepare<P::Request, P::Prediction>) {
        self.broadcast.push_back(ProtocolPayload::Prepare(message));
    }

    fn prepare_ok(&mut self, index: usize, message: PrepareOk) {
        self.send.push_back(Envelope {
            destination: index,
            payload: ProtocolPayload::PrepareOk(message),
        });
    }

    fn commit(&mut self, message: Commit) {
        self.broadcast.push_back(ProtocolPayload::Commit(message));
    }

    fn get_state(&mut self, index: usize, message: GetState) {
        self.send.push_back(Envelope {
            destination: index,
            payload: ProtocolPayload::GetState(message),
        });
    }

    fn new_state(&mut self, index: usize, message: NewState<P::Request, P::Prediction>) {
        self.send.push_back(Envelope {
            destination: index,
            payload: ProtocolPayload::NewState(message),
        });
    }

    fn start_view_change(&mut self, message: StartViewChange) {
        self.broadcast
            .push_back(ProtocolPayload::StartViewChange(message));
    }

    fn do_view_change(&mut self, index: usize, message: DoViewChange<P::Request, P::Prediction>) {
        self.send.push_back(Envelope {
            destination: index,
            payload: ProtocolPayload::DoViewChange(message),
        });
    }

    fn start_view(&mut self, message: StartView<P::Request, P::Prediction>) {
        self.broadcast
            .push_back(ProtocolPayload::StartView(message));
    }

    fn recovery(&mut self, message: Recovery) {
        self.broadcast.push_back(ProtocolPayload::Recovery(message));
    }

    fn recovery_response(
        &mut self,
        index: usize,
        message: RecoveryResponse<P::Request, P::Prediction>,
    ) {
        self.send.push_back(Envelope {
            destination: index,
            payload: ProtocolPayload::RecoveryResponse(message),
        });
    }

    fn reply(&mut self, client: ClientIdentifier, reply: &Reply<P::Reply>) {
        self.replies.push_back(Envelope {
            destination: client,
            payload: reply.clone(),
        });
    }
}

impl<P> Inbox<P> for BufferedMailbox<P>
where
    P: Protocol,
{
    fn push_prepare(&mut self, message: Prepare<P::Request, P::Prediction>) {
        self.inbound.push_back(ProtocolPayload::Prepare(message));
    }

    fn push_prepare_ok(&mut self, message: PrepareOk) {
        self.inbound.push_back(ProtocolPayload::PrepareOk(message));
    }

    fn push_commit(&mut self, message: Commit) {
        self.inbound.push_back(ProtocolPayload::Commit(message));
    }

    fn push_get_state(&mut self, message: GetState) {
        self.inbound.push_back(ProtocolPayload::GetState(message));
    }

    fn push_new_state(&mut self, message: NewState<P::Request, P::Prediction>) {
        self.inbound.push_back(ProtocolPayload::NewState(message));
    }

    fn push_start_view_change(&mut self, message: StartViewChange) {
        self.inbound
            .push_back(ProtocolPayload::StartViewChange(message));
    }

    fn push_do_view_change(&mut self, message: DoViewChange<P::Request, P::Prediction>) {
        self.inbound
            .push_back(ProtocolPayload::DoViewChange(message));
    }

    fn push_start_view(&mut self, message: StartView<P::Request, P::Prediction>) {
        self.inbound.push_back(ProtocolPayload::StartView(message));
    }

    fn push_recovery(&mut self, message: Recovery) {
        self.inbound.push_back(ProtocolPayload::Recovery(message));
    }

    fn push_recovery_response(&mut self, message: RecoveryResponse<P::Request, P::Prediction>) {
        self.inbound
            .push_back(ProtocolPayload::RecoveryResponse(message));
    }
}

impl<P> Mailbox<P> for BufferedMailbox<P> where P: Protocol {}
