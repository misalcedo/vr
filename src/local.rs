use crate::mail::Outbox;
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

#[derive(Debug)]
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

pub struct BufferedOutbox<P>
where
    P: Protocol,
{
    replies: VecDeque<Envelope<ClientIdentifier, Reply<P::Reply>>>,
    send: VecDeque<Envelope<usize, ProtocolPayload<P>>>,
    broadcast: VecDeque<ProtocolPayload<P>>,
}

impl<P> Default for BufferedOutbox<P>
where
    P: Protocol,
{
    fn default() -> Self {
        Self {
            replies: Default::default(),
            send: Default::default(),
            broadcast: Default::default(),
        }
    }
}

impl<P> BufferedOutbox<P>
where
    P: Protocol,
{
    pub fn is_empty(&self) -> bool {
        self.replies.is_empty() && self.send.is_empty() && self.broadcast.is_empty()
    }

    pub fn replies(&self) -> usize {
        self.replies.len()
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

impl<P> Outbox<P> for BufferedOutbox<P>
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
