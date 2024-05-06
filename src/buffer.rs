use crate::mail::{Inbox, Mailbox, Outbox};
use crate::protocol::{
    Commit, DoViewChange, GetState, NewState, Prepare, PrepareOk, Recovery, RecoveryResponse,
    StartView, StartViewChange,
};
use crate::request::{ClientIdentifier, Reply};
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::iter::FusedIterator;

pub struct Envelope<D, P> {
    pub destination: D,
    pub payload: P,
}

#[derive(Eq, PartialEq)]
pub enum ProtocolPayload {
    Prepare(Prepare),
    PrepareOk(PrepareOk),
    Commit(Commit),
    GetState(GetState),
    NewState(NewState),
    StartViewChange(StartViewChange),
    DoViewChange(DoViewChange),
    StartView(StartView),
    Recovery(Recovery),
    RecoveryResponse(RecoveryResponse),
}

impl Clone for ProtocolPayload {
    fn clone(&self) -> Self {
        match self {
            ProtocolPayload::Prepare(message) => Self::Prepare(message.clone()),
            ProtocolPayload::PrepareOk(message) => Self::PrepareOk(message.clone()),
            ProtocolPayload::Commit(message) => Self::Commit(message.clone()),
            ProtocolPayload::GetState(message) => Self::GetState(message.clone()),
            ProtocolPayload::NewState(message) => Self::NewState(message.clone()),
            ProtocolPayload::StartViewChange(message) => Self::StartViewChange(message.clone()),
            ProtocolPayload::DoViewChange(message) => Self::DoViewChange(message.clone()),
            ProtocolPayload::StartView(message) => Self::StartView(message.clone()),
            ProtocolPayload::Recovery(message) => Self::Recovery(message.clone()),
            ProtocolPayload::RecoveryResponse(message) => Self::RecoveryResponse(message.clone()),
        }
    }
}

impl Debug for ProtocolPayload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolPayload::Prepare(message) => write!(f, "{message:?}"),
            ProtocolPayload::PrepareOk(message) => write!(f, "{message:?}"),
            ProtocolPayload::Commit(message) => write!(f, "{message:?}"),
            ProtocolPayload::GetState(message) => write!(f, "{message:?}"),
            ProtocolPayload::NewState(message) => write!(f, "{message:?}"),
            ProtocolPayload::StartViewChange(message) => write!(f, "{message:?}"),
            ProtocolPayload::DoViewChange(message) => write!(f, "{message:?}"),
            ProtocolPayload::StartView(message) => write!(f, "{message:?}"),
            ProtocolPayload::Recovery(message) => write!(f, "{message:?}"),
            ProtocolPayload::RecoveryResponse(message) => write!(f, "{message:?}"),
        }
    }
}

impl ProtocolPayload {
    pub fn unwrap_prepare(self) -> Prepare {
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

pub struct BufferedMailbox {
    inbound: VecDeque<ProtocolPayload>,
    replies: VecDeque<Envelope<ClientIdentifier, Reply>>,
    send: VecDeque<Envelope<usize, ProtocolPayload>>,
    broadcast: VecDeque<ProtocolPayload>,
}

impl Default for BufferedMailbox {
    fn default() -> Self {
        Self {
            inbound: Default::default(),
            replies: Default::default(),
            send: Default::default(),
            broadcast: Default::default(),
        }
    }
}

impl BufferedMailbox {
    pub fn is_empty(&self) -> bool {
        self.inbound.is_empty()
            && self.replies.is_empty()
            && self.send.is_empty()
            && self.broadcast.is_empty()
    }

    pub fn pop_inbound(&mut self) -> Option<ProtocolPayload> {
        self.inbound.pop_front()
    }

    pub fn drain_inbound(
        &mut self,
    ) -> impl DoubleEndedIterator<Item = ProtocolPayload> + ExactSizeIterator + FusedIterator + '_
    {
        self.inbound.drain(..)
    }

    pub fn drain_replies(
        &mut self,
    ) -> impl DoubleEndedIterator<Item = Envelope<ClientIdentifier, Reply>>
           + ExactSizeIterator
           + FusedIterator
           + '_ {
        self.replies.drain(..)
    }

    pub fn drain_send(
        &mut self,
    ) -> impl DoubleEndedIterator<Item = Envelope<usize, ProtocolPayload>>
           + ExactSizeIterator
           + FusedIterator
           + '_ {
        self.send.drain(..)
    }

    pub fn drain_broadcast(
        &mut self,
    ) -> impl DoubleEndedIterator<Item = ProtocolPayload> + ExactSizeIterator + FusedIterator + '_
    {
        self.broadcast.drain(..)
    }
}

impl Outbox for BufferedMailbox {
    fn prepare(&mut self, message: Prepare) {
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

    fn new_state(&mut self, index: usize, message: NewState) {
        self.send.push_back(Envelope {
            destination: index,
            payload: ProtocolPayload::NewState(message),
        });
    }

    fn start_view_change(&mut self, message: StartViewChange) {
        self.broadcast
            .push_back(ProtocolPayload::StartViewChange(message));
    }

    fn do_view_change(&mut self, index: usize, message: DoViewChange) {
        self.send.push_back(Envelope {
            destination: index,
            payload: ProtocolPayload::DoViewChange(message),
        });
    }

    fn start_view(&mut self, message: StartView) {
        self.broadcast
            .push_back(ProtocolPayload::StartView(message));
    }

    fn recovery(&mut self, message: Recovery) {
        self.broadcast.push_back(ProtocolPayload::Recovery(message));
    }

    fn recovery_response(&mut self, index: usize, message: RecoveryResponse) {
        self.send.push_back(Envelope {
            destination: index,
            payload: ProtocolPayload::RecoveryResponse(message),
        });
    }

    fn reply(&mut self, client: ClientIdentifier, reply: &Reply) {
        self.replies.push_back(Envelope {
            destination: client,
            payload: reply.clone(),
        });
    }
}

impl Inbox for BufferedMailbox {
    fn push_prepare(&mut self, message: Prepare) {
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

    fn push_new_state(&mut self, message: NewState) {
        self.inbound.push_back(ProtocolPayload::NewState(message));
    }

    fn push_start_view_change(&mut self, message: StartViewChange) {
        self.inbound
            .push_back(ProtocolPayload::StartViewChange(message));
    }

    fn push_do_view_change(&mut self, message: DoViewChange) {
        self.inbound
            .push_back(ProtocolPayload::DoViewChange(message));
    }

    fn push_start_view(&mut self, message: StartView) {
        self.inbound.push_back(ProtocolPayload::StartView(message));
    }

    fn push_recovery(&mut self, message: Recovery) {
        self.inbound.push_back(ProtocolPayload::Recovery(message));
    }

    fn push_recovery_response(&mut self, message: RecoveryResponse) {
        self.inbound
            .push_back(ProtocolPayload::RecoveryResponse(message));
    }
}

impl Mailbox for BufferedMailbox {}
