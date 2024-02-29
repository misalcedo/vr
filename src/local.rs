use crate::mail::Outbox;
use crate::protocol::{
    Commit, DoViewChange, GetState, NewState, Prepare, PrepareOk, Protocol, Recovery,
    RecoveryResponse, StartView, StartViewChange,
};
use crate::request::{ClientIdentifier, Reply};
use std::collections::VecDeque;

pub struct Envelope<D, P> {
    pub destination: D,
    pub payload: P,
}

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
