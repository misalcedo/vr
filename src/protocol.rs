use crate::log::Log;
use crate::request::Request;
use crate::viewstamp::{OpNumber, View};
use serde::{Deserialize, Serialize};

pub trait Message<'a>: Serialize + Deserialize<'a> {
    fn view(&self) -> View;
}

#[derive(Serialize, Deserialize)]
pub enum Protocol<R, P> {
    Prepare(Prepare<R, P>),
    PrepareOk(PrepareOk),
    Commit(Commit),
    GetState(GetState),
    NewState(NewState<R, P>),
    StartViewChange(StartViewChange),
    DoViewChange(DoViewChange<R, P>),
    StartView(StartView<R, P>),
}

impl<R, P> TryFrom<Protocol<R, P>> for Prepare<R, P> {
    type Error = Protocol<R, P>;

    fn try_from(value: Protocol<R, P>) -> Result<Self, Self::Error> {
        match value {
            Protocol::Prepare(m) => Ok(m),
            _ => Err(value),
        }
    }
}

impl<R, P> From<Prepare<R, P>> for Protocol<R, P> {
    fn from(value: Prepare<R, P>) -> Self {
        Protocol::Prepare(value)
    }
}

impl<R, P> TryFrom<Protocol<R, P>> for NewState<R, P> {
    type Error = Protocol<R, P>;

    fn try_from(value: Protocol<R, P>) -> Result<Self, Self::Error> {
        match value {
            Protocol::NewState(p) => Ok(p),
            _ => Err(value),
        }
    }
}

impl<R, P> From<NewState<R, P>> for Protocol<R, P> {
    fn from(value: NewState<R, P>) -> Self {
        Protocol::NewState(value)
    }
}

impl<'a, R, P> Message<'a> for Protocol<R, P>
where
    R: Serialize + Deserialize<'a>,
    P: Serialize + Deserialize<'a>,
{
    fn view(&self) -> View {
        match self {
            Protocol::Prepare(m) => m.view,
            Protocol::PrepareOk(m) => m.view,
            Protocol::Commit(m) => m.view,
            Protocol::GetState(m) => m.view,
            Protocol::NewState(m) => m.view,
            Protocol::StartViewChange(m) => m.view,
            Protocol::DoViewChange(m) => m.view,
            Protocol::StartView(m) => m.view,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Prepare<R, P> {
    /// The current view of the replica.
    pub view: View,
    /// The op-number assigned to the request.
    pub op_number: OpNumber,
    /// The message received from the client along with a prediction for supporting non-deterministic behavior.
    pub request: Request<R>,
    /// The prediction of non-deterministic behavior performed at the primary.
    pub prediction: P,
    /// The op-number of the last committed log entry.
    pub committed: OpNumber,
}

impl<'a, R, P> Message<'a> for Prepare<R, P>
where
    R: Serialize + Deserialize<'a>,
    P: Serialize + Deserialize<'a>,
{
    fn view(&self) -> View {
        self.view
    }
}

#[derive(Serialize, Deserialize)]
pub struct PrepareOk {
    /// The current view of the replica.
    pub view: View,
    /// The op-number assigned to the request.
    pub op_number: OpNumber,
    /// The index of the replica that prepared the operation.
    pub index: usize,
}

impl<'a> Message<'a> for PrepareOk {
    fn view(&self) -> View {
        self.view
    }
}

#[derive(Serialize, Deserialize)]
pub struct Commit {
    /// The current view of the replica.
    pub view: View,
    /// The op-number of the latest committed request known to the replica.
    pub committed: OpNumber,
}

impl<'a> Message<'a> for Commit {
    fn view(&self) -> View {
        self.view
    }
}

#[derive(Serialize, Deserialize)]
pub struct GetState {
    /// The current view of the replica.
    pub view: View,
    /// The latest op-number the replica is aware of.
    pub op_number: OpNumber,
    /// The index of the replica that needs to get the new state.
    pub index: usize,
}

impl<'a> Message<'a> for GetState {
    fn view(&self) -> View {
        self.view
    }
}

#[derive(Serialize, Deserialize)]
pub struct NewState<R, P> {
    /// The current view of the replica.
    pub view: View,
    /// An excerpt of the log based on the last known op number.
    pub log: Log<R, P>,
    /// The op-number of the latest committed request known to the replica.
    pub committed: OpNumber,
}

impl<'a, R, P> Message<'a> for NewState<R, P>
where
    R: Serialize + Deserialize<'a>,
    P: Serialize + Deserialize<'a>,
{
    fn view(&self) -> View {
        self.view
    }
}

#[derive(Serialize, Deserialize)]
pub struct StartViewChange {
    /// The current view of the replica.
    pub view: View,
    /// The index of the replica that needs to get the new state.
    pub index: usize,
}

impl<'a> Message<'a> for StartViewChange {
    fn view(&self) -> View {
        self.view
    }
}

#[derive(Serialize, Deserialize)]
pub struct DoViewChange<R, P> {
    /// The current view of the replica.
    pub view: View,
    /// The log of the replica from its last normal view.
    pub log: Log<R, P>,
    /// The op-number of the latest committed request known to the replica.
    pub committed: OpNumber,
    /// The index of the replica that sent the message.
    pub index: usize,
}

impl<'a, R, P> Message<'a> for DoViewChange<R, P>
where
    R: Serialize + Deserialize<'a>,
    P: Serialize + Deserialize<'a>,
{
    fn view(&self) -> View {
        self.view
    }
}

#[derive(Serialize, Deserialize)]
pub struct StartView<R, P> {
    /// The current view of the replica.
    pub view: View,
    /// The log to use in the new view.
    pub log: Log<R, P>,
    /// The op-number of the latest committed request known to the replica.
    pub committed: OpNumber,
}

impl<'a, R, P> Message<'a> for StartView<R, P>
where
    R: Serialize + Deserialize<'a>,
    P: Serialize + Deserialize<'a>,
{
    fn view(&self) -> View {
        self.view
    }
}
