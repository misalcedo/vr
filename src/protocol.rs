use crate::log::Log;
use crate::request::{Request, RequestIdentifier};
use crate::viewstamp::{OpNumber, View};
use serde::{Deserialize, Serialize};

pub trait Message<'a>: Serialize + Deserialize<'a> {
    fn view(&self) -> View;
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

#[derive(Serialize, Deserialize)]
pub struct Recovery {
    /// The index of the replica that needs to get the new state.
    pub index: usize,
    /// A value coined for single use to detect replays of previous recovery requests.
    pub nonce: RequestIdentifier,
}

impl<'a> Message<'a> for Recovery {
    fn view(&self) -> View {
        Default::default()
    }
}

#[derive(Serialize, Deserialize)]
pub struct RecoveryResponse<R, P> {
    /// The current view of the replica.
    pub view: View,
    /// A value coined for single use to detect replays of previous recovery requests.
    pub nonce: RequestIdentifier,
    /// The log to use in the new view.
    pub log: Log<R, P>,
    /// The op-number of the latest committed request known to the replica.
    pub committed: OpNumber,
    /// The index of the sender.
    pub index: usize,
}

impl<'a, R, P> Message<'a> for RecoveryResponse<R, P>
where
    R: Serialize + Deserialize<'a>,
    P: Serialize + Deserialize<'a>,
{
    fn view(&self) -> View {
        self.view
    }
}
