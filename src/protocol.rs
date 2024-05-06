use crate::log::Log;
use crate::nonce::Nonce;
use crate::request::{Payload, Request};
use crate::viewstamp::{OpNumber, View};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Prepare {
    /// The current view of the replica.
    pub view: View,
    /// The op-number assigned to the request.
    pub op_number: OpNumber,
    /// The message received from the client along with a prediction for supporting non-deterministic behavior.
    pub request: Request,
    /// The prediction of non-deterministic behavior performed at the primary.
    pub prediction: Payload,
    /// The op-number of the last committed log entry.
    pub committed: OpNumber,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrepareOk {
    /// The current view of the replica.
    pub view: View,
    /// The op-number assigned to the request.
    pub op_number: OpNumber,
    /// The index of the replica that prepared the operation.
    pub index: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Commit {
    /// The current view of the replica.
    pub view: View,
    /// The op-number of the latest committed request known to the replica.
    pub committed: OpNumber,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GetState {
    /// The current view of the replica.
    pub view: View,
    /// The latest op-number the replica is aware of.
    pub op_number: OpNumber,
    /// The index of the replica that needs to get the new state.
    pub index: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewState {
    /// The current view of the replica.
    pub view: View,
    /// An excerpt of the log based on the last known op number.
    pub log: Log,
    /// The op-number of the latest committed request known to the replica.
    pub committed: OpNumber,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StartViewChange {
    /// The current view of the replica.
    pub view: View,
    /// The index of the replica that needs to get the new state.
    pub index: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DoViewChange {
    /// The current view of the replica.
    pub view: View,
    /// The log of the replica from its last normal view.
    pub log: Log,
    /// The op-number of the latest committed request known to the replica.
    pub committed: OpNumber,
    /// The index of the replica that sent the message.
    pub index: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StartView {
    /// The current view of the replica.
    pub view: View,
    /// The log to use in the new view.
    pub log: Log,
    /// The op-number of the latest committed request known to the replica.
    pub committed: OpNumber,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Recovery {
    /// The index of the replica that needs to get the new state.
    pub index: usize,
    /// The last committed operation included in the checkpoint the replica used to recover.
    pub committed: OpNumber,
    /// A value coined for single use to detect replays of previous recovery requests.
    pub nonce: Nonce,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecoveryResponse {
    /// The current view of the replica.
    pub view: View,
    /// A value coined for single use to detect replays of previous recovery requests.
    pub nonce: Nonce,
    /// The log to use in the new view.
    pub log: Log,
    /// The op-number of the latest committed request known to the replica.
    pub committed: OpNumber,
    /// The index of the sender.
    pub index: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// The last committed operation reflected in the application state.
    pub committed: OpNumber,
    /// The application state when the checkpoint was taken.
    pub state: Payload,
}
