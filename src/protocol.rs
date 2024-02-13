use crate::request::Request;
use crate::viewstamp::View;
use serde::{Deserialize, Serialize};

pub trait Message<'a>: Serialize + Deserialize<'a> {
    fn view(&self) -> View;
}

#[derive(Serialize, Deserialize)]
pub struct Prepare<R, P> {
    /// The current view of the replica.
    pub view: View,
    /// The op-number assigned to the request.
    pub op_number: usize,
    /// The message received from the client along with a prediction for supporting non-deterministic behavior.
    pub request: Request<R>,
    /// The prediction of non-deterministic behavior performed at the primary.
    pub prediction: P,
    /// The op-number of the last committed log entry.
    pub committed: usize,
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
    pub op_number: usize,
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
    pub v: View,
    /// The op-number of the latest committed request known to the replica.
    pub k: usize,
}

impl<'a> Message<'a> for Commit {
    fn view(&self) -> View {
        self.v
    }
}
