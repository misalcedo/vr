use crate::viewstamp::View;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ClientIdentifier(u128);

impl Default for ClientIdentifier {
    fn default() -> Self {
        Self(uuid::Uuid::new_v4().as_u128())
    }
}

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct RequestIdentifier(u128);

impl Default for RequestIdentifier {
    fn default() -> Self {
        Self(uuid::Uuid::now_v7().as_u128())
    }
}

impl RequestIdentifier {
    pub fn increment(&mut self) -> Self {
        *self = Self::default();
        *self
    }

    pub fn next(&self) -> Self {
        Self::default()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Request<R> {
    /// The operation (with its arguments) the client wants to run.
    pub payload: R,
    /// Client id
    pub client: ClientIdentifier,
    /// Client-assigned number for the request.
    pub id: RequestIdentifier,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Reply<R> {
    /// The current view of the replica.
    pub view: View,
    /// Client-assigned number for the request.
    pub id: RequestIdentifier,
    /// The response from the service after executing the operation.
    pub payload: R,
}
