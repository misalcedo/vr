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
    pub fn next(&mut self) -> Self {
        *self = Self::default();
        *self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Request<R> {
    /// The operation (with its arguments) the client wants to run.
    pub op: R,
    /// Client id
    pub client: ClientIdentifier,
    /// Client-assigned number for the request.
    pub id: RequestIdentifier,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Reply<R> {
    /// The response from the service after executing the operation.
    pub x: R,
    /// Client-assigned number for the request.
    pub s: RequestIdentifier,
}
