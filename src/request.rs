use crate::viewstamp::View;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ClientIdentifier(u128);

impl Default for ClientIdentifier {
    fn default() -> Self {
        Self(uuid::Uuid::new_v4().as_u128())
    }
}

#[derive(
    Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize,
)]
pub struct RequestIdentifier(u128);

impl RequestIdentifier {
    pub fn increment(&mut self) {
        self.0 += 1;
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Payload {
    bytes: Vec<u8>,
}

impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

impl Display for Payload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(hex::encode_upper(&self.bytes).as_str())
    }
}

impl<const N: usize> TryFrom<&Payload> for [u8; N] {
    type Error = usize;

    fn try_from(value: &Payload) -> Result<Self, Self::Error> {
        let n = value.bytes.len();
        if n == N {
            let mut array = [0u8; N];
            array.copy_from_slice(&value.bytes);
            Ok(array)
        } else {
            Err(n)
        }
    }
}

impl<const N: usize> From<[u8; N]> for Payload {
    fn from(value: [u8; N]) -> Self {
        Self {
            bytes: Vec::from(value),
        }
    }
}

impl<C> From<C> for Payload
where
    C: CustomPayload,
{
    fn from(value: C) -> Self {
        value.to_payload()
    }
}

pub trait CustomPayload {
    fn to_payload(&self) -> Payload;

    fn from_payload(payload: &Payload) -> Self;
}

impl CustomPayload for () {
    fn to_payload(&self) -> Payload {
        Default::default()
    }

    fn from_payload(_: &Payload) -> Self {
        ()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Request {
    /// The operation (with its arguments) the client wants to run.
    pub payload: Payload,
    /// Client id
    pub client: ClientIdentifier,
    /// Client-assigned number for the request.
    pub id: RequestIdentifier,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Reply {
    /// The current view of the replica.
    pub view: View,
    /// Client-assigned number for the request.
    pub id: RequestIdentifier,
    /// The response from the service after executing the operation.
    pub payload: Payload,
}
