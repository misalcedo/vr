use crate::identifiers::{ClientIdentifier, RequestIdentifier};
use crate::mailbox::Address;
use crate::stamps::{OpNumber, View};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    pub from: Address,
    pub to: Address,
    pub view: View,
    pub payload: Payload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Payload {
    Request(Request),
    Prepare(Prepare),
    PrepareOk(PrepareOk),
    Reply(Reply),
    DoViewChange(DoViewChange),
    StartView(StartView),
    Ping,
    OutdatedView,
    OutdatedRequest(OutdatedRequest),
    ConcurrentRequest(ConcurrentRequest),
}

impl From<Request> for Payload {
    fn from(value: Request) -> Self {
        Self::Request(value)
    }
}

impl From<Prepare> for Payload {
    fn from(value: Prepare) -> Self {
        Self::Prepare(value)
    }
}

impl From<PrepareOk> for Payload {
    fn from(value: PrepareOk) -> Self {
        Self::PrepareOk(value)
    }
}

impl From<Reply> for Payload {
    fn from(value: Reply) -> Self {
        Self::Reply(value)
    }
}

impl From<DoViewChange> for Payload {
    fn from(value: DoViewChange) -> Self {
        Self::DoViewChange(value)
    }
}

impl From<StartView> for Payload {
    fn from(value: StartView) -> Self {
        Self::StartView(value)
    }
}

impl From<ConcurrentRequest> for Payload {
    fn from(value: ConcurrentRequest) -> Self {
        Self::ConcurrentRequest(value)
    }
}

impl From<OutdatedRequest> for Payload {
    fn from(value: OutdatedRequest) -> Self {
        Self::OutdatedRequest(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Request {
    /// The operation (with its arguments) the client wants to run.
    pub op: Vec<u8>,
    /// Client id
    pub c: ClientIdentifier,
    /// Client-assigned number for the request.
    pub s: RequestIdentifier,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Prepare {
    /// The op-number assigned to the request.
    pub n: OpNumber,
    /// The message received from the client.
    pub m: Request,
    /// The op-number of the last committed log entry.
    pub k: OpNumber,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PrepareOk {
    /// The op-number assigned to the request.
    pub n: OpNumber,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Reply {
    /// The response from the service after executing the operation.
    pub x: Vec<u8>,
    /// Client-assigned number for the request.
    pub s: RequestIdentifier,
}

// TODO: Use a view table to reduce the bandwidth usage of the view change protocol.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DoViewChange {
    /// The log of the replica.
    pub l: Vec<Request>,
    /// The op-number of the latest committed request known to the replica.
    pub k: OpNumber,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartView {
    /// The log of the replica.
    pub l: Vec<Request>,
    /// The op-number of the latest committed request known to the replica.
    pub k: OpNumber,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConcurrentRequest {
    /// Client-assigned number for the request in-progress.
    pub s: RequestIdentifier,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutdatedRequest {
    /// Client-assigned number for the most recent request processed.
    pub s: RequestIdentifier,
}
