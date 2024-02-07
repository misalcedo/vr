use crate::stamps::{OpNumber, View};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum Address {
    Replica(ReplicaIdentifier),
    Group(GroupIdentifier),
    Client(ClientIdentifier),
}

impl From<ReplicaIdentifier> for Address {
    fn from(value: ReplicaIdentifier) -> Self {
        Self::Replica(value)
    }
}

impl From<GroupIdentifier> for Address {
    fn from(value: GroupIdentifier) -> Self {
        Self::Group(value)
    }
}

impl From<ClientIdentifier> for Address {
    fn from(value: ClientIdentifier) -> Self {
        Self::Client(value)
    }
}

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

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct ReplicaIdentifier(GroupIdentifier, usize);

impl ReplicaIdentifier {
    pub fn group(&self) -> GroupIdentifier {
        self.0
    }

    pub fn primary(&self, view: View) -> Self {
        self.0.primary(view)
    }

    pub fn sub_majority(&self) -> usize {
        self.0.sub_majority()
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct GroupIdentifier(u128, usize);

impl Default for GroupIdentifier {
    fn default() -> Self {
        Self::new(3)
    }
}

impl GroupIdentifier {
    pub fn new(replicas: usize) -> Self {
        Self(uuid::Uuid::now_v7().as_u128(), replicas)
    }

    pub fn primary(&self, view: View) -> ReplicaIdentifier {
        ReplicaIdentifier(*self, (view.as_u128() % (self.1 as u128)) as usize)
    }

    pub fn replicas(&self) -> impl Iterator<Item = ReplicaIdentifier> {
        let clone = *self;
        (0..self.1)
            .into_iter()
            .map(move |i| ReplicaIdentifier(clone, i))
    }

    pub fn sub_majority(&self) -> usize {
        (self.1 - 1) / 2
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct ClientIdentifier(u128);

impl Default for ClientIdentifier {
    fn default() -> Self {
        Self(uuid::Uuid::now_v7().as_u128())
    }
}

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct RequestIdentifier(u128);

impl RequestIdentifier {
    pub fn increment(&mut self) -> Self {
        self.0 += 1;
        *self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sub_majority() {
        assert_eq!(GroupIdentifier::new(3).sub_majority(), 1);
        assert_eq!(GroupIdentifier::new(4).sub_majority(), 1);
        assert_eq!(GroupIdentifier::new(5).sub_majority(), 2);
    }
}
