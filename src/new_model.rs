use std::num::NonZeroUsize;

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
pub struct Envelope {
    pub from: Address,
    pub to: Address,
    pub message: Message,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    pub view: View,
    pub payload: Payload,
}

impl Message {
    pub fn new(view: View, payload: impl Into<Payload>) -> Self {
        Self {
            view,
            payload: payload.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Payload {
    Request(Request),
    Prepare(Prepare),
    PrepareOk(PrepareOk),
    Reply(Reply),
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
        ReplicaIdentifier(*self, (view.0 % (self.1 as u128)) as usize)
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

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct RequestIdentifier(u128);

impl RequestIdentifier {
    pub fn increment(&mut self) -> Self {
        self.0 += 1;
        *self
    }
}

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct OpNumber(Option<NonZeroUsize>);

impl From<usize> for OpNumber {
    fn from(value: usize) -> Self {
        Self(NonZeroUsize::new(value))
    }
}

impl From<OpNumber> for usize {
    fn from(value: OpNumber) -> Self {
        value.0.map(NonZeroUsize::get).unwrap_or_default() as usize
    }
}

impl OpNumber {
    pub fn increment(&mut self) {
        self.0 = NonZeroUsize::new(1 + self.0.map(NonZeroUsize::get).unwrap_or(0))
    }

    pub fn next(&self) -> Self {
        Self(NonZeroUsize::new(
            1 + self.0.map(NonZeroUsize::get).unwrap_or(0),
        ))
    }
}

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct View(u128);

impl View {
    pub fn increment(&mut self) {
        self.0 = 1 + self.0;
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