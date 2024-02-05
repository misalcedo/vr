use std::num::NonZeroU128;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum Address {
    Replica(ReplicaIdentifier),
    Group(GroupIdentifier),
    Client(ClientIdentifier)
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
    pub message: Message
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    pub view: View,
    pub payload: Payload
}

impl Message {
    pub fn new(view: View, payload: impl Into<Payload>) -> Self {
        Self { view, payload: payload.into() }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Payload {
    Request(Request),
    Prepare(Prepare)
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

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct ReplicaIdentifier(GroupIdentifier, usize);

impl ReplicaIdentifier {
    pub fn primary(&self, view: View) -> Self {
        self.0.primary(view)
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

    pub fn replicas(&self) -> impl Iterator<Item=ReplicaIdentifier> {
        let clone = *self;
        (0..self.1).into_iter().map(move |i| ReplicaIdentifier(clone, i))
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
pub struct OpNumber(Option<NonZeroU128>);

impl From<u128> for OpNumber {
    fn from(value: u128) -> Self {
        Self(NonZeroU128::new(value))
    }
}

impl From<usize> for OpNumber {
    fn from(value: usize) -> Self {
        Self(NonZeroU128::new(value as u128))
    }
}

impl From<i32> for OpNumber {
    fn from(value: i32) -> Self {
        Self(NonZeroU128::new(value as u128))
    }
}

impl From<OpNumber> for u128 {
    fn from(value: OpNumber) -> Self {
        value.0.map(NonZeroU128::get).unwrap_or(0)
    }
}

impl OpNumber {
    pub fn increment(&mut self) {
        self.0 = NonZeroU128::new(1 + u128::from(*self))
    }
}

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct View(u128);

impl From<u128> for View {
    fn from(value: u128) -> Self {
        Self(value)
    }
}

impl From<View> for u128 {
    fn from(value: View) -> Self {
        value.0
    }
}

impl View {
    pub fn increment(&mut self) {
        self.0 = 1 + self.0;
    }
}
