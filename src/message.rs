use serde::{Deserialize, Serialize};

pub type View = usize;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Message {
    Request(Request),
    Reply(Reply),
    Protocol(usize, ProtocolMessage),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Request {
    pub operation: (),
    pub client: u128,
    pub id: u128,
}

impl From<Request> for Message {
    fn from(value: Request) -> Self {
        Self::Request(value)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Reply {
    pub view: View,
    pub result: (),
    pub client: u128,
    pub id: u128,
}

impl From<Reply> for Message {
    fn from(value: Reply) -> Self {
        Self::Reply(value)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ProtocolMessage {
    Prepare(Prepare),
    PrepareOk(PrepareOk),
    Commit(Commit),
    GetState(GetState),
    NewState(NewState),
    StartViewChange(StartViewChange),
    DoViewChange(DoViewChange),
}

impl ProtocolMessage {
    pub fn view(&self) -> usize {
        match self {
            Self::Prepare(m) => m.view,
            Self::PrepareOk(m) => m.view,
            Self::Commit(m) => m.view,
            Self::GetState(m) => m.view,
            Self::NewState(m) => m.view,
            Self::StartViewChange(m) => m.view,
            Self::DoViewChange(m) => m.view,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Prepare {
    pub view: View,
    pub op_number: usize,
    pub commit: usize,
    pub request: Request,
}

impl From<Prepare> for ProtocolMessage {
    fn from(value: Prepare) -> Self {
        Self::Prepare(value)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrepareOk {
    pub view: View,
    pub op_number: usize,
    pub index: usize,
}

impl From<PrepareOk> for ProtocolMessage {
    fn from(value: PrepareOk) -> Self {
        Self::PrepareOk(value)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Commit {
    pub view: View,
    pub commit: usize,
}

impl From<Prepare> for Commit {
    fn from(value: Prepare) -> Self {
        Self {
            view: value.view,
            commit: value.commit,
        }
    }
}

impl From<Commit> for ProtocolMessage {
    fn from(value: Commit) -> Self {
        Self::Commit(value)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GetState {
    pub view: View,
    pub op_number: usize,
    pub index: usize,
}

impl From<GetState> for ProtocolMessage {
    fn from(value: GetState) -> Self {
        Self::GetState(value)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewState {
    pub view: View,
    pub log: [Request; 0],
    pub op_number: usize,
    pub commit: usize,
}

impl From<NewState> for ProtocolMessage {
    fn from(value: NewState) -> Self {
        Self::NewState(value)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StartViewChange {
    pub view: View,
    pub index: usize,
}

impl From<StartViewChange> for ProtocolMessage {
    fn from(value: StartViewChange) -> Self {
        Self::StartViewChange(value)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DoViewChange {
    pub view: View,
    pub log: [Request; 0],
    pub last_normal_view: usize,
    pub op_number: usize,
    pub commit: usize,
    pub index: usize,
}

impl From<DoViewChange> for ProtocolMessage {
    fn from(value: DoViewChange) -> Self {
        Self::DoViewChange(value)
    }
}
