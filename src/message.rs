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

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Reply {
    pub view: View,
    pub result: (),
    pub client: u128,
    pub id: u128,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ProtocolMessage {
    Prepare(Prepare),
    PrepareOk(PrepareOk),
    Commit(Commit),
    GetState(GetState),
    NewState(NewState),
}

impl ProtocolMessage {
    pub fn view(&self) -> usize {
        match self {
            ProtocolMessage::Prepare(m) => m.view,
            ProtocolMessage::PrepareOk(m) => m.view,
            ProtocolMessage::Commit(m) => m.view,
            ProtocolMessage::GetState(m) => m.view,
            ProtocolMessage::NewState(m) => m.view,
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
        ProtocolMessage::Prepare(value)
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
        ProtocolMessage::PrepareOk(value)
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
        ProtocolMessage::Commit(value)
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
        ProtocolMessage::GetState(value)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewState {
    pub view: View,
    pub log: [Request; 0], // TODO
    pub op_number: usize,
    pub commit: usize,
}

impl From<NewState> for ProtocolMessage {
    fn from(value: NewState) -> Self {
        ProtocolMessage::NewState(value)
    }
}
