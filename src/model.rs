#[derive(Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct Request {
    /// The operation (with its arguments) the client wants to run).
    pub op: Vec<u8>,
    /// Client id
    pub c: u128,
    /// Client-assigned number for the request.
    pub s: u128,
    /// View number known to the client.
    pub v: usize,
}

#[derive(Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct Reply {
    /// View number.
    pub v: usize,
    /// The number the client provided in the request.
    pub s: u128,
    /// The result of the up-call to the service.
    pub x: Vec<u8>,
}

impl From<Reply> for Message {
    fn from(value: Reply) -> Self {
        Message::Reply(value)
    }
}

#[derive(Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct Prepare {
    /// The current view-number.
    pub v: usize,
    /// The op-number assigned to the request.
    pub n: usize,
    /// The message received from the client.
    pub m: Request,
}

impl From<Prepare> for Message {
    fn from(value: Prepare) -> Self {
        Message::Prepare(value)
    }
}

#[derive(Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct PrepareOk {
    /// The current view-number known to the replica.
    pub v: usize,
    /// The op-number assigned to the accepted prepare message.
    pub n: usize,
    /// The index of the replica accepting the prepare message.
    pub i: usize,
}

impl From<PrepareOk> for Message {
    fn from(value: PrepareOk) -> Self {
        Message::PrepareOk(value)
    }
}

#[derive(Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct Commit {
    /// The current view-number.
    pub v: usize,
    /// The op-number of the last committed log entry.
    pub n: usize,
}

impl From<Commit> for Message {
    fn from(value: Commit) -> Self {
        Message::Commit(value)
    }
}

#[derive(Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct Inform {
    /// The current view-number.
    pub v: usize,
}

impl From<Inform> for Message {
    fn from(value: Inform) -> Self {
        Message::Inform(value)
    }
}

#[derive(Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct Ping {
    /// The current view-number.
    pub v: usize,
}

impl From<Ping> for Message {
    fn from(value: Ping) -> Self {
        Message::Ping(value)
    }
}

#[derive(Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct DoViewChange {
    /// The current view-number of the replica.
    pub v: usize,
    /// The log of the replica.
    pub l: Vec<Request>,
    /// The op-number of the latest committed request known to the replica.
    pub k: usize,
    /// The index of the replica that detected the primary's failure.
    pub i: usize
}

impl From<DoViewChange> for Message {
    fn from(value: DoViewChange) -> Self {
        Message::DoViewChange(value)
    }
}

#[derive(Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct StartView {
    /// The current view-number.
    pub v: usize,
    /// The log of the new primary.
    pub l: usize,
    /// The op-number of the latest committed request known to the primary.
    pub k: usize,
}

impl From<StartView> for Message {
    fn from(value: StartView) -> Self {
        Message::StartView(value)
    }
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub enum Message {
    Request(Request),
    Prepare(Prepare),
    PrepareOk(PrepareOk),
    Reply(Reply),
    Commit(Commit),
    Inform(Inform),
    Ping(Ping),
    DoViewChange(DoViewChange),
    StartView(StartView),
}

impl Message {
    pub fn view_number(&self) -> usize {
        match self {
            Message::Request(request) => request.v,
            Message::Prepare(prepare) => prepare.v,
            Message::PrepareOk(prepare_ok) => prepare_ok.v,
            Message::Reply(reply) => reply.v,
            Message::Commit(commit) => commit.v,
            Message::Inform(inform) => inform.v,
            Message::Ping(ping) => ping.v,
            Message::DoViewChange(do_view_change) => do_view_change.v,
            Message::StartView(start_view) => start_view.v,
        }
    }
}
