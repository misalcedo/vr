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

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub enum Message {
    Request(Request),
    Prepare {
        /// The current view-number.
        v: usize,
        /// The op-number assigned to the request.
        n: usize,
        /// The message received from the client.
        m: Request,
    },
    PrepareOk {
        /// The current view-number known to the replica.
        v: usize,
        /// The op-number assigned to the accepted prepare message.
        n: usize,
        /// The index of the replica accepting the prepare message.
        i: usize
    },
    Reply(Reply),
}