use crate::protocol::Message;
use crate::request::{ClientIdentifier, Reply};

pub trait Inbox {
    fn receive<'a, M>(&mut self) -> M
    where
        M: Message<'a>;
}

pub trait Outbox {
    type Reply;

    fn send<'a, M>(&mut self, index: usize, message: &M)
    where
        M: Message<'a>;

    fn broadcast<'a, M>(&mut self, message: &M)
    where
        M: Message<'a>;

    fn reply(&mut self, client: ClientIdentifier, reply: &Reply<Self::Reply>);
}

pub trait Mailbox: Inbox + Outbox {}
