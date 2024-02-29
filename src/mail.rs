use crate::protocol::Message;
use crate::request::{ClientIdentifier, Reply};

pub trait Inbox {
    fn receive<M>(&mut self) -> M
    where
        M: Message;
}

pub trait Outbox {
    fn send<M>(&mut self, index: usize, message: &M)
    where
        M: Message;

    fn broadcast<M>(&mut self, message: &M)
    where
        M: Message;

    fn reply<R>(&mut self, client: ClientIdentifier, reply: &Reply<R>);
}

pub trait Mailbox: Inbox + Outbox {}
