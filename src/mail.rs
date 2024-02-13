use crate::protocol::Message;
use crate::request::{ClientIdentifier, Reply};

pub trait Mailbox {
    type Reply;

    fn broadcast<'a, M>(&mut self, message: &M)
    where
        M: Message<'a>;

    fn reply(&mut self, client: ClientIdentifier, reply: &Reply<Self::Reply>);
}
