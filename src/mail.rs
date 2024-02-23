use crate::protocol::{Message, Protocol};
use crate::request::{ClientIdentifier, Reply, Request};
use std::future::Future;

pub trait Inbox {
    type Request;
    type Prediction;

    fn receive(
        &mut self,
    ) -> Either<Request<Self::Request>, Protocol<Self::Request, Self::Prediction>>;

    fn receive_response<'a, M, F>(&mut self, predicate: F) -> M
    where
        M: Message<'a>
            + TryFrom<
                Protocol<Self::Request, Self::Prediction>,
                Error = Protocol<Self::Request, Self::Prediction>,
            > + Into<Protocol<Self::Request, Self::Prediction>>,
        F: Fn(&M) -> bool;
}

pub enum Either<L, R> {
    Left(L),
    Right(R),
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
