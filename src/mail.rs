use crate::protocol::{Message, Protocol};
use crate::request::{ClientIdentifier, Reply, Request};

pub trait Inbox {
    type Request;
    type Prediction;

    fn receive(
        &mut self,
    ) -> impl std::future::Future<
        Output = Either<Request<Self::Request>, Protocol<Self::Request, Self::Prediction>>,
    > + Unpin;
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
