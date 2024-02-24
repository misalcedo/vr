use crate::mail::{Either, Inbox, Mailbox, Outbox};
use crate::protocol::{Message, Protocol};
use crate::request::{ClientIdentifier, Reply, Request};
use std::collections::VecDeque;
use std::future::Future;

type Payload<R, P> = Either<Request<R>, Protocol<R, P>>;

pub struct LocalMailbox<Req, Pre, Rep> {
    sender: (),
    receiver: (),
    inbound_requests: VecDeque<Request<Req>>,
    inbound_messages: VecDeque<Protocol<Req, Pre>>,
    outbound_replies: VecDeque<(ClientIdentifier, Reply<Rep>)>,
    outbound_messages: VecDeque<(usize, Protocol<Req, Pre>)>,
}

impl<Req, Pre, Rep> Default for LocalMailbox<Req, Pre, Rep> {
    fn default() -> Self {
        Self {
            sender: (),
            receiver: (),
            inbound_requests: Default::default(),
            inbound_messages: Default::default(),
            outbound_replies: Default::default(),
            outbound_messages: Default::default(),
        }
    }
}

impl<Req, Pre, Rep> Inbox for LocalMailbox<Req, Pre, Rep> {
    fn receive<'a, M>(&mut self) -> M
    where
        M: Message<'a>,
    {
        todo!()
    }
}

impl<Req, Pre, Rep> Outbox for LocalMailbox<Req, Pre, Rep> {
    type Reply = Rep;

    fn send<'a, M>(&mut self, index: usize, message: &M)
    where
        M: Message<'a>,
    {
        todo!()
    }

    fn broadcast<'a, M>(&mut self, message: &M)
    where
        M: Message<'a>,
    {
        todo!()
    }

    fn reply(&mut self, client: ClientIdentifier, reply: &Reply<Self::Reply>) {
        todo!()
    }
}

impl<Req, Pre, Rep> Mailbox for LocalMailbox<Req, Pre, Rep> {}
