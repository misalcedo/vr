use crate::mail::{Inbox, Mailbox, Outbox};
use crate::protocol::Message;
use crate::request::{ClientIdentifier, Reply, Request};
use bytes::Bytes;
use std::collections::VecDeque;

#[derive(Default)]
pub struct BufferedMailbox {
    requests: VecDeque<Bytes>,
    replies: VecDeque<Bytes>,
    inbound_messages: VecDeque<Bytes>,
    outbound_messages: VecDeque<Bytes>,
}

impl BufferedMailbox {
    pub fn deliver<R>(&mut self, request: Request<R>) {
        todo!()
    }

    pub fn is_empty(&self) -> bool {
        self.inbound_messages.is_empty()
    }
}

impl Inbox for BufferedMailbox {
    fn receive<M>(&mut self) -> M
    where
        M: Message,
    {
        todo!()
    }
}

impl Outbox for BufferedMailbox {
    fn send<M>(&mut self, index: usize, message: &M)
    where
        M: Message,
    {
        todo!()
    }

    fn broadcast<M>(&mut self, message: &M)
    where
        M: Message,
    {
        todo!()
    }

    fn reply<R>(&mut self, client: ClientIdentifier, reply: &Reply<R>) {
        todo!()
    }
}

impl Mailbox for BufferedMailbox {}
