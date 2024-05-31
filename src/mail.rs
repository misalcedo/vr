use crate::message::{Message, ProtocolMessage, Reply};
use std::collections::VecDeque;

pub struct Mailbox {
    outbox: VecDeque<Message>,
    pending: VecDeque<Message>,
    inbox: VecDeque<Message>,
}

impl Mailbox {
    /// Reply to a client from the primary.
    pub fn reply(&mut self, message: Reply) {
        self.outbox.push_back(Message::Reply(message))
    }

    /// Send a message from a replica to another replica.
    pub fn send(&mut self, to: usize, message: impl Into<ProtocolMessage>) {
        self.outbox.push_back(Message::Protocol(to, message.into()))
    }

    /// Receive a message from a replica to another replica.
    pub fn receive(&mut self) -> Option<Message> {
        self.inbox.pop_front()
    }

    /// Push a protocol message to the back of the queue.
    pub fn push(&mut self, protocol: impl Into<Message>) {
        self.inbox.push_back(protocol.into())
    }

    /// Get the next outbound message to send to a replica.
    pub fn pop(&mut self) -> Option<Message> {
        let head = self.outbox.pop_front()?;
        self.pending.push_back(head);
        Some(head)
    }
}
