use crate::message::{Message, ProtocolMessage, Reply};
use std::collections::VecDeque;

#[derive(Default)]
pub struct Mailbox {
    outbox: VecDeque<Message>,
    inbox: VecDeque<Message>,
}

impl Mailbox {
    /// Add a reply to a client from the primary to the outbound queue.
    pub fn reply(&mut self, message: Reply) {
        self.outbox.push_back(Message::Reply(message))
    }

    /// Add a protocol message to the outbound queue.
    pub fn send(&mut self, to: usize, message: impl Into<ProtocolMessage>) {
        self.outbox.push_back(Message::Protocol(to, message.into()))
    }

    /// Receive a message from the inbound queue.
    pub fn receive(&mut self) -> Option<Message> {
        self.inbox.pop_front()
    }

    /// Push a message to the back of the inbound queue.
    pub fn push(&mut self, message: impl Into<Message>) {
        self.inbox.push_back(message.into())
    }

    /// Get the next outbound message to deliver.
    pub fn pop(&mut self) -> Option<Message> {
        let head = self.outbox.pop_front()?;
        Some(head)
    }
}
