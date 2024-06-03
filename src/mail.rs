use crate::message::{Message, ProtocolMessage, Reply};
use std::collections::VecDeque;

/// Implements inbound and outbound queues for replicas.
#[derive(Default)]
pub struct Mailbox {
    view: usize,
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

    /// Push a message to the inbound queue.
    /// Messages for the current view are pushed to the back of the queue.
    /// Messages with a higher view number are pushed to the front of the queue.
    pub fn push(&mut self, message: impl Into<Message>) {
        match message.into() {
            Message::Protocol(index, message) if message.view() > self.view => {
                self.inbox.push_front(Message::Protocol(index, message))
            }
            message => self.inbox.push_back(message),
        }
    }

    /// Get the next outbound message to deliver.
    pub fn pop(&mut self) -> Option<Message> {
        let head = self.outbox.pop_front()?;
        Some(head)
    }

    /// Update the current view number.
    pub fn set_view(&mut self, view: usize) {
        self.view = view;
    }
}
