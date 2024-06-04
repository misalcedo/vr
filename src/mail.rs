use crate::message::{InboundMessage, OutboundMessage, ProtocolMessage, Reply};
use std::collections::VecDeque;

/// Implements inbound and outbound queues for replicas.
#[derive(Default)]
pub struct Mailbox {
    outbox: VecDeque<OutboundMessage>,
    inbox: VecDeque<InboundMessage>,
}

impl Mailbox {
    /// Add a reply to a client from the primary to the outbound queue.
    pub fn reply(&mut self, message: Reply) {
        self.outbox.push_back(OutboundMessage::Reply(message))
    }

    /// Add a protocol message to the outbound queue.
    pub fn send(&mut self, to: usize, message: impl Into<ProtocolMessage>) {
        self.outbox
            .push_back(OutboundMessage::Protocol(to, message.into()))
    }

    /// Receive a message from the inbound queue.
    pub fn receive(&mut self) -> Option<InboundMessage> {
        self.inbox.pop_front()
    }

    /// Push a message to the inbound queue.
    /// Messages for the current view are pushed to the back of the queue.
    /// Messages with a higher view number are pushed to the front of the queue.
    pub fn push(&mut self, message: impl Into<InboundMessage>) {
        self.inbox.push_back(message.into());
    }

    /// Get the next outbound message to deliver.
    /// Re-sending messages is the responsibility of the caller.
    pub fn pop(&mut self) -> Option<OutboundMessage> {
        let head = self.outbox.pop_front()?;
        Some(head)
    }
}
