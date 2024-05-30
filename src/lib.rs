//! A Primary Copy Method to Support Highly-Available Distributed Systems.

use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::net::IpAddr;

pub struct Mailbox<'a> {
    outbox: VecDeque<Message<'a>>,
    pending: VecDeque<Message<'a>>,
    inbox: VecDeque<Protocol<'a>>,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct Message<'a> {
    to: usize,
    protocol: Protocol<'a>,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum Protocol<'a> {
    GetState(GetState),
    NewState(NewState<'a>),
}

impl Mailbox {
    /// Send a message from a replica to another replica.
    fn send<'a>(&mut self, to: usize, protocol: impl Into<Protocol>) {
        self.outbox.push_back(Message {
            to,
            protocol: protocol.into(),
        })
    }

    /// Receive a message from a replica to another replica.
    async fn receive<P: TryFrom<Protocol, Error = Protocol>>(&mut self) -> Option<P> {
        let head = self.inbox.pop_front()?;
        match P::try_from(head) {
            Ok(unwrapped) => Some(unwrapped),
            Err(wrapped) => {
                self.inbox.push_front(wrapped);
                None
            }
        }
    }

    /// Get the next outbound message to send to a replica.
    fn pop(&mut self) -> Option<Message> {
        let head = self.outbox.pop_front()?;
        self.pending.push_back(head);
        Some(head)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct GetState {
    view: usize,
    op_number: usize,
    index: usize,
}

impl From<GetState> for Protocol {
    fn from(value: GetState) -> Self {
        Protocol::GetState(value)
    }
}

impl<'a> TryFrom<Protocol<'a>> for GetState {
    type Error = Protocol<'a>;

    fn try_from(value: Protocol) -> Result<Self, Self::Error> {
        match value {
            Protocol::GetState(unwrapped) => Ok(unwrapped),
            wrapped => Err(wrapped),
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct Request<'a>;

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct NewState<'a> {
    view: usize,
    log: [Request<'a>],
    op_number: usize,
    commit: usize,
}

impl From<NewState> for Protocol {
    fn from(value: NewState) -> Self {
        Protocol::NewState(value)
    }
}

impl<'a> TryFrom<Protocol<'a>> for NewState {
    type Error = Protocol<'a>;

    fn try_from(value: Protocol) -> Result<Self, Self::Error> {
        match value {
            Protocol::NewState(unwrapped) => Ok(unwrapped),
            wrapped => Err(wrapped),
        }
    }
}

pub struct Configuration {
    addresses: Vec<IpAddr>,
}

impl Configuration {
    pub fn len(&self) -> usize {
        self.addresses.len()
    }
}

/// State transfer is used by a node that has gotten behind (but hasn't crashed) to bring itself up-to-date.
/// The replica sends a message to one of the other replicas to learn about requests after a given op-number.
async fn transfer_state(
    replicas: usize,
    message: GetState,
    mailbox: &mut Mailbox,
) -> Result<NewState, GetState> {
    let mut to = message.index;
    while to == message.index {
        to = rand::thread_rng().gen_range(0..replicas);
    }

    mailbox.send(to, message);
    mailbox.receive::<NewState>().await
}
