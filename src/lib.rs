//! A Primary Copy Method to Support Highly-Available Distributed Systems.

use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::ops::Range;

pub struct Mailbox {
    outbox: VecDeque<Message>,
    pending: VecDeque<Message>,
    inbox: VecDeque<Message>,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum Message {
    Request(Request),
    Protocol(usize, ProtocolMessage),
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum ProtocolMessage {
    Prepare(Prepare),
    PrepareOk(PrepareOk),
    Commit(Commit),
    GetState(GetState),
    NewState(NewState),
}

impl ProtocolMessage {
    pub fn view(&self) -> usize {
        match self {
            ProtocolMessage::Prepare(m) => m.view,
            ProtocolMessage::PrepareOk(m) => m.view,
            ProtocolMessage::Commit(m) => m.view,
            ProtocolMessage::GetState(m) => m.view,
            ProtocolMessage::NewState(m) => m.view,
        }
    }
}

impl Mailbox {
    /// Send a message from a replica to another replica.
    fn send(&mut self, to: usize, message: impl Into<ProtocolMessage>) {
        self.outbox.push_back(Message::Protocol(to, message.into()))
    }

    /// Receive a message from a replica to another replica.
    fn receive(&mut self) -> Option<Message> {
        self.inbox.pop_front()
    }

    /// Push a protocol message to the back of the queue.
    fn push(&mut self, protocol: impl Into<Message>) {
        self.inbox.push_back(protocol.into())
    }

    /// Get the next outbound message to send to a replica.
    fn pop(&mut self) -> Option<Message> {
        let head = self.outbox.pop_front()?;
        self.pending.push_back(head);
        Some(head)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Prepare {
    view: usize,
    op_number: usize,
    commit: usize,
    request: Request,
}

impl From<Prepare> for ProtocolMessage {
    fn from(value: Prepare) -> Self {
        ProtocolMessage::Prepare(value)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrepareOk {
    view: usize,
    op_number: usize,
    index: usize,
}

impl From<PrepareOk> for ProtocolMessage {
    fn from(value: PrepareOk) -> Self {
        ProtocolMessage::PrepareOk(value)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Commit {
    view: usize,
    commit: usize,
}

impl From<Commit> for ProtocolMessage {
    fn from(value: Commit) -> Self {
        ProtocolMessage::Commit(value)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct GetState {
    view: usize,
    op_number: usize,
    index: usize,
}

impl From<GetState> for ProtocolMessage {
    fn from(value: GetState) -> Self {
        ProtocolMessage::GetState(value)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Request {
    operation: (),
    client: u128,
    id: u128,
}

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewState {
    view: usize,
    log: [Request; 0], // TODO
    op_number: usize,
    commit: usize,
}

impl From<NewState> for ProtocolMessage {
    fn from(value: NewState) -> Self {
        ProtocolMessage::NewState(value)
    }
}

pub struct Configuration {
    addresses: Vec<IpAddr>,
}

impl IntoIterator for &Configuration {
    type Item = usize;
    type IntoIter = Range<usize>;

    fn into_iter(self) -> Self::IntoIter {
        0..self.addresses.len()
    }
}

impl Configuration {
    pub fn len(&self) -> usize {
        self.addresses.len()
    }
}

pub struct ClientTable {}

impl ClientTable {
    pub fn compare_to(&self, request: &Request) -> Ordering {
        Ordering::Less
    }

    pub fn insert(&mut self, request: &Request) {}
}

pub enum Status {
    Normal,
    ViewChange,
    Recovery,
}

pub struct Replica {
    view: usize,
    op_number: usize,
    commit: usize,
    index: usize,
    configuration: Configuration,
    log: Vec<Request>,
    cache: ClientTable,
    status: Status,
}

impl Replica {
    pub fn receive(&mut self, mailbox: &mut Mailbox) {
        match self.status {
            Status::Normal => self.normal_receive(mailbox),
            Status::ViewChange => {}
            Status::Recovery => {}
        }
    }

    pub fn normal_receive(&mut self, mailbox: &mut Mailbox) {
        match mailbox.receive() {
            None => {
                if self.is_primary() {
                    self.broadcast(
                        mailbox,
                        Commit {
                            view: self.view,
                            commit: self.commit,
                        },
                    );
                } else {
                    todo!("handle idle backup")
                }
            }
            Some(Message::Request(request)) if self.is_primary() => {
                match self.cache.compare_to(&request).reverse() {
                    Ordering::Greater => {
                        let offset = self.log.len();

                        self.op_number += 1;
                        self.log.push(request);

                        let request = &self.log[offset];

                        self.cache.insert(&request);
                        self.broadcast(
                            mailbox,
                            Prepare {
                                view: self.view,
                                op_number: self.op_number,
                                commit: self.commit,
                                request: request.clone(), // TODO: turn this into a borrowed request type.
                            },
                        );
                    }
                    Ordering::Equal => todo!("resend reply if complete"),
                    Ordering::Less => {}
                }
            }
            Some(Message::Protocol(_, message)) if message.view() < self.view => {}
            Some(Message::Protocol(index, message)) if message.view() > self.view => {
                self.trim_log();
                self.start_state_transfer(mailbox);
                mailbox.push(Message::Protocol(index, message));
            }
            Some(Message::Protocol(_, ProtocolMessage::Prepare(message))) => {}
            Some(_) => {}
        }
    }

    fn is_primary(&self) -> bool {
        self.index == (self.view % self.configuration.len())
    }

    fn broadcast(&self, mailbox: &mut Mailbox, message: impl Into<ProtocolMessage>) {
        let protocol_message = message.into();

        for index in self.configuration.into_iter() {
            if self.index == index {
                continue;
            }

            mailbox.send(index, protocol_message)
        }
    }

    fn trim_log(&mut self) {
        self.log.truncate(self.commit);
        self.op_number = self.commit;
    }

    fn start_state_transfer(&self, mailbox: &mut Mailbox) {
        let message = GetState {
            view: self.view,
            op_number: self.op_number,
            index: self.index,
        };
        let mut to = message.index;
        while to == message.index {
            to = rand::thread_rng().gen_range(0..self.configuration.len());
        }

        mailbox.send(to, message);
    }
}
