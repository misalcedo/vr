//! A Primary Copy Method to Support Highly-Available Distributed Systems.

use crate::configuration::Configuration;
use crate::mail::Mailbox;
use crate::message::*;
use crate::table::ClientTable;
use rand::Rng;
use std::cmp::Ordering;

mod configuration;
mod mail;
mod message;
mod table;

pub enum Status {
    /// Normal case processing of user requests.
    Normal,
    /// View changes to select a new primary.
    ViewChange,
    /// Recovery of a failed replica so that it can rejoin the group.
    Recovery,
}

pub struct Replica {
    view: usize,
    op_number: usize,
    commit: usize,
    index: usize,
    configuration: Configuration,
    log: Vec<Request>,
    client_table: ClientTable,
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

            // The client sends a REQUEST message to the primary.
            Some(Message::Request(request)) if self.is_primary() => {
                self.receive_request(request, mailbox);
            }
            // If the sender is behind, the receiver drops the message.
            Some(Message::Protocol(_, message)) if message.view() < self.view => {}

            // If the sender is ahead, the replica performs a state transfer:
            // it requests information it is missing from the other replicas and uses this information
            // to bring itself up to date before processing the message.
            Some(Message::Protocol(index, message)) if message.view() > self.view => {
                self.trim_log();
                self.start_state_transfer(mailbox);
                mailbox.push(Message::Protocol(index, message));
            }
            Some(Message::Protocol(_, ProtocolMessage::Prepare(message))) => {}
            Some(_) => {}
        }
    }

    /// When the primary receives a request,
    /// it compares the request-number in the request with the information in the client table.
    /// If the request-number isn’t bigger than the information in the table it drops the request,
    /// but it will re-send the response if the request is the most recent one from this client,
    /// and it has already been executed.
    ///
    /// The primary advances op-number, adds the request to the end of the log,
    /// and updates the information for this client in the client-table to contain the new request number.
    /// Then it sends a PREPARE message to the other replicas.
    fn receive_request(&mut self, request: Request, mailbox: &mut Mailbox) {
        match self.client_table.compare(&request) {
            Ordering::Less => {}
            Ordering::Equal => {
                if let Some(reply) = self.client_table.reply(&request) {
                    mailbox.reply(*reply);
                }
            }
            Ordering::Greater => {
                let offset = self.log.len();

                self.op_number += 1;
                self.log.push(request);

                let request = &self.log[offset];

                self.client_table.start(&request);
                self.broadcast(
                    mailbox,
                    Prepare {
                        view: self.view,
                        op_number: self.op_number,
                        commit: self.commit,
                        request: *request,
                    },
                );
            }
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
