use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};

use rand::Rng;

use crate::configuration::Configuration;
use crate::mail::Mailbox;
use crate::message::{
    Commit, GetState, Message, Prepare, PrepareOk, ProtocolMessage, Reply, Request,
};
use crate::table::ClientTable;

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
    prepared: VecDeque<HashSet<usize>>,
}

impl Replica {
    pub fn new(configuration: Configuration, index: usize) -> Self {
        Self {
            view: 0,
            op_number: 0,
            commit: 0,
            index,
            configuration,
            log: vec![],
            client_table: Default::default(),
            status: Status::Normal,
            prepared: Default::default(),
        }
    }

    /// ## Examples
    /// ### Single Request
    /// ```rust
    /// use viewstamped_replication::{Configuration, Mailbox, Replica};
    /// use viewstamped_replication::message::*;
    ///
    /// let configuration = Configuration::new([
    ///     "127.0.0.1".parse().unwrap(),
    ///     "127.0.0.2".parse().unwrap(),
    ///     "127.0.0.3".parse().unwrap(),
    /// ]);
    /// let mut primary = Replica::new(configuration.clone(), 0);
    /// let mut backup1 = Replica::new(configuration.clone(), 1);
    /// let mut mailbox = Mailbox::default();
    ///
    /// // pretend to receive a request over the network.
    /// mailbox.push(Request {
    ///     operation: (),
    ///     client: 1,
    ///     id: 1,
    /// });
    /// primary.receive(&mut mailbox);
    ///
    /// // pretend to deliver the message over the network.
    /// let message = mailbox.pop().unwrap();
    /// mailbox.push(message);
    /// backup1.receive(&mut mailbox);
    ///
    /// // ignore the prepare message for backup2.
    /// mailbox.pop().unwrap();
    ///
    /// // pretend to deliver the message over the network.
    /// let message = mailbox.pop().unwrap();
    /// mailbox.push(message);
    /// primary.receive(&mut mailbox);
    ///
    /// assert_eq!(mailbox.pop(), Some(Reply {
    ///     view: 0,
    ///     result: (),
    ///     client: 1,
    ///     id: 1,
    /// }.into()));
    /// ```
    pub fn receive(&mut self, mailbox: &mut Mailbox) {
        match self.status {
            Status::Normal => self.normal_receive(mailbox),
            Status::ViewChange => {}
            Status::Recovery => {}
        }
    }

    fn normal_receive(&mut self, mailbox: &mut Mailbox) {
        match mailbox.receive() {
            None if self.is_primary() => {
                // Normally the primary informs backups about the commit when it sends the next PREPARE message;
                // this is the purpose of the commit-number in the PREPARE message.
                // However, if the primary does not receive a new client request in a timely way,
                // it instead informs the backups of the latest commit by sending them a COMMIT message
                // (note that in this case commit-number = op-number).
                self.broadcast(
                    mailbox,
                    Commit {
                        view: self.view,
                        commit: self.commit,
                    },
                );
            }

            None => {}

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
            Some(Message::Protocol(_, ProtocolMessage::Prepare(message))) if !self.is_primary() => {
                self.receive_prepare(message, mailbox)
            }
            Some(Message::Protocol(_, ProtocolMessage::PrepareOk(message)))
                if self.is_primary() =>
            {
                self.receive_prepare_ok(message, mailbox)
            }

            Some(Message::Protocol(_, ProtocolMessage::Commit(message))) if !self.is_primary() => {
                self.receive_commit(message, mailbox)
            }
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

                // enable tracking prepared backups.
                self.prepared.push_back(Default::default());
            }
        }
    }

    fn primary(&self) -> usize {
        self.view % self.configuration.len()
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

    /// Backups process PREPARE messages in order:
    /// a backup won’t prepare a request until it has entries for all earlier requests in its log.
    ///
    /// When a backup receives a PREPARE message,
    /// it waits until it has entries in its log for all earlier requests
    /// (doing state transfer if necessary to get the missing information).
    ///
    /// Then it increments its op-number,
    /// adds the request to the end of its log,
    /// updates the client’s information in the client-table,
    /// and sends a PREPAREOK message to the primary to indicate that this operation and all earlier ones have prepared locally.
    fn receive_prepare(&mut self, prepare: Prepare, mailbox: &mut Mailbox) {
        if prepare.op_number < self.op_number {
            return;
        }

        if prepare.op_number > self.op_number + 1 {
            self.start_state_transfer(mailbox);
            mailbox.push(Message::Protocol(self.index, prepare.into()));
            return;
        }

        self.op_number += 1;
        self.client_table.start(&prepare.request);
        mailbox.send(
            self.primary(),
            PrepareOk {
                view: self.view,
                op_number: self.op_number,
                index: self.index,
            },
        );

        self.receive_commit(Commit::from(prepare), mailbox);
    }

    /// The primary waits for f PREPAREOK messages from different backups;
    /// at this point it considers the operation (and all earlier ones) to be committed.
    /// Then, after it has executed all earlier operations (those assigned smaller op-numbers),
    /// the primary executes the operation by making an up-call to the service code,
    /// and increments its commit-number.
    ///
    /// Then it sends a REPLY message to the client.
    /// The primary also updates the client’s entry in the client-table to contain the result.
    fn receive_prepare_ok(&mut self, prepare_ok: PrepareOk, mailbox: &mut Mailbox) {
        if prepare_ok.op_number <= self.commit {
            return;
        }

        // track prepared backups.
        let offset = (prepare_ok.op_number - self.commit) - 1;
        let prepared = &mut self.prepared[offset];

        prepared.insert(prepare_ok.index);

        if prepared.len() < self.configuration.threshold() {
            return;
        }

        self.commit(prepare_ok.op_number, mailbox);
    }

    fn commit(&mut self, operation: usize, mailbox: &mut Mailbox) {
        while self.commit < operation {
            self.commit += 1;

            let request = &self.log[self.commit - 1];
            let reply = Reply {
                view: self.view,
                result: (),
                client: request.client,
                id: request.id,
            };

            if self.is_primary() {
                mailbox.reply(reply);
            }

            self.client_table.finish(request, reply);

            // disable tracking prepared backups.
            self.prepared.pop_front();
        }
    }

    /// When a backup learns of a commit, it waits until it has the request in its log
    /// (which may require state transfer) and until it has executed all earlier operations.
    /// Then it executes the operation by performing the up-call to the service code,
    /// increments its commit-number,
    /// updates the client’s entry in the client-table,
    /// but does not send the reply to the client.
    fn receive_commit(&mut self, commit: Commit, mailbox: &mut Mailbox) {
        if commit.commit > self.op_number {
            self.start_state_transfer(mailbox);
            mailbox.push(Message::Protocol(self.index, commit.into()));
            return;
        }

        self.commit(commit.commit, mailbox);
    }
}
