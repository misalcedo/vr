use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};

use rand::Rng;
use uuid::Uuid;

use crate::configuration::Configuration;
use crate::mail::Mailbox;
use crate::message::{
    Commit, DoViewChange, GetState, Message, NewState, Prepare, PrepareOk, ProtocolMessage,
    Recover, RecoveryResponse, Reply, Request, StartView, StartViewChange,
};
use crate::table::ClientTable;
use crate::Service;

pub enum Status {
    /// Normal case processing of user requests.
    Normal,
    /// View changes to select a new primary.
    ViewChange,
    /// Recovery of a failed replica so that it can rejoin the group.
    Recovery,
}

/// A replica implements all the sub-protocols of the Viewstamped Replication protocol.
/// The replica does not track operation execution separately from committed operations.
pub struct Replica<S> {
    service: S,
    view: usize,
    last_normal_view: usize,
    op_number: usize,
    committed: usize,
    index: usize,
    configuration: Configuration,
    log: Vec<Request>,
    client_table: ClientTable,
    status: Status,
    prepared: VecDeque<HashSet<usize>>,
    view_change_votes: HashSet<usize>,
    view_change_state: HashMap<usize, DoViewChange>,
    recovery_responses: HashMap<usize, RecoveryResponse>,
    nonce: u128,
}

impl<S> Replica<S>
where
    S: Service + Default,
{
    pub fn new(configuration: Configuration, index: usize) -> Self {
        Self {
            service: Default::default(),
            view: 0,
            last_normal_view: 0,
            op_number: 0,
            committed: 0,
            index,
            configuration,
            log: vec![],
            client_table: Default::default(),
            status: Status::Normal,
            prepared: Default::default(),
            view_change_votes: Default::default(),
            view_change_state: Default::default(),
            recovery_responses: Default::default(),
            nonce: Uuid::now_v7().as_u128(),
        }
    }
}

impl<S> Replica<S>
where
    S: Service,
{
    /// Implements the various sub-protocols of VR.
    ///
    /// Calling receive without a message in the mailbox triggers idle behavior.
    /// The specific behavior depends on the status of the replica.
    pub fn receive(&mut self, mailbox: &mut Mailbox) {
        let message = mailbox.receive();
        match self.status {
            Status::Normal => self.normal_receive(message, mailbox),
            Status::ViewChange => self.view_change_receive(message, mailbox),
            Status::Recovery => self.recovery_receive(message, mailbox),
        }
    }

    ///  The recovering replica, i, sends a RECOVERY message to all other replicas, where x is a nonce.
    pub fn recover(&mut self, mailbox: &mut Mailbox) {
        self.status = Status::Recovery;
        self.recovery_responses.clear();
        self.broadcast(
            mailbox,
            Recover {
                index: self.index,
                nonce: self.nonce,
            },
        );
    }

    fn normal_receive(&mut self, message: Option<Message>, mailbox: &mut Mailbox) {
        match message {
            // Normally the primary informs backups about the commit when it sends the next PREPARE message;
            // this is the purpose of the commit-number in the PREPARE message.
            // However, if the primary does not receive a new client request in a timely way,
            // it instead informs the backups of the latest commit by sending them a COMMIT message
            // (note that in this case commit-number = op-number).
            None if self.is_primary() => {
                self.broadcast(
                    mailbox,
                    Commit {
                        view: self.view,
                        commit: self.committed,
                    },
                );
            }
            None => {
                self.start_view_change(self.view + 1, mailbox);
            }
            // The client sends a REQUEST message to the primary.
            Some(Message::Request(request)) if self.is_primary() => {
                self.receive_request(request, mailbox);
            }
            Some(Message::Protocol(_, ProtocolMessage::Recover(message))) => {
                self.receive_recover(message, mailbox)
            }
            // If the sender is behind, the receiver drops the message.
            Some(Message::Protocol(_, message)) if message.view() < self.view => {}
            Some(Message::Protocol(index, ProtocolMessage::StartViewChange(message)))
                if message.view > self.view =>
            {
                self.start_view_change(message.view, mailbox);
                self.view_change_receive(Some(Message::Protocol(index, message.into())), mailbox);
            }
            Some(Message::Protocol(index, ProtocolMessage::DoViewChange(message)))
                if message.view > self.view =>
            {
                self.start_view_change(message.view, mailbox);
                self.view_change_receive(Some(Message::Protocol(index, message.into())), mailbox);
            }
            Some(Message::Protocol(_, ProtocolMessage::NewState(message)))
                if message.view >= self.view =>
            {
                self.receive_new_state(message, mailbox)
            }
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
            Some(Message::Protocol(_, ProtocolMessage::GetState(message))) => {
                self.receive_get_state(message, mailbox)
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
                    mailbox.reply(reply.clone());
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
                        commit: self.committed,
                        request: request.clone(),
                    },
                );

                // start tracking prepared backups.
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

            mailbox.send(index, protocol_message.clone())
        }
    }

    fn trim_log(&mut self) {
        self.log.truncate(self.committed);
        self.op_number = self.committed;
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
    fn receive_prepare(&mut self, message: Prepare, mailbox: &mut Mailbox) {
        // NOTE: ignore operations we have already prepared.
        if message.op_number <= self.op_number {
            return;
        }

        if message.op_number > self.op_number + 1 {
            self.start_state_transfer(mailbox);
            mailbox.push(Message::Protocol(self.index, message.into()));
            return;
        }

        self.op_number += 1;
        self.client_table.start(&message.request);
        mailbox.send(
            self.primary(),
            PrepareOk {
                view: self.view,
                op_number: self.op_number,
                index: self.index,
            },
        );

        self.receive_commit(Commit::from(message), mailbox);
    }

    /// The primary waits for f PREPAREOK messages from different backups;
    /// at this point it considers the operation (and all earlier ones) to be committed.
    /// Then, after it has executed all earlier operations (those assigned smaller op-numbers),
    /// the primary executes the operation by making an up-call to the service code,
    /// and increments its commit-number.
    ///
    /// Then it sends a REPLY message to the client.
    /// The primary also updates the client’s entry in the client-table to contain the result.
    fn receive_prepare_ok(&mut self, message: PrepareOk, mailbox: &mut Mailbox) {
        // NOTE: ignore operations we have already committed.
        if message.op_number <= self.committed {
            return;
        }

        // track prepared backups.
        let offset = (message.op_number - self.committed) - 1;
        let prepared = &mut self.prepared[offset];

        prepared.insert(message.index);

        // SAFETY: wait until we have at least f PREPAREOK messages.
        if prepared.len() < self.configuration.threshold() {
            return;
        }

        self.execute(message.op_number, mailbox);
    }

    fn execute(&mut self, committed: usize, mailbox: &mut Mailbox) {
        while self.committed < committed {
            let request = &self.log[self.committed];
            let reply = Reply {
                view: self.view,
                result: self.service.invoke(request.operation.clone()),
                client: request.client,
                id: request.id,
            };

            self.committed += 1;

            if self.is_primary() {
                mailbox.reply(reply.clone());

                // stop tracking prepared backups.
                self.prepared.pop_front();
            }

            self.client_table.finish(request, reply);
        }
    }

    /// When a backup learns of a commit, it waits until it has the request in its log
    /// (which may require state transfer) and until it has executed all earlier operations.
    /// Then it executes the operation by performing the up-call to the service code,
    /// increments its commit-number,
    /// updates the client’s entry in the client-table,
    /// but does not send the reply to the client.
    fn receive_commit(&mut self, message: Commit, mailbox: &mut Mailbox) {
        if message.commit > self.op_number {
            self.start_state_transfer(mailbox);
            mailbox.push(Message::Protocol(self.index, message.into()));
            return;
        }

        self.execute(message.commit, mailbox);
    }

    /// A replica responds to a GETSTATE message only if its status is normal, and it is currently in view v.
    /// In this case it sends a NEWSTATE message.
    fn receive_get_state(&mut self, message: GetState, mailbox: &mut Mailbox) {
        mailbox.send(
            message.index,
            NewState {
                view: self.view,
                log: [], // log after message.op_number
                op_number: self.op_number,
                commit: self.committed,
            },
        );
    }

    /// When a replica receives the NEWSTATE message,
    /// it appends the log in the message to its log and updates its state using the other information in the message.
    fn receive_new_state(&mut self, message: NewState, mailbox: &mut Mailbox) {
        // SAFETY: Only use new state that matches what we requested.
        if (self.op_number + message.log.len()) != message.op_number {
            return;
        }

        // Because of garbage collecting the log,
        // it’s possible for there to be a gap between the last operation known to the slow replica and what the responder knows.
        // Should a gap occur,
        // the slow replica first brings itself almost up to date using application state
        // (like a recovering node would do) to get to a recent checkpoint,
        // and then completes the job by obtaining the log forward from the point.
        // In the process of getting the checkpoint,
        // it moves to the view in which that checkpoint was taken.
        if message.log.is_empty() {
            // TODO: handle garbage collection.
        }

        self.log.extend_from_slice(&message.log);
        self.op_number = message.op_number;
        self.view = self.view;

        // SAFETY: We have not updated the replica's commit-number to be the message's.
        // We do this in order to re-use the method from the normal protocol to execute committed operations.
        self.execute(message.commit, mailbox);
        self.client_table.remove_pending();

        let mut current = self.committed;
        while let Some(request) = self.log.get(current) {
            self.client_table.start(request);

            // SAFETY: The op-number of the current operation is 1 more than its index into the log.
            current += 1;
        }
    }

    /// A replica that notices the need for a view change advances its view-number,
    /// sets its status to viewchange,
    /// and sends a STARTVIEWCHANGE message to the all other replicas.
    /// A replica notices the need for a view change either based on its own timer,
    /// or because it receives a STARTVIEWCHANGE or DOVIEWCHANGE message for a view with a larger
    /// number than its own view-number.
    fn start_view_change(&mut self, new_view: usize, mailbox: &mut Mailbox) {
        self.view = new_view;
        self.status = Status::ViewChange;
        self.broadcast(
            mailbox,
            StartViewChange {
                view: self.view,
                index: self.index,
            },
        );

        // Reset tracker of STARTVIEWCHANGE messages.
        self.view_change_votes.clear();
        // Reset tracker on DOVIEWCHANGE messages.
        self.view_change_state.clear();
    }

    fn view_change_receive(&mut self, message: Option<Message>, mailbox: &mut Mailbox) {
        match message {
            None => {
                // A view change may not succeed, e.g., because the new primary fails.
                // In this case the replicas will start a further view change, with yet another primary.
                self.start_view_change(self.view + 1, mailbox);
            }
            // SAFETY: We skip view change messages for higher view numbers.
            // Messages from a higher view could be from a minority partition.
            Some(Message::Protocol(_, ProtocolMessage::StartViewChange(message)))
                if message.view == self.view =>
            {
                self.receive_start_view_change(message, mailbox);
            }
            // SAFETY: We skip view change messages for higher view numbers.
            // Messages from a higher view could be from a minority partition.
            Some(Message::Protocol(_, ProtocolMessage::DoViewChange(message)))
                if message.view == self.view && self.is_primary() =>
            {
                self.receive_do_view_change(message, mailbox);
            }
            // SAFETY: We skip view change messages for higher view numbers.
            // Messages from a higher view could be from a minority partition.
            Some(Message::Protocol(_, ProtocolMessage::StartView(message)))
                if message.view == self.view && !self.is_primary() =>
            {
                self.receive_start_view(message, mailbox);
            }
            Some(_) => {}
        }
    }

    /// When a replica receives STARTVIEWCHANGE messages for its view-number from f other replicas,
    /// it sends a DOVIEWCHANGE message to the node that will be the primary in the new view.
    fn receive_start_view_change(&mut self, message: StartViewChange, mailbox: &mut Mailbox) {
        self.view_change_votes.insert(message.index);
        if self.view_change_votes.len() >= self.configuration.threshold() {
            mailbox.send(
                self.primary(),
                DoViewChange {
                    view: self.view,
                    log: [],
                    last_normal_view: self.last_normal_view,
                    op_number: self.op_number,
                    commit: self.committed,
                    index: self.index,
                },
            );
        }
    }

    /// When the new primary receives f + 1 DOVIEWCHANGE messages from different replicas (including itself),
    /// it sets its view-number to that in the messages and selects as the new log the one contained in the message with the largest v';
    /// if several messages have the same v' it selects the one among them with the largest n.
    /// It sets its op-number to that of the topmost entry in the new log,
    /// sets its commit-number to the largest such number it received in the DOVIEWCHANGE messages,
    /// changes its status to normal,
    /// and informs the other replicas of the completion of the view change by sending STARTVIEW messages to the other replicas.
    fn receive_do_view_change(&mut self, message: DoViewChange, mailbox: &mut Mailbox) {
        self.view_change_state.insert(message.index, message);
        if self.view_change_state.len() > self.configuration.threshold() {
            if let Some(mut state) = self.view_change_state.get(&self.index) {
                let mut commit = state.commit;

                // find the log with the most recent information.
                for s in self.view_change_state.values() {
                    commit = commit.max(s.commit);

                    if (s.last_normal_view, s.op_number) > (state.last_normal_view, state.op_number)
                    {
                        state = s;
                    }
                }

                self.view = state.view;
                self.op_number = state.op_number;
                self.status_normal();
                self.broadcast(
                    mailbox,
                    StartView {
                        view: self.view,
                        log: [],
                        op_number: self.op_number,
                        // SAFETY: We use the message's commit-number since the replica's has not been updated yet.
                        // We do this in order to re-use the method from the normal protocol to execute committed operations.
                        commit,
                    },
                );

                // The new primary starts accepting client requests.
                // It also executes (in order) any committed operations that it hadn’t executed previously,
                // updates its client table,
                // and sends the replies to the clients.
                self.execute(commit, mailbox);
                self.client_table.remove_pending();

                let mut current = self.committed;
                while let Some(request) = self.log.get(current) {
                    self.client_table.start(request);
                    // start tracking prepared backups.
                    self.prepared.push_back(Default::default());

                    current += 1;
                }
            }
        }
    }

    /// When other replicas receive the STARTVIEW message,
    /// they replace their log with the one in the message,
    /// set their op-number to that of the latest entry in the log,
    /// set their view-number to the view number in the message,
    /// change their status to normal,
    /// and update the information in their client-table.
    /// If there are non-committed operations in the log,
    /// they send a PREPAREOK message to the primary; here n is the op-number.
    /// Then they execute all operations known to be committed that they haven’t executed previously,
    /// advance their commit-number,
    /// and update the information in their client-table.
    fn receive_start_view(&mut self, message: StartView, mailbox: &mut Mailbox) {
        // TODO: self.log = message.log;
        self.op_number = message.op_number;
        self.view = message.view;
        self.status_normal();

        // SAFETY: We have not updated the replica's commit-number to be the message's.
        // We do this in order to re-use the method from the normal protocol to execute committed operations.
        self.execute(message.commit, mailbox);
        self.client_table.remove_pending();

        let primary = self.primary();
        let mut current = self.committed;

        while let Some(request) = self.log.get(current) {
            self.client_table.start(request);

            // SAFETY: The op-number of the current operation is 1 more than its index into the log.
            current += 1;

            mailbox.send(
                primary,
                PrepareOk {
                    view: self.view,
                    op_number: current,
                    index: self.index,
                },
            );
        }
    }

    /// A replica j replies to a RECOVERY message only when its status is normal.
    /// In this case the replica sends a RECOVERYRESPONSE message to the recovering replica.
    /// If j is the primary of its view, l is its log, n is its op-number, and k is the commit-number;
    /// otherwise these values are nil.
    fn receive_recover(&mut self, message: Recover, mailbox: &mut Mailbox) {
        mailbox.send(
            message.index,
            RecoveryResponse {
                view: self.view,
                log: [], // TODO: only the primary includes its log.
                op_number: self.op_number,
                commit: self.committed,
                index: self.index,
                nonce: message.nonce,
            },
        );
    }

    fn recovery_receive(&mut self, message: Option<Message>, mailbox: &mut Mailbox) {
        match message {
            None => {
                // SAFETY: ensures recovering replicas can handle view changes and dropped messages
                self.broadcast(
                    mailbox,
                    Recover {
                        index: self.index,
                        nonce: self.nonce,
                    },
                );
            }
            Some(Message::Protocol(_, ProtocolMessage::RecoveryResponse(message)))
                if message.nonce == self.nonce =>
            {
                self.receive_recovery_response(message, mailbox)
            }
            Some(_) => {}
        }
    }

    /// The recovering replica waits to receive at least f +1 RECOVERYRESPONSE messages from different replicas,
    /// all containing the nonce it sent in its RECOVERY message,
    /// including one from the primary of the latest view it learns of in these messages.
    /// Then it updates its state using the information from the primary,
    /// changes its status to normal,
    /// and the recovery protocol is complete.
    fn receive_recovery_response(&mut self, message: RecoveryResponse, _: &mut Mailbox) {
        let mut view = message.view;

        self.recovery_responses.insert(message.index, message);
        if self.recovery_responses.len() > self.configuration.threshold() {
            for response in self.recovery_responses.values() {
                view = view.max(response.view);
            }

            let primary = view % self.configuration.len();
            if let Some(response) = self.recovery_responses.get(&primary) {
                self.view = response.view;
                // TODO: self.log = response.log;
                self.op_number = response.op_number;
                self.committed = response.commit;
                self.status_normal();
            }
        }
    }

    fn status_normal(&mut self) {
        self.status = Status::Normal;
        self.last_normal_view = self.view;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[derive(Debug, Default)]
    struct Echo;

    impl Service for Echo {
        fn invoke(&mut self, request: Bytes) -> Bytes {
            request
        }
    }

    #[test]
    fn single_request() {
        let configuration = Configuration::new([
            "127.0.0.1".parse().unwrap(),
            "127.0.0.2".parse().unwrap(),
            "127.0.0.3".parse().unwrap(),
        ]);
        let mut primary: Replica<Echo> = Replica::new(configuration.clone(), 0);
        let mut backup1: Replica<Echo> = Replica::new(configuration.clone(), 1);
        let mut mailbox = Mailbox::default();

        // pretend to receive a request over the network.
        mailbox.push(Request {
            operation: Bytes::from("test"),
            client: 1,
            id: 1,
        });
        primary.receive(&mut mailbox);

        // pretend to deliver the message over the network.
        let message = mailbox.pop().unwrap();
        mailbox.push(message);
        backup1.receive(&mut mailbox);

        // ignore the prepare message for backup2.
        mailbox.pop().unwrap();

        // pretend to deliver the message over the network.
        let message = mailbox.pop().unwrap();
        mailbox.push(message);
        primary.receive(&mut mailbox);

        assert_eq!(
            mailbox.pop(),
            Some(
                Reply {
                    view: 0,
                    result: Bytes::from("test"),
                    client: 1,
                    id: 1,
                }
                .into()
            )
        );
    }

    #[test]
    fn start_state_transfer_prepare() {
        let configuration = Configuration::new([
            "127.0.0.1".parse().unwrap(),
            "127.0.0.2".parse().unwrap(),
            "127.0.0.3".parse().unwrap(),
        ]);
        let mut backup: Replica<Echo> = Replica::new(configuration.clone(), 1);
        let mut mailbox = Mailbox::default();

        // pretend to receive a request over the network.
        mailbox.push(Message::Protocol(
            1,
            Prepare {
                view: 0,
                op_number: 2,
                commit: 0,
                request: Request {
                    operation: Bytes::from("test"),
                    client: 1,
                    id: 2,
                },
            }
            .into(),
        ));
        backup.receive(&mut mailbox);

        let Some(Message::Protocol(0 | 2, ProtocolMessage::GetState(message))) = mailbox.pop()
        else {
            panic!("invalid message type");
        };

        assert_eq!(
            message,
            GetState {
                view: 0,
                op_number: 0,
                index: 1,
            }
        );
    }
}
