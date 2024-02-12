use crate::health::{HealthDetector, HealthStatus};
use crate::identifiers::ReplicaIdentifier;
use crate::mailbox::{Address, Mailbox};
use crate::model::{
    Commit, ConcurrentRequest, DoViewChange, Message, Payload, RecoveryResponse, StartView,
};
use crate::replica::{NonVolatileState, Replica, Status};
use crate::service::Service;
use crate::stamps::OpNumber;
use crate::state::State;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

pub trait Primary {
    fn process_normal(&mut self, mailbox: &mut Mailbox);

    fn process_view_change(&mut self, mailbox: &mut Mailbox);
}

impl<NS, S, HD> Primary for Replica<NS, S, HD>
where
    NS: State<NonVolatileState>,
    S: Service,
    HD: HealthDetector,
{
    fn process_normal(&mut self, mailbox: &mut Mailbox) {
        let mut prepared: HashMap<OpNumber, HashSet<ReplicaIdentifier>> = HashMap::new();
        let mut committed = self.committed;

        mailbox.visit(|message| {
            if let Message {
                from: Address::Replica(replica),
                payload: Payload::PrepareOk(prepare_ok),
                ..
            } = message
            {
                let replication = prepared.entry(prepare_ok.n).or_insert_with(HashSet::new);

                replication.insert(*replica);

                if replication.len() >= self.identifier.sub_majority() {
                    committed = committed.max(prepare_ok.n);
                }
            }
        });

        mailbox.select_all(|sender, message| match message {
            Message {
                from: Address::Client(_),
                payload: Payload::Request(request),
                ..
            } => {
                let cached_request = self.client_table.get(&request);

                match cached_request {
                    None => {
                        // this is the first request from the client.
                        self.client_table.start(&request);
                        self.prepare_primary(sender, request);
                    }
                    Some(last_request) => {
                        match last_request.partial_cmp(&request) {
                            None => {
                                // got a newer request from the client.
                                self.client_table.start(&request);
                                self.prepare_primary(sender, request);
                            }
                            Some(Ordering::Less) => sender.send(
                                request.c,
                                self.view,
                                ConcurrentRequest {
                                    s: last_request.request(),
                                },
                            ),
                            Some(Ordering::Equal) => match last_request.reply() {
                                None => {
                                    // the client resent the latest request.
                                    // we do not want to re-broadcast here to avoid the client being able to overwhelm the network.
                                }
                                // send back a cached response for latest request from the client.
                                Some(reply) => sender.send(request.c, self.view, reply),
                            },
                            Some(Ordering::Greater) => (), // drop older requests
                        }
                    }
                }

                None
            }
            Message {
                from: Address::Replica(_),
                payload: Payload::PrepareOk(prepare_ok),
                ..
            } if prepare_ok.n <= committed => None,
            Message {
                from: Address::Replica(_),
                payload: Payload::PrepareOk(_),
                ..
            } => Some(message),
            Message {
                from: Address::Replica(replica),
                payload: Payload::Recovery,
                ..
            } => {
                sender.send(
                    replica,
                    self.view,
                    RecoveryResponse {
                        l: self.log.clone(),
                        k: self.committed,
                    },
                );

                None
            }
            _ => None,
        });

        self.execute_committed(committed, Some(mailbox));

        if self.health_detector.detect(self.view, self.identifier) >= HealthStatus::Suspect {
            mailbox.broadcast(self.view, Commit { k: self.committed });
        }
    }

    fn process_view_change(&mut self, mailbox: &mut Mailbox) {
        let mut replicas = HashSet::new();

        mailbox.visit(|message| {
            if let Message {
                from: Address::Replica(replica),
                payload: Payload::DoViewChange(_),
                ..
            } = message
            {
                replicas.insert(*replica);
            }
        });

        let quorum = self.identifier.sub_majority() + 1;

        if replicas.len() < quorum {
            return;
        }

        let mut candidate = DoViewChange {
            l: Default::default(),
            k: Default::default(),
        };

        mailbox.select_all(|_, message| match message {
            Message {
                payload: Payload::DoViewChange(do_view_change),
                ..
            } => {
                candidate.k = candidate.k.max(do_view_change.k);

                if do_view_change.l.len() > candidate.l.len() {
                    candidate.l = do_view_change.l;
                }

                None
            }
            _ => Some(message),
        });

        self.replace_log(candidate.l);
        self.status = Status::Normal;
        self.save_non_volatile_state();

        mailbox.broadcast(
            self.view,
            StartView {
                l: self.log.clone(),
                k: candidate.k,
            },
        );

        self.execute_committed(candidate.k, Some(mailbox));

        for in_progress in self.committed.as_usize()..self.op_number.as_usize() {
            self.client_table.start(&self.log[in_progress])
        }
    }
}
