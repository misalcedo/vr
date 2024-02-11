use crate::health::{HealthDetector, HealthStatus};
use crate::mailbox::{Address, Mailbox};
use crate::model::{Commit, ConcurrentRequest, Message, OutdatedRequest, Payload, StartView};
use crate::replica::{NonVolatileState, Replica, Role, Status};
use crate::service::Service;
use crate::stamps::OpNumber;
use crate::state::State;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

pub struct Primary<'a, NS, S, HD>(pub &'a mut Replica<NS, S, HD>);

impl<'a, NS, S, HD> Role for Primary<'a, NS, S, HD>
where
    NS: State<NonVolatileState>,
    S: Service,
    HD: HealthDetector,
{
    fn process_normal(&mut self, mailbox: &mut Mailbox) {
        let mut prepared: HashMap<OpNumber, HashSet<Address>> = HashMap::new();

        mailbox.select(|sender, message| match message {
            Message {
                payload: Payload::Request(request),
                ..
            } => {
                let cached_request = self.0.client_table.get(&request);

                match cached_request {
                    None => {
                        // this is the first request from the client.
                        self.0.client_table.start(&request);
                        self.0.prepare_primary(sender, request);
                    }
                    Some(last_request) => {
                        match last_request.partial_cmp(&request) {
                            None => {
                                // got a newer request from the client.
                                self.0.client_table.start(&request);
                                self.0.prepare_primary(sender, request);
                            }
                            Some(Ordering::Less) => sender.send(
                                request.c,
                                self.0.view,
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
                                Some(reply) => sender.send(request.c, self.0.view, reply),
                            },
                            Some(Ordering::Greater) => sender.send(
                                request.c,
                                self.0.view,
                                OutdatedRequest {
                                    s: last_request.request(),
                                },
                            ),
                        }
                    }
                }

                None
            }
            ref message @ Message {
                from,
                payload: Payload::PrepareOk(prepare_ok),
                ..
            } => {
                if self.0.committed >= prepare_ok.n {
                    None
                } else {
                    let replication = prepared.entry(prepare_ok.n).or_insert_with(HashSet::new);

                    replication.insert(from);

                    if replication.len() >= self.0.identifier.sub_majority() {
                        self.0.committed = self.0.committed.max(prepare_ok.n);
                        self.0.execute_primary(sender);

                        None
                    } else {
                        Some(message.clone())
                    }
                }
            }
            _ => Some(message),
        });

        if self
            .0
            .health_detector
            .detect(self.0.view, self.0.identifier)
            >= HealthStatus::Suspect
        {
            mailbox.broadcast(
                self.0.view,
                Commit {
                    k: self.0.committed,
                },
            );
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

        let quorum = self.0.identifier.sub_majority() + 1;

        if replicas.len() >= quorum {
            mailbox.select_all(|_, message| match message {
                Message {
                    payload: Payload::DoViewChange(do_view_change),
                    ..
                } => {
                    if do_view_change.l.len() > self.0.log.len() {
                        self.0.replace_log(do_view_change.k, do_view_change.l);
                    }

                    None
                }
                _ => Some(message),
            });

            self.0.status = Status::Normal;
            self.0
                .state
                .save(NonVolatileState::new(self.0.identifier, self.0.view));

            mailbox.broadcast(
                self.0.view,
                StartView {
                    l: self.0.log.clone(),
                    k: self.0.committed,
                },
            );

            self.0.execute_primary(mailbox);
        }
    }

    fn process_recovering(&mut self, _mailbox: &mut Mailbox) {
        todo!()
    }
}
