use std::collections::HashMap;

use crate::driver::Driver;
use crate::health::HealthDetector;
use crate::identifiers::{ClientIdentifier, GroupIdentifier, ReplicaIdentifier};
use crate::mailbox::{Address, Mailbox};
use crate::model::Message;
use crate::replica::{NonVolatileState, Replica};
use crate::service::Service;
use crate::state::LocalState;

#[derive(Debug)]
pub struct LocalDriver<S, H> {
    mailboxes: HashMap<Address, Mailbox>,
    replicas: HashMap<ReplicaIdentifier, Replica<LocalState<NonVolatileState>, S, H>>,
    states: HashMap<ReplicaIdentifier, LocalState<NonVolatileState>>,
}

impl<S: Service + Default, H: HealthDetector + Default> LocalDriver<S, H> {
    pub fn new(group: GroupIdentifier) -> Self {
        let mut mailboxes = HashMap::with_capacity(group.size());
        let mut replicas = HashMap::with_capacity(group.size());

        for replica in group {
            let state = LocalState::new(NonVolatileState::from(replica));

            mailboxes.insert(replica.into(), Mailbox::from(replica));
            replicas.insert(
                replica,
                Replica::new(state, Default::default(), Default::default()),
            );
        }

        Self {
            mailboxes,
            replicas,
            states: Default::default(),
        }
    }

    pub fn recover<I, II>(&mut self, replicas: II)
    where
        I: Iterator<Item = ReplicaIdentifier>,
        II: IntoIterator<Item = ReplicaIdentifier, IntoIter = I>,
    {
        for replica in replicas {
            let state = match self.states.remove(&replica) {
                None => LocalState::new(NonVolatileState::from(replica)),
                Some(s) => s,
            };

            self.mailboxes
                .entry(replica.into())
                .or_insert_with(|| Mailbox::from(replica));
            self.replicas
                .entry(replica)
                .or_insert_with(|| Replica::new(state, Default::default(), Default::default()));
        }
    }
}

impl<S: Service + Default, H: HealthDetector + Clone> LocalDriver<S, H> {
    pub fn with_health_detector(group: GroupIdentifier, detector: &H) -> Self {
        let mut mailboxes = HashMap::with_capacity(group.size());
        let mut replicas = HashMap::with_capacity(group.size());

        for replica in group {
            let state = LocalState::new(NonVolatileState::from(replica));

            mailboxes.insert(replica.into(), Mailbox::from(replica));
            replicas.insert(
                replica,
                Replica::new(state, Default::default(), detector.clone()),
            );
        }

        Self {
            mailboxes,
            replicas,
            states: Default::default(),
        }
    }

    pub fn recover_with_detector<I, II>(&mut self, replicas: II, detector: &H)
    where
        I: Iterator<Item = ReplicaIdentifier>,
        II: IntoIterator<Item = ReplicaIdentifier, IntoIter = I>,
    {
        for replica in replicas {
            let state = match self.states.remove(&replica) {
                None => LocalState::new(NonVolatileState::from(replica)),
                Some(s) => s,
            };

            self.mailboxes
                .entry(replica.into())
                .or_insert_with(|| Mailbox::from(replica));
            self.replicas
                .entry(replica)
                .or_insert_with(|| Replica::new(state, Default::default(), detector.clone()));
        }
    }
}

impl<S: Service, H: HealthDetector> LocalDriver<S, H> {
    pub fn take(
        mut self,
        identifier: ReplicaIdentifier,
    ) -> Result<(Replica<LocalState<NonVolatileState>, S, H>, Mailbox), Self> {
        match (
            self.replicas.remove(&identifier),
            self.mailboxes.remove(&identifier.into()),
        ) {
            (Some(replica), Some(mailbox)) => Ok((replica, mailbox)),
            _ => Err(self),
        }
    }

    fn poll(&mut self, identifier: ReplicaIdentifier) {
        let mut messages = Vec::new();

        if let (Some(replica), Some(mailbox)) = (
            self.replicas.get_mut(&identifier),
            self.mailboxes.get_mut(&identifier.into()),
        ) {
            replica.poll(mailbox);
            messages = mailbox.drain_outbound().collect();
        }

        for message in messages {
            self.route(message);
        }
    }

    fn route(&mut self, message: Message) {
        match message.to {
            Address::Replica(_) => {
                if let Some(mailbox) = self.mailboxes.get_mut(&message.to) {
                    mailbox.deliver(message);
                }
            }
            Address::Group(group) => {
                for replica in group {
                    // Don't send a broadcast back to the sender.
                    if message.from == replica.into() {
                        continue;
                    }

                    if let Some(mailbox) = self.mailboxes.get_mut(&replica.into()) {
                        mailbox.deliver(message.clone());
                    }
                }
            }
            Address::Client(client) => {
                let mailbox = self
                    .mailboxes
                    .entry(client.into())
                    .or_insert_with(|| Mailbox::from(client));
                mailbox.deliver(message);
            }
        }
    }

    pub fn is_empty(&self, identifier: ReplicaIdentifier) -> bool {
        !self.replicas.contains_key(&identifier)
            || self
                .mailboxes
                .get(&identifier.into())
                .map(Mailbox::is_empty)
                .unwrap_or_default()
    }

    pub fn crash<I, II>(&mut self, replicas: II)
    where
        I: Iterator<Item = ReplicaIdentifier>,
        II: IntoIterator<Item = ReplicaIdentifier, IntoIter = I>,
    {
        for replica in replicas {
            self.mailboxes.remove(&replica.into());

            if let Some(state) = self.replicas.remove(&replica).map(Replica::state) {
                self.states.insert(replica, state);
            }
        }
    }

    pub fn deliver(&mut self, message: Message) {
        self.route(message)
    }

    pub fn fetch(&mut self, client: ClientIdentifier) -> Vec<Message> {
        match self.mailboxes.get_mut(&client.into()) {
            None => Vec::new(),
            Some(mailbox) => mailbox.drain_inbound().collect(),
        }
    }
}

impl<S: Service, H: HealthDetector> Driver for LocalDriver<S, H> {
    fn drive<I, II>(&mut self, replicas: II)
    where
        I: Iterator<Item = ReplicaIdentifier>,
        II: IntoIterator<Item = ReplicaIdentifier, IntoIter = I>,
    {
        for replica in replicas {
            self.poll(replica)
        }
    }

    fn drive_to_empty<I, II>(&mut self, replicas: II)
    where
        I: Iterator<Item = ReplicaIdentifier> + Clone,
        II: IntoIterator<Item = ReplicaIdentifier, IntoIter = I>,
    {
        let iterator = replicas.into_iter();

        while iterator.clone().any(|r| !self.is_empty(r)) {
            self.drive(iterator.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::health::HealthStatus;
    use crate::model::{Commit, Payload};
    use crate::stamps::{OpNumber, View};
    use crate::state::State;

    use super::*;

    #[test]
    fn take() {
        let group = GroupIdentifier::new(3);
        let identifier = group.into_iter().last().unwrap();
        let view = View::default();
        let message = Message {
            from: group.primary(view).into(),
            to: identifier.into(),
            view,
            payload: Commit {
                k: OpNumber::default(),
            }
            .into(),
        };

        let mut driver: LocalDriver<usize, HealthStatus> = LocalDriver::new(group);

        driver.deliver(message.clone());

        let (replica, mut mailbox) = driver.take(identifier).unwrap();
        let messages: Vec<Message> = mailbox.drain_inbound().collect();

        assert_eq!(replica.identifier(), identifier);
        assert_eq!(messages, vec![message]);
    }

    #[test]
    fn take_crashed() {
        let group = GroupIdentifier::new(3);
        let mut driver: LocalDriver<usize, HealthStatus> = LocalDriver::new(group);
        let primary = group.primary(View::default());

        driver.crash(Some(primary));

        assert!(driver.take(primary).is_err());
    }

    #[test]
    fn crash_loses_messages() {
        let group = GroupIdentifier::new(3);
        let identifier = group.into_iter().last().unwrap();
        let view = View::default();
        let message = Message {
            from: group.primary(view).into(),
            to: identifier.into(),
            view,
            payload: Commit {
                k: OpNumber::default(),
            }
            .into(),
        };

        let mut driver: LocalDriver<usize, HealthStatus> = LocalDriver::new(group);

        driver.deliver(message.clone());
        driver.crash(Some(identifier));
        driver.deliver(message.clone());
        driver.recover(Some(identifier));

        let (replica, mut mailbox) = driver.take(identifier).unwrap();
        let messages: Vec<Message> = mailbox.drain_inbound().collect();

        assert_eq!(replica.identifier(), identifier);
        assert_eq!(messages, vec![]);
    }

    #[test]
    fn recover_maintains_state() {
        let group = GroupIdentifier::new(3);
        let identifier = group.into_iter().last().unwrap();
        let view = View::default();

        let mut driver: LocalDriver<usize, HealthStatus> = LocalDriver::new(group);

        driver.crash(Some(identifier));
        driver
            .states
            .entry(identifier)
            .and_modify(|s| s.save(NonVolatileState::new(identifier, view.next())));
        driver.recover(Some(identifier));

        let (replica, _) = driver.take(identifier).unwrap();

        assert_eq!(replica.identifier(), identifier);
        assert_eq!(replica.view(), view.next());
    }

    #[test]
    fn deliver_self() {
        let group = GroupIdentifier::new(3);
        let identifier = group.into_iter().last().unwrap();
        let view = View::default();
        let message = Message {
            from: identifier.into(),
            to: identifier.into(),
            view,
            payload: Commit {
                k: OpNumber::default(),
            }
            .into(),
        };

        let mut driver: LocalDriver<usize, HealthStatus> = LocalDriver::new(group);

        driver.deliver(message.clone());

        let (replica, mut mailbox) = driver.take(identifier).unwrap();
        let messages: Vec<Message> = mailbox.drain_inbound().collect();

        assert_eq!(replica.identifier(), identifier);
        assert_eq!(messages, vec![message]);
    }

    #[test]
    fn deliver_group_excludes_self() {
        let group = GroupIdentifier::new(3);
        let identifier = group.into_iter().last().unwrap();
        let view = View::default();
        let message = Message {
            from: identifier.into(),
            to: group.into(),
            view,
            payload: Commit {
                k: OpNumber::default(),
            }
            .into(),
        };

        let mut driver: LocalDriver<usize, HealthStatus> = LocalDriver::new(group);

        driver.deliver(message.clone());

        let (replica, mut mailbox) = driver.take(identifier).unwrap();
        let messages: Vec<Message> = mailbox.drain_inbound().collect();

        assert_eq!(replica.identifier(), identifier);
        assert_eq!(messages, vec![]);
    }

    #[test]
    fn empty() {
        let group = GroupIdentifier::new(3);
        let driver: LocalDriver<usize, HealthStatus> = LocalDriver::new(group);

        assert!(driver.is_empty(group.primary(View::default())))
    }

    #[test]
    fn not_empty() {
        let group = GroupIdentifier::new(3);
        let view = View::default();
        let identifier = group.replicas(view).last().unwrap();
        let message = Message {
            from: group.primary(view).into(),
            to: identifier.into(),
            view,
            payload: Commit {
                k: OpNumber::default(),
            }
            .into(),
        };
        let mut driver: LocalDriver<usize, HealthStatus> = LocalDriver::new(group);

        driver.deliver(message);

        assert!(!driver.is_empty(identifier))
    }
}
