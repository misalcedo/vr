use crate::driver::Driver;
use crate::health::HealthDetector;
use crate::identifiers::{ClientIdentifier, GroupIdentifier, ReplicaIdentifier};
use crate::mailbox::{Address, Mailbox};
use crate::model::Message;
use crate::replica::Replica;
use crate::service::Service;
use std::collections::HashMap;

pub struct BasicDriver<S, H> {
    mailboxes: HashMap<Address, Mailbox>,
    replicas: HashMap<ReplicaIdentifier, Replica<S, H>>,
}

impl<S: Service + Default, H: HealthDetector + Default> BasicDriver<S, H> {
    pub fn new(group: GroupIdentifier) -> Self {
        let mut mailboxes = HashMap::with_capacity(group.size());
        let mut replicas = HashMap::with_capacity(group.size());

        for replica in group {
            mailboxes.insert(replica.into(), Mailbox::from(replica));
            replicas.insert(
                replica,
                Replica::new(replica, Default::default(), Default::default()),
            );
        }

        Self {
            mailboxes,
            replicas,
        }
    }
}

impl<S: Service, H: HealthDetector> BasicDriver<S, H> {
    pub fn poll(&mut self, identifier: ReplicaIdentifier) {
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
}

impl<S: Service + Default, H: HealthDetector + Default> Driver for BasicDriver<S, H> {
    fn drive<I, II>(&mut self, replicas: II)
    where
        I: Iterator<Item = ReplicaIdentifier>,
        II: IntoIterator<Item = ReplicaIdentifier, IntoIter = I>,
    {
        for replica in replicas {
            self.poll(replica)
        }
    }

    fn crash<I, II>(&mut self, replicas: II)
    where
        I: Iterator<Item = ReplicaIdentifier>,
        II: IntoIterator<Item = ReplicaIdentifier, IntoIter = I>,
    {
        for replica in replicas {
            self.mailboxes.remove(&replica.into());
            self.replicas.remove(&replica);
        }
    }

    fn recover<I, II>(&mut self, replicas: II)
    where
        I: Iterator<Item = ReplicaIdentifier>,
        II: IntoIterator<Item = ReplicaIdentifier, IntoIter = I>,
    {
        for replica in replicas {
            self.mailboxes
                .entry(replica.into())
                .or_insert_with(|| Mailbox::from(replica));
            self.replicas
                .entry(replica)
                .or_insert_with(|| Replica::new(replica, Default::default(), Default::default()));
        }
    }

    fn deliver(&mut self, message: Message) {
        self.route(message)
    }

    fn fetch(&mut self, client: ClientIdentifier) -> Vec<Message> {
        match self.mailboxes.get_mut(&client.into()) {
            None => Vec::new(),
            Some(mailbox) => mailbox.drain_inbound().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Client;
    use crate::health::HealthStatus;
    use crate::model::{ConcurrentRequest, Reply};

    #[test]
    fn simple() {
        let group = GroupIdentifier::new(3);
        let mut driver: BasicDriver<usize, HealthStatus> = BasicDriver::new(group);
        let mut client = Client::new(group);

        let operation = b"Hello, world!";
        let request = client.new_message(operation);
        let primary = group.primary(client.view());

        driver.deliver(request);
        driver.drive(Some(primary));
        driver.drive(group.replicas(client.view()));
        driver.drive(Some(primary));

        let messages = driver.fetch(client.identifier());
        let reply = Message {
            from: primary.into(),
            to: client.address(),
            view: client.view(),
            payload: Reply {
                x: operation.len().to_be_bytes().to_vec(),
                s: client.last_request(),
            }
            .into(),
        };

        assert_eq!(messages, vec![reply]);
    }

    #[test]
    fn concurrent_requests() {
        let group = GroupIdentifier::new(3);
        let mut driver: BasicDriver<usize, HealthStatus> = BasicDriver::new(group);
        let mut client = Client::new(group);

        let operation = b"Hello, world!";
        let primary = group.primary(client.view());
        let old_request = client.new_message(operation);
        let last_request = client.last_request();
        let new_request = client.new_message(operation);

        driver.deliver(old_request);
        driver.deliver(new_request);
        driver.drive(Some(primary));
        driver.drive(Some(primary));

        let messages = driver.fetch(client.identifier());
        let response = Message {
            from: primary.into(),
            to: client.address(),
            view: client.view(),
            payload: ConcurrentRequest { s: last_request }.into(),
        };

        assert_eq!(messages, vec![response]);
    }
}
