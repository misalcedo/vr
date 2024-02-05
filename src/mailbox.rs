use std::collections::VecDeque;
use crate::new_model::{Address, Envelope, Message, ReplicaIdentifier};

#[derive(Debug)]
pub struct Mailbox {
    inbound: Vec<Option<Envelope>>,
    outbound: Sender
}

#[derive(Debug)]
pub struct Sender {
    address: Address,
    group: Address,
    outbound: VecDeque<Envelope>,
}

impl Sender {
    fn new(address: Address, group: Address) -> Self {
        Self { address, group, outbound: Default::default() }
    }

    pub fn send(&mut self, to: Address, message: Message) {
        let from = self.address;

        self.outbound.push_back(Envelope { from, to, message });
    }

    pub fn broadcast(&mut self, message: Message) {
        let from = self.address;
        let to = self.group;

        self.outbound.push_back(Envelope { from, to, message });
    }

    pub fn drain(&mut self) -> impl Iterator<Item=Envelope> + '_ {
        self.outbound.drain(..)
    }
}

impl From<ReplicaIdentifier> for Mailbox {
    fn from(value: ReplicaIdentifier) -> Self {
        Self::new(value.into(), value.group().into())
    }
}

impl Mailbox {
    pub fn new(address: Address, group: Address) -> Self {
        Self {
            inbound: Default::default(),
            outbound: Sender::new(address.into(), group.into())
        }
    }

    pub fn deliver(&mut self, envelope: Envelope) {
        // Find an empty slot starting at the end
        for slot in self.inbound.iter_mut().rev() {
            if slot.is_none() {
                *slot = Some(envelope);
                return;
            }
        }

        self.inbound.push(Some(envelope));
    }

    pub fn select<F: FnMut(&mut Sender, Envelope) -> Option<Envelope>>(&mut self, mut f: F) {
        for slot in self.inbound.iter_mut() {
            if let Some(envelope) = slot.take() {
                *slot = f(&mut self.outbound, envelope);

                if slot.is_none() {
                    break;
                }
            }
        }
    }

    pub fn drain_outbound(&mut self) -> impl Iterator<Item=Envelope> + '_ {
        self.outbound.drain()
    }
}

#[cfg(test)]
mod tests {
    use crate::new_model::{ClientIdentifier, GroupIdentifier, Request, View};
    use super::*;


    #[test]
    fn mailbox() {
        let group = GroupIdentifier::default();
        let client = ClientIdentifier::default();
        let client_address = Address::from(client);
        let replica = Address::from(group.replicas().next().unwrap());
        let view = View::default();
        let request = Request { op: vec![], c: client, s: Default::default() };
        let message: Message = Message::new(view, request);

        let mut instance = Mailbox::new(replica, Address::from(group));

        instance.inbound = vec![
            None,
            Some(Envelope { from: client_address, to: replica, message: message.clone() }),
            Some(Envelope { from: client_address, to: replica, message }),
        ];

        instance.select(|s, e| Some(e));
        assert!(!instance.inbound.iter().all(Option::is_none));

        instance.select(|_, _| None);
        assert!(!instance.inbound.iter().all(Option::is_none));

        instance.select(|_, _| None);
        assert!(instance.inbound.iter().all(Option::is_none));
    }

    #[test]
    fn deliver() {
        let group = GroupIdentifier::default();
        let client = ClientIdentifier::default();
        let client_address = Address::from(client);
        let replica = Address::from(group.replicas().next().unwrap());
        let view = View::default();
        let request = Request { op: vec![], c: client, s: Default::default() };
        let message: Message = Message::new(view, request);

        let mut instance = Mailbox::new(replica, Address::from(group));

        instance.inbound = vec![
            None,
            Some(Envelope { from: replica, to: replica, message: message.clone() }),
        ];

        instance.deliver(Envelope { from: client_address, to: replica, message: message.clone() });
        assert!(instance.inbound.iter().all(Option::is_some));
        assert_eq!(instance.inbound.len(), 2);

        instance.deliver(Envelope { from: client_address, to: replica, message: message.clone() });
        assert!(instance.inbound.iter().all(Option::is_some));
        assert_eq!(instance.inbound.len(), 3);
    }
}