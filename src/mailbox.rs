use std::collections::VecDeque;
use crate::new_model::{Address, Envelope, Message};

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
    pub fn new(address: Address, group: Address) -> Self {
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
}

impl Mailbox {
    pub fn new(address: Address, group: Address) -> Self {
        Self {
            inbound: Default::default(),
            outbound: Sender::new(address, group)
        }
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
}