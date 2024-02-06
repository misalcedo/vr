use crate::new_model::{Address, Message, Payload, ReplicaIdentifier, View};
use std::collections::VecDeque;

#[derive(Debug)]
pub struct Mailbox {
    address: Address,
    group: Address,
    inbound: Vec<Option<Message>>,
    outbound: VecDeque<Message>,
}

impl From<ReplicaIdentifier> for Mailbox {
    fn from(value: ReplicaIdentifier) -> Self {
        Self::new(value.into(), value.group().into())
    }
}

impl Mailbox {
    pub fn new(address: Address, group: Address) -> Self {
        Self {
            address,
            group,
            inbound: Default::default(),
            outbound: Default::default(),
        }
    }

    pub fn deliver(&mut self, envelope: Message) {
        // Find an empty slot starting at the end
        for slot in self.inbound.iter_mut().rev() {
            if slot.is_none() {
                *slot = Some(envelope);
                return;
            }
        }

        self.inbound.push(Some(envelope));
    }

    pub fn select<F: FnMut(&mut Self, Message) -> Option<Message>>(&mut self, mut f: F) {
        for index in 0..self.inbound.len() {
            if let Some(envelope) = self.inbound.get_mut(index).and_then(Option::take) {
                self.inbound[index] = f(self, envelope);

                if self.inbound[index].is_none() {
                    break;
                }
            }
        }
    }

    pub fn send(&mut self, to: impl Into<Address>, view: View, payload: impl Into<Payload>) {
        let from = self.address;
        let to = to.into();
        let payload = payload.into();

        self.outbound.push_back(Message {
            from,
            to,
            view,
            payload,
        });
    }

    pub fn broadcast(&mut self, view: View, payload: impl Into<Payload>) {
        let from = self.address;
        let to = self.group;
        let payload = payload.into();

        self.outbound.push_back(Message {
            from,
            to,
            view,
            payload,
        });
    }

    pub fn drain_outbound(&mut self) -> impl Iterator<Item = Message> + '_ {
        self.outbound.drain(..)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::new_model::{ClientIdentifier, GroupIdentifier, Request, View};

    #[test]
    fn mailbox() {
        let group = GroupIdentifier::default();
        let client = ClientIdentifier::default();
        let client_address = Address::from(client);
        let replica = Address::from(group.replicas().next().unwrap());
        let view = View::default();
        let request = Request {
            op: vec![],
            c: client,
            s: Default::default(),
        };

        let mut instance = Mailbox::new(replica, Address::from(group));

        instance.inbound = vec![
            None,
            Some(Message {
                from: client_address,
                to: replica,
                view,
                payload: request.clone().into(),
            }),
            Some(Message {
                from: client_address,
                to: replica,
                view,
                payload: request.clone().into(),
            }),
        ];

        instance.select(|_, e| Some(e));
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
        let request = Request {
            op: vec![],
            c: client,
            s: Default::default(),
        };

        let mut instance = Mailbox::new(replica, Address::from(group));

        instance.inbound = vec![
            None,
            Some(Message {
                from: replica,
                to: replica,
                view,
                payload: request.clone().into(),
            }),
        ];

        instance.deliver(Message {
            from: client_address,
            to: replica,
            view,
            payload: request.clone().into(),
        });
        assert!(instance.inbound.iter().all(Option::is_some));
        assert_eq!(instance.inbound.len(), 2);

        instance.deliver(Message {
            from: client_address,
            to: replica,
            view,
            payload: request.clone().into(),
        });
        assert!(instance.inbound.iter().all(Option::is_some));
        assert_eq!(instance.inbound.len(), 3);
    }
}
