use crate::new_model::{Address, Envelope, Message, ReplicaIdentifier};
use std::collections::VecDeque;

#[derive(Debug)]
pub struct Mailbox {
    address: Address,
    group: Address,
    inbound: Vec<Option<Envelope>>,
    outbound: VecDeque<Envelope>,
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

    pub fn select<F: FnMut(&mut Self, Envelope) -> Option<Envelope>>(&mut self, mut f: F) {
        for index in 0..self.inbound.len() {
            if let Some(envelope) = self.inbound[index].take() {
                self.inbound[index] = f(self, envelope);

                if self.inbound[index].is_none() {
                    break;
                }
            }
        }
    }

    pub fn test<A, F: Fn(&Envelope, &mut A)>(&self, initial: A, f: F) -> A {
        let mut accumulator = initial;

        for slot in self.inbound.iter() {
            if let Some(envelope) = slot {
                f(envelope, &mut accumulator);
            }
        }

        accumulator
    }

    pub fn send(&mut self, to: impl Into<Address>, message: Message) {
        let from = self.address;
        let to = to.into();

        self.outbound.push_back(Envelope { from, to, message });
    }

    pub fn broadcast(&mut self, message: Message) {
        let from = self.address;
        let to = self.group;

        self.outbound.push_back(Envelope { from, to, message });
    }

    pub fn drain_outbound(&mut self) -> impl Iterator<Item = Envelope> + '_ {
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
        let message: Message = Message::new(view, request);

        let mut instance = Mailbox::new(replica, Address::from(group));

        instance.inbound = vec![
            None,
            Some(Envelope {
                from: client_address,
                to: replica,
                message: message.clone(),
            }),
            Some(Envelope {
                from: client_address,
                to: replica,
                message,
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
        let message: Message = Message::new(view, request);

        let mut instance = Mailbox::new(replica, Address::from(group));

        instance.inbound = vec![
            None,
            Some(Envelope {
                from: replica,
                to: replica,
                message: message.clone(),
            }),
        ];

        instance.deliver(Envelope {
            from: client_address,
            to: replica,
            message: message.clone(),
        });
        assert!(instance.inbound.iter().all(Option::is_some));
        assert_eq!(instance.inbound.len(), 2);

        instance.deliver(Envelope {
            from: client_address,
            to: replica,
            message: message.clone(),
        });
        assert!(instance.inbound.iter().all(Option::is_some));
        assert_eq!(instance.inbound.len(), 3);
    }

    #[test]
    fn test_predicate() {
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
        let message: Message = Message::new(view, request);

        let mut instance = Mailbox::new(replica, Address::from(group));

        instance.inbound = vec![
            None,
            Some(Envelope {
                from: replica,
                to: replica,
                message: message.clone(),
            }),
            Some(Envelope {
                from: replica,
                to: replica,
                message: message.clone(),
            }),
            Some(Envelope {
                from: client_address,
                to: replica,
                message: message.clone(),
            }),
        ];

        let result = instance.test(0, |e, c| {
            if e.from == replica {
                *c += 1
            }
        });

        assert_eq!(result, 2);
    }
}
