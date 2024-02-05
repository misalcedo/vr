use crate::model::{Address, Envelope, Envelope2, Inform, Message};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::mpsc::TryRecvError;
use std::sync::{mpsc, Arc, RwLock};
use crate::stamps::View;

type Stream = (mpsc::Sender<Envelope>, mpsc::Receiver<Envelope>);

#[derive(Debug)]
pub struct Mailbox {
    address: Address,
    group: Address,
    inbound: Vec<Option<Envelope2>>,
    outbound: VecDeque<Envelope2>
}

pub struct Sender<'a>(&'a mut VecDeque<Envelope2>);

impl Mailbox {
    pub fn new(address: Address, group: Address) -> Self {
        Self {
            address,
            group,
            inbound: Default::default(),
            outbound: Default::default()
        }
    }

    pub fn send(&mut self, view: View, to: Address, payload: impl Into<Message>) {
        let from = self.address;
        let message = payload.into();

        self.outbound.push_back(Envelope2 { view, from, to, message });
    }

    pub fn broadcast(&mut self, view: View, payload: impl Into<Message>) {
        let from = self.address;
        let to = self.group;
        let message = payload.into();

        self.outbound.push_back(Envelope2 { view, from, to, message });
    }

    pub fn inform(&mut self, view: View) {
        for slot in self.inbound.iter_mut() {
            match slot.take() {
                Some(envelope) if envelope.view < view => {
                    let from = self.address;
                    let to = envelope.to;
                    let message = Inform { v: view }.into();

                    self.outbound.push_back(Envelope2 { view, from, to, message } )
                },
                value => {
                    *slot = value;
                }
            }
        }
    }

    pub fn select<F: FnMut(&mut Sender, Envelope2) -> Option<Envelope2>>(&mut self, mut f: F) {
        let mut sender = Sender(&mut self.outbound);

         for slot in self.inbound.iter_mut() {
             if let Some(envelope) = slot.take() {
                 *slot = f(&mut sender, envelope);
             }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Network {
    channels: Arc<RwLock<HashMap<SocketAddr, Stream>>>,
}

impl Network {
    pub fn bind(&mut self, address: SocketAddr) -> io::Result<()> {
        let mut guard = self.channels.write().unwrap_or_else(|e| {
            let mut guard = e.into_inner();
            *guard = HashMap::new();
            guard
        });

        match guard.entry(address) {
            Entry::Occupied(_) => Err(io::Error::from(io::ErrorKind::AddrInUse)),
            Entry::Vacant(entry) => {
                entry.insert(mpsc::channel());

                Ok(())
            }
        }
    }

    pub fn receive(&mut self, interface: SocketAddr) -> io::Result<Envelope> {
        let guard = self
            .channels
            .read()
            .map_err(|_| io::Error::from(io::ErrorKind::AddrNotAvailable))?;
        let receiver = guard
            .get(&interface)
            .map(|(_, receiver)| receiver)
            .ok_or_else(|| io::Error::from(io::ErrorKind::AddrNotAvailable))?;

        receiver.try_recv().map_err(|e| match e {
            TryRecvError::Empty => io::Error::from(io::ErrorKind::WouldBlock),
            TryRecvError::Disconnected => io::Error::from(io::ErrorKind::ConnectionAborted),
        })
    }

    pub fn send(&mut self, envelope: Envelope) -> io::Result<()> {
        let guard = self
            .channels
            .read()
            .map_err(|_| io::Error::from(io::ErrorKind::AddrNotAvailable))?;

        let sender = guard
            .get(&envelope.to)
            .map(|(sender, _)| sender)
            .cloned()
            .ok_or_else(|| io::Error::from(io::ErrorKind::AddrNotAvailable))?;

        sender
            .send(envelope)
            .map_err(|_| io::Error::from(io::ErrorKind::ConnectionReset))
    }
}

/// Represents the communication mechanism between replicas.
/// Order and delivery to the recipient are not guaranteed.
///
/// If a message is undeliverable, the message is returned to sender on a receive call on the Network with the sender and recipient unchanged.
///
/// Implementations must provide the invariant that undeliverable messages are returned to sender.
///
/// The primary must re-send a prepare if there are X prepares waiting for a prepareOK to a single replica.
/// This ensures that replicas will either trigger a view change or limit the buffering.
/// To ensure replicas don't trigger view changes due to unreliable networks (high message drop rates or out of order deliveries),
/// the replicas must allow a larger number of buffered prepares than the primary does.
/// One way to ensure this is to define it as a multiplier on the outstanding prepare configuration.
///
/// TODO: implement an outbound with return-to-sender semantics.
// Need to determine what the primary will do in the case of return-to-sender.
pub trait Outbound {
    fn send(&mut self, envelope: Envelope);
}

impl Outbound for Network {
    fn send(&mut self, envelope: Envelope) {
        Self::send(self, envelope).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Inform, Prepare, Request};
    use crate::stamps::{OpNumber, View};

    #[test]
    fn mailbox() {
        let a = Address::Replica(1);
        let b = Address::Client(0);
        let view = View::default();
        let message = Inform { v: Default::default() }.into();

        let mut instance = Mailbox::new(Address::Replica(0), Address::Group(0));

        instance.inbound = vec![None, Some(Envelope2 { view, from: a, to: b, message })];

        instance.select(|s, e| Some(e));
        assert!(!instance.inbound.iter().all(Option::is_none));

        instance.select(|_, _| None);
        assert!(instance.inbound.iter().all(Option::is_none));
    }

    #[test]
    fn basic() {
        let mut network = Network::default();
        let a = "127.0.0.1:3001".parse().unwrap();
        let b = "127.0.0.1:3002".parse().unwrap();

        network.bind(a).unwrap();
        network.bind(b).unwrap();

        let message = Prepare {
            v: View::from(1),
            n: OpNumber::from(1),
            m: Request {
                op: b"Hello, World!".to_vec(),
                c: 1,
                s: 1,
                v: Default::default(),
            },
            c: Default::default()
        };
        let envelope = Envelope::new(View::default(), a, b, message);

        network.send(envelope.clone()).unwrap();

        assert_eq!(network.receive(b).unwrap(), envelope);
    }
}
