use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::io;
use std::sync::{Arc, mpsc, RwLock};
use std::sync::mpsc::TryRecvError;
use crate::group::{Event, Message, ModuleIdentifier};
use crate::order::ViewStamp;

#[derive(Clone, Debug, Default)]
pub struct Network {
    outbound: Arc<RwLock<HashMap<ModuleIdentifier, mpsc::Sender<Message>>>>,
    registry: Arc<RwLock<HashSet<ModuleIdentifier>>>
}

impl Network {
    pub fn bind(&mut self, interface: ModuleIdentifier) -> io::Result<CommunicationBuffer> {
        let mut guard = self.outbound.write().unwrap_or_else(|e| {
            let mut guard = e.into_inner();
            *guard = HashMap::new();
            guard
        });

        match guard.entry(interface) {
            Entry::Occupied(_) => {
                Err(io::Error::new(io::ErrorKind::AddrInUse, "module identifier is already in use"))
            }
            Entry::Vacant(entry) => {
                let (outbound, inbound) = mpsc::channel();
                let network = self.clone();

                entry.insert(outbound);

                Ok(CommunicationBuffer { last: ViewStamp::default(), buffer: BinaryHeap::new(), inbound, network })
            }
        }
    }

    fn connect(&self, to: ModuleIdentifier) -> io::Result<mpsc::Sender<Message>> {
        let guard = self.outbound.read().map_err(|_| io::Error::new(io::ErrorKind::ConnectionRefused, "unable to connect"))?;

        guard.get(&to).cloned().ok_or_else(|| io::Error::new(io::ErrorKind::AddrNotAvailable, "module identifier is not bound on this network"))
    }
}

#[derive(Debug)]
pub struct CommunicationBuffer {
    last: ViewStamp,
    buffer: BinaryHeap<Reverse<Message>>,
    inbound: mpsc::Receiver<Message>,
    network: Network,
}

impl CommunicationBuffer {
    pub fn add(&mut self, _event: Event) -> ViewStamp {
        ViewStamp::default()
    }

    pub fn force_to(&mut self, _view_stamp: ViewStamp) {}

    pub fn receive(&mut self) -> io::Result<Message> {
        self.inbound.try_recv().map_err(|e| match e {
            TryRecvError::Empty => io::Error::from(io::ErrorKind::WouldBlock),
            TryRecvError::Disconnected => io::Error::from(io::ErrorKind::ConnectionAborted),
        })
    }

    fn send(&mut self, to: ModuleIdentifier, message: Message) -> io::Result<()> {
        let outbound = self.network.connect(to)?;

        outbound.send(message).map_err(|_| io::Error::new(io::ErrorKind::ConnectionReset, "the receiving end is already closed"))
    }
}

#[cfg(test)]
mod tests {
    use crate::group::{CallIdentifier, Procedure};
    use crate::order::ViewIdentifier;
    use super::*;

    #[test]
    fn basic() {
        let mut network = Network::default();
        let a = ModuleIdentifier::default();
        let b = ModuleIdentifier::default();

        let mut a_buffer = network.bind(a).unwrap();
        let mut b_buffer = network.bind(b).unwrap();

        let message = Message::new(ViewIdentifier::from(1), CallIdentifier::from(0), Procedure::Abort);

        a_buffer.send(b, message.clone()).unwrap();

        assert_eq!(b_buffer.receive().unwrap(), message);
    }
}