use crate::model::Envelope;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::mpsc::TryRecvError;
use std::sync::{mpsc, Arc, RwLock};

type Stream = (mpsc::Sender<Envelope>, mpsc::Receiver<Envelope>);

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
    use crate::model::{Prepare, Request};
    use crate::stamps::{OpNumber, View};

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
        let envelope = Envelope::new(a, b, message);

        network.send(envelope.clone()).unwrap();

        assert_eq!(network.receive(b).unwrap(), envelope);
    }
}
