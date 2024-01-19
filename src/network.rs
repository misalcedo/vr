use crate::model::Message;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::mpsc::TryRecvError;
use std::sync::{mpsc, Arc, RwLock};

type StreamWriter = mpsc::Sender<(SocketAddr, Message)>;

#[derive(Clone, Debug, Default)]
pub struct Network {
    outbound: Arc<RwLock<HashMap<SocketAddr, StreamWriter>>>,
}

impl Network {
    pub fn bind(&mut self, address: SocketAddr) -> io::Result<CommunicationStream> {
        let mut guard = self.outbound.write().unwrap_or_else(|e| {
            let mut guard = e.into_inner();
            *guard = HashMap::new();
            guard
        });

        match guard.entry(address) {
            Entry::Occupied(_) => Err(io::Error::from(io::ErrorKind::AddrInUse)),
            Entry::Vacant(entry) => {
                let (outbound, inbound) = mpsc::channel();
                let network = self.clone();

                entry.insert(outbound);

                Ok(CommunicationStream {
                    address,
                    inbound,
                    network,
                })
            }
        }
    }

    pub fn connect(&self, to: SocketAddr) -> io::Result<mpsc::Sender<(SocketAddr, Message)>> {
        let guard = self
            .outbound
            .read()
            .map_err(|_| io::Error::from(io::ErrorKind::AddrNotAvailable))?;

        guard
            .get(&to)
            .cloned()
            .ok_or_else(|| io::Error::from(io::ErrorKind::AddrNotAvailable))
    }
}

#[derive(Debug)]
pub struct CommunicationStream {
    address: SocketAddr,
    inbound: mpsc::Receiver<(SocketAddr, Message)>,
    network: Network,
}

impl CommunicationStream {
    pub fn receive(&mut self) -> io::Result<(SocketAddr, Message)> {
        self.inbound.try_recv().map_err(|e| match e {
            TryRecvError::Empty => io::Error::from(io::ErrorKind::WouldBlock),
            TryRecvError::Disconnected => io::Error::from(io::ErrorKind::ConnectionAborted),
        })
    }

    pub fn send<M: Into<Message>>(&mut self, to: SocketAddr, message: M) -> io::Result<()> {
        let outbound = self.network.connect(to)?;

        outbound
            .send((self.address, message.into()))
            .map_err(|_| io::Error::from(io::ErrorKind::ConnectionReset))
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

        let mut a_stream = network.bind(a).unwrap();
        let mut b_stream = network.bind(b).unwrap();

        let message = Message::Prepare(Prepare {
            v: View::from(1),
            n: OpNumber::from(1),
            m: Request {
                op: b"Hello, World!".to_vec(),
                c: 1,
                s: 1,
                v: Default::default(),
            },
        });

        a_stream.send(b, message.clone()).unwrap();

        assert_eq!(b_stream.receive().unwrap(), (a, message));
    }
}
