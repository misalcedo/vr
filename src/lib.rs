//! A Primary Copy Method to Support Highly-Available Distributed Systems.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;

mod model;
mod network;

pub use network::{Network, CommunicationStream};
use crate::model::{Message, Reply, Request};

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub enum Status {
    #[default]
    Normal,
    ViewChange,
    Recovering
}

#[derive(Debug)]
pub struct Replica {
    /// The service code for processing committed client requests.
    service: fn(Request) -> Vec<u8>,
    /// The interface for this replica to communicate with other replicas.
    communication: CommunicationStream,
    /// The configuration, i.e., the IP address and replica number for each of the 2f + 1 replicas.
    /// The replicas are numbered 0 to 2f.
    configuration: Vec<SocketAddr>,
    /// Each replica also knows its own replica number.
    index: usize,
    /// The current view-number, initially 0.
    view_number: usize,
    /// The current status, either normal, view-change, or recovering.
    status: Status,
    /// The op-number assigned to the most recently received request, initially 0.
    op_number: usize,
    /// This is an array containing op-number entries.
    /// The entries contain the requests that have been received so far in their assigned order.
    log: Vec<Request>,
    /// This records for each client the number of its most recent request,
    /// plus, if the request has been executed, the result sent for that request.
    client_table: HashMap<u128, Reply>,
}

impl Replica {
    pub fn new(service: fn(Request) -> Vec<u8>, communication: CommunicationStream, configuration: Vec<SocketAddr>, index: usize) -> Self {
        Self {
            service,
            communication,
            configuration,
            index,
            view_number: 0,
            status: Default::default(),
            op_number: 0,
            log: vec![],
            client_table: Default::default(),
        }
    }

    pub fn poll(&mut self) -> io::Result<()> {
        match self.communication.receive() {
            Ok(Message::Request(request)) => {
                Ok(())
            }
            Ok(Message::Prepare(message)) => {
                Ok(())
            }
            Ok(Message::PrepareOk(message)) => {
                Ok(())
            }
            Ok(Message::Reply(reply)) => {
                Ok(())
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(e) => Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::model::Message;
    use super::*;

    #[test]
    fn simulate() {
        let configuration = vec!["127.0.0.1:3001".parse().unwrap(), "127.0.0.1:3002".parse().unwrap(), "127.0.0.1:3003".parse().unwrap()];
        let mut network = Network::default();
        let mut replicas = Vec::with_capacity(configuration.len());

        for (index, address) in configuration.iter().enumerate() {
            let service = |request| { b"Bye, World!".to_vec() };
            replicas.push(Replica::new(service, network.bind(*address).unwrap(), configuration.clone(), index));
        }

        let mut client = network.bind("127.0.0.1:4001".parse().unwrap()).unwrap();

        let request = Request {
            op: b"Hello, World!".to_vec(),
            c: 1,
            s: 1,
            v: 0,
        };

        client.send(configuration[0], Message::Request(request.clone())).unwrap();

        replicas[0].poll().unwrap();
        replicas[1].poll().unwrap();
        replicas[2].poll().unwrap();
        replicas[0].poll().unwrap();
        replicas[0].poll().unwrap();

        assert_eq!(client.receive().unwrap(), Message::Reply(Reply {
            v: request.v,
            s: request.s,
            x: b"Bye, World!".to_vec(),
        }));
    }
}

