//! A Primary Copy Method to Support Highly-Available Distributed Systems.

use std::collections::HashMap;
use std::net::SocketAddr;

mod network;
mod group;
mod order;

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub enum Status {
    #[default]
    Normal,
    ViewChange,
    Recovering
}

#[derive(Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct Request {
    /// The operation (with its arguments) the client wants to run).
    op: Vec<u8>,
    /// Client id
    c: u128,
    /// Client-assigned number for the request.
    s: u128,
    /// View number known to the client.
    v: usize,
}

#[derive(Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct Reply {
    /// View number.
    v: usize,
    /// The number the client provided in the request.
    s: u128,
    /// The result of the up-call to the service.
    x: Vec<u8>,
}

#[derive(Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct Replica {
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
    client_table: HashMap<u128, Reply>
}

pub enum Message {
    Prepare {
        /// The message received from the client.
        m: Request,
        /// The current view-number.
        v: usize,
        /// The op-number assigned to the request.
        n: usize
    },
    PrepareOk {
        /// The current view-number known to the replica.
        v: usize,
        /// The op-number assigned to the accepted prepare message.
        n: usize,
        /// The index of the replica accepting the prepare message.
        i: usize
    }
}

#[cfg(test)]
mod tests {
    use crate::group::{Cohort, Configuration, Group, GroupIdentifier, ModuleIdentifier};
    use crate::network::Network;
    use super::*;

    #[test]
    fn simulate() {
        let size = 3;
        let group = Group::new(Configuration::with_cohorts(size));
        let mut network = Network::default();
        let mut cohorts = Vec::with_capacity(size);

        for cohort in group.cohorts() {
            cohorts.push(Cohort::new(group.id(), cohort, network.bind(cohort).unwrap()));
        }
    }
}

