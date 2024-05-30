//! A Primary Copy Method to Support Highly-Available Distributed Systems.

use rand::Rng;
use std::net::IpAddr;

pub struct Mailbox;

impl Mailbox {
    fn send_get_state(&mut self, to: usize, message: &GetState) {}

    async fn receive_new_state(&mut self) -> Option<NewState> {
        None
    }
}

pub struct GetState {
    view: usize,
    op_number: usize,
    index: usize,
}

pub struct Request;

pub struct NewState {
    view: usize,
    log: Vec<Request>,
    op_number: usize,
    commit: usize,
}

pub struct Configuration {
    addresses: Vec<IpAddr>,
}

impl Configuration {
    pub fn len(&self) -> usize {
        self.addresses.len()
    }
}

/// State transfer is used by a node that has gotten behind (but hasn't crashed) to bring itself up-to-date.
/// The replica sends a message to one of the other replicas to learn about requests after a given op-number.
async fn transfer_state(
    configuration: Configuration,
    message: GetState,
    mailbox: &mut Mailbox,
) -> Result<NewState, GetState> {
    let mut to = message.index;
    while to == message.index {
        to = rand::thread_rng().gen_range(0..configuration.len());
    }

    mailbox.send_get_state(to, &message);
    mailbox.receive_new_state().await.ok_or(message)
}
