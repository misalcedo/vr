use crate::configuration::Configuration;
use crate::local::BufferedMailbox;
use crate::replica::Replica;
use crate::request::{ClientIdentifier, Reply, Request};
use crate::service::Service;
use std::collections::HashMap;

pub struct Driver<'a, S: Service<'a>> {
    configuration: Configuration,
    checkpoint: S::Checkpoint,
    replicas: Vec<Replica<'a, S>>,
    mailboxes: Vec<BufferedMailbox>,
    replies: HashMap<ClientIdentifier, Reply<S::Reply>>,
}

// TODO: update driver to be for a single replica.
impl<'a, S> Driver<'a, S>
where
    S: Service<'a>,
{
    pub fn new(configuration: Configuration, checkpoint: S::Checkpoint) -> Self {
        let mut replicas = Vec::with_capacity(configuration.replicas());
        let mut mailboxes = Vec::with_capacity(configuration.replicas());

        for index in 0..configuration.replicas() {
            replicas.push(Replica::new(
                configuration,
                index,
                checkpoint.clone().into(),
            ));
            mailboxes.push(Default::default());
        }

        Self {
            configuration,
            checkpoint,
            replicas,
            mailboxes,
            replies: Default::default(),
        }
    }

    pub fn send(&mut self, index: usize, request: Request<S::Request>) {
        if let Some(mailbox) = self.mailboxes.get_mut(index) {
            mailbox.deliver(request);
        }
    }

    pub fn broadcast(&mut self, request: Request<S::Request>) {
        for mailbox in self.mailboxes.iter_mut() {
            mailbox.deliver(request.clone());
        }
    }

    pub fn drive(&mut self, index: usize) {
        if let (Some(replica), Some(mailbox)) =
            (self.replicas.get_mut(index), self.mailboxes.get_mut(index))
        {
            todo!()
        }
    }

    pub fn step(&mut self) {
        for (replica, mailbox) in self.replicas.iter_mut().zip(self.mailboxes.iter_mut()) {
            todo!()
        }
    }

    pub fn step_loop(&mut self, max_iterations: usize) {}
}
