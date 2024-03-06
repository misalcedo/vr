use crate::configuration::Configuration;
use crate::request::{ClientIdentifier, Reply, Request, RequestIdentifier};
use crate::viewstamp::View;

pub struct Client {
    configuration: Configuration,
    view: View,
    identifier: ClientIdentifier,
    last_request: RequestIdentifier,
}

impl Client {
    pub fn new(configuration: Configuration) -> Self {
        Self {
            configuration,
            view: Default::default(),
            identifier: Default::default(),
            last_request: Default::default(),
        }
    }

    pub fn identifier(&self) -> ClientIdentifier {
        self.identifier
    }

    pub fn update_view<P>(&mut self, reply: &Reply<P>) {
        self.view = self.view.max(reply.view);
    }

    pub fn new_request<P>(&mut self, payload: P) -> Request<P> {
        self.last_request.increment();

        Request {
            payload,
            client: self.identifier,
            id: self.last_request,
        }
    }

    pub fn primary(&self) -> usize {
        self.configuration % self.view
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn requests() {
        let configuration = Configuration::from(5);
        let mut client = Client::new(configuration);

        let request_a = client.new_request(5);
        let request_b = client.new_request(5);

        assert_ne!(request_a.id, request_b.id);
        assert_eq!(request_a.id.cmp(&request_b.id), Ordering::Less);
    }
}
