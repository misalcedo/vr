use crate::configuration::Configuration;
use crate::request::{ClientIdentifier, Reply, Request, RequestIdentifier};
use crate::viewstamp::View;

pub struct Client {
    configuration: Configuration,
    view: View,
    identifier: ClientIdentifier,
    last_request: Option<RequestIdentifier>,
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

    pub fn update_view<P>(&mut self, reply: Reply<P>) {
        self.view = self.view.max(reply.view);
    }

    pub fn new_request<P>(&mut self, payload: P) -> Request<P> {
        let request = self
            .last_request
            .map(RequestIdentifier::next)
            .unwrap_or_default();

        self.last_request = Some(request);

        Request {
            payload,
            client: self.identifier,
            id: request,
        }
    }
}
