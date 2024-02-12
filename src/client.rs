use crate::identifiers::{GroupIdentifier, ReplicaIdentifier};
use crate::mailbox::Address;
use crate::model::{Message, Request};
use crate::request::{ClientIdentifier, RequestIdentifier};
use crate::stamps::View;

pub struct Client {
    identifier: ClientIdentifier,
    view: View,
    request: RequestIdentifier,
    group: GroupIdentifier,
}

impl Client {
    pub fn new(group: GroupIdentifier) -> Self {
        Self {
            identifier: Default::default(),
            view: Default::default(),
            request: Default::default(),
            group,
        }
    }

    pub fn address(&self) -> Address {
        self.identifier.into()
    }

    pub fn identifier(&self) -> ClientIdentifier {
        self.identifier
    }

    pub fn primary(&self) -> ReplicaIdentifier {
        self.group.primary(self.view)
    }

    pub fn group(&self) -> GroupIdentifier {
        self.group
    }

    pub fn view(&self) -> View {
        self.view
    }

    pub fn last_request(&self) -> RequestIdentifier {
        self.request
    }

    pub fn new_message(&mut self, payload: &[u8]) -> Message {
        self.request.next();
        self.message(payload, None)
    }

    pub fn new_request(&mut self, payload: &[u8]) -> Request {
        self.request.next();
        self.request(payload, None)
    }

    pub fn message(&self, payload: &[u8], request: Option<RequestIdentifier>) -> Message {
        Message {
            from: self.identifier.into(),
            to: self.primary().into(),
            view: self.view,
            payload: self.request(payload, request).into(),
        }
    }

    pub fn broadcast(&self, payload: &[u8]) -> Message {
        Message {
            from: self.identifier.into(),
            to: self.group.into(),
            view: self.view,
            payload: self.request(payload, None).into(),
        }
    }

    pub fn request(&self, payload: &[u8], request: Option<RequestIdentifier>) -> Request {
        Request {
            op: Vec::from(payload),
            c: self.identifier,
            s: request.unwrap_or(self.request),
        }
    }

    pub fn set_view(&mut self, view: View) {
        self.view = view;
    }
}
