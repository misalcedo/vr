use crate::model::{
    Address, ClientIdentifier, GroupIdentifier, Message, Request, RequestIdentifier, View,
};

#[derive(Copy, Clone, Eq, PartialEq)]
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

    pub fn view(&self) -> View {
        self.view
    }

    pub fn last_request(&self) -> RequestIdentifier {
        self.request
    }

    pub fn new_message(&mut self, payload: &[u8]) -> Message {
        self.request.increment();
        self.message(payload)
    }

    pub fn new_request(&mut self, payload: &[u8]) -> Request {
        self.request.increment();
        self.request(payload)
    }

    fn message(&self, payload: &[u8]) -> Message {
        Message {
            from: self.identifier.into(),
            to: self.group.primary(self.view).into(),
            view: self.view,
            payload: self.request(payload).into(),
        }
    }

    pub fn request(&self, payload: &[u8]) -> Request {
        Request {
            op: Vec::from(payload),
            c: self.identifier,
            s: self.request,
        }
    }
}
