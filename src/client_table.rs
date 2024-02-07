use crate::model::{ClientIdentifier, Reply, Request};
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Debug)]
pub struct LastRequest {
    request: Request,
    reply: Option<Reply>,
}

impl LastRequest {
    fn new(request: Request) -> Self {
        Self {
            request,
            reply: None,
        }
    }
}

#[derive(Debug)]
pub struct ClientTable {
    cache: HashMap<ClientIdentifier, LastRequest>,
}

impl Default for ClientTable {
    fn default() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }
}

impl ClientTable {
    pub fn get(&mut self, request: &Request) -> Option<Reply> {
        self.cache.get(&request.c)?.reply.as_ref().map(Reply::clone)
    }

    pub fn set(&mut self, request: &Request, reply: &Reply) {
        let last_request = self
            .cache
            .entry(request.c)
            .or_insert_with(|| LastRequest::new(request.clone()));

        last_request.reply = Some(reply.clone());
    }

    pub fn start(&mut self, request: &Request) {
        self.cache.insert(
            request.c,
            LastRequest {
                request: request.clone(),
                reply: None,
            },
        );
    }
}

impl PartialEq<Request> for ClientTable {
    fn eq(&self, other: &Request) -> bool {
        match self.cache.get(&other.c) {
            None => false,
            Some(last_request) => &last_request.request == other,
        }
    }
}

impl PartialOrd<Request> for ClientTable {
    fn partial_cmp(&self, other: &Request) -> Option<Ordering> {
        let last_request = self.cache.get(&other.c)?;

        if last_request.request.c == other.c {
            // ignore cached completed requests.
            last_request
                .request
                .s
                .partial_cmp(&other.s)
                .filter(|o| o != &Ordering::Less || last_request.reply.is_none())
        } else {
            None
        }
    }
}
