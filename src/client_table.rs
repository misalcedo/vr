use crate::identifiers::{ClientIdentifier, RequestIdentifier};
use crate::model::{Reply, Request};
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Debug)]
pub struct CachedRequest {
    request: Request,
    reply: Option<Reply>,
}

impl CachedRequest {
    fn new(request: Request) -> Self {
        Self {
            request,
            reply: None,
        }
    }

    pub fn request(&self) -> RequestIdentifier {
        self.request.s
    }

    pub fn reply(&self) -> Option<Reply> {
        self.reply.clone()
    }
}

#[derive(Debug)]
pub struct ClientTable {
    cache: HashMap<ClientIdentifier, CachedRequest>,
}

impl Default for ClientTable {
    fn default() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }
}

impl ClientTable {
    pub fn get(&mut self, request: &Request) -> Option<&CachedRequest> {
        self.cache.get(&request.c)
    }

    pub fn set(&mut self, request: &Request, reply: Reply) {
        let last_request = self
            .cache
            .entry(request.c)
            .or_insert_with(|| CachedRequest::new(request.clone()));

        last_request.reply = Some(reply);
    }

    pub fn start(&mut self, request: &Request) {
        self.cache.insert(
            request.c,
            CachedRequest {
                request: request.clone(),
                reply: None,
            },
        );
    }
}

impl PartialEq<Request> for CachedRequest {
    fn eq(&self, other: &Request) -> bool {
        &self.request == other
    }
}

impl PartialOrd<Request> for CachedRequest {
    fn partial_cmp(&self, other: &Request) -> Option<Ordering> {
        if self.request.c == other.c {
            // ignore cached completed requests.
            self.request
                .s
                .partial_cmp(&other.s)
                .filter(|o| o != &Ordering::Less || self.reply.is_none())
        } else {
            None
        }
    }
}
