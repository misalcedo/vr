use std::cmp::Ordering;
use std::collections::HashMap;
use crate::model::{Reply, Request};

#[derive(Debug)]
pub struct LastRequest {
    request: Request,
    reply: Option<Reply>
}

#[derive(Debug)]
pub struct RequestCache {
    cache: HashMap<u128, LastRequest>
}

impl Default for RequestCache {
    fn default() -> Self {
        Self {
            cache: HashMap::new()
        }
    }
}

impl RequestCache {
    pub fn get(&mut self, request: &Request) -> Option<Reply> {
        self.cache.get(&request.c)?.reply.as_ref().map(Reply::clone)
    }

    pub fn set(&mut self, request: &Request, reply: &Reply) {
        self.cache.entry(request.c).and_modify(|l| l.reply = Some(reply.clone()));
    }

    pub fn start(&mut self, request: &Request) {
        self.cache.insert(request.c, LastRequest { request: request.clone(), reply: None });
    }
}

impl PartialEq<Request> for RequestCache {
    fn eq(&self, other: &Request) -> bool {
        match self.cache.get(&other.c) {
            None => false,
            Some(last_request) => &last_request.request == other
        }
    }
}

impl PartialOrd<Request> for RequestCache {
    fn partial_cmp(&self, other: &Request) -> Option<Ordering> {
        let last_request = self.cache.get(&other.c)?;

        // ignore cached completed requests.
        last_request.request.partial_cmp(other).filter(|o| o != &Ordering::Less || last_request.reply.is_none())
    }
}