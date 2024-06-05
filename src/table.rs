use crate::message::{Reply, Request};
use std::cmp::Ordering;
use std::collections::HashMap;

pub struct CachedRequest {
    request: u128,
    reply: Option<Reply>,
}

impl CachedRequest {
    fn new(request: &Request) -> Self {
        Self {
            request: request.id,
            reply: None,
        }
    }

    pub fn reply(&self) -> Option<&Reply> {
        self.reply.as_ref()
    }
}

pub struct ClientTable {
    cache: HashMap<u128, CachedRequest>,
}

impl Default for ClientTable {
    fn default() -> Self {
        Self {
            cache: Default::default(),
        }
    }
}

impl ClientTable {
    pub fn get(&self, client: &u128) -> Option<u128> {
        self.cache
            .get(client)
            .filter(|c| c.reply.is_none())
            .map(|c| c.request)
    }

    pub fn remove_pending(&mut self) {
        self.cache.retain(|_, v| v.reply.is_some());
    }

    pub fn compare(&self, request: &Request) -> Ordering {
        match self.cache.get(&request.client) {
            None => Ordering::Greater,
            Some(cached) => request.id.cmp(&cached.request),
        }
    }

    pub fn reply(&self, request: &Request) -> Option<&Reply> {
        self.cache
            .get(&request.client)
            .and_then(CachedRequest::reply)
    }

    pub fn start(&mut self, request: &Request) {
        self.cache
            .insert(request.client, CachedRequest::new(request));
    }

    pub fn finish(&mut self, request: &Request, reply: Reply) {
        let last_request = self
            .cache
            .entry(request.client)
            .or_insert_with(|| CachedRequest::new(request));

        last_request.reply = Some(reply);
    }
}
