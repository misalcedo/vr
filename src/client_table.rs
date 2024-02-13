use crate::request::{ClientIdentifier, Reply, Request, RequestIdentifier};
use std::collections::HashMap;

pub struct CachedRequest<R> {
    request: RequestIdentifier,
    reply: Option<Reply<R>>,
}

impl<R> CachedRequest<R> {
    fn new<T>(request: &Request<T>) -> Self {
        Self {
            request: request.id,
            reply: None,
        }
    }

    pub fn request(&self) -> RequestIdentifier {
        self.request
    }

    pub fn reply(&self) -> Option<&Reply<R>> {
        self.reply.as_ref()
    }
}

#[derive(Default)]
pub struct ClientTable<R> {
    cache: HashMap<ClientIdentifier, CachedRequest<R>>,
}

impl<R> ClientTable<R> {
    pub fn get<T>(&mut self, client: ClientIdentifier) -> Option<&CachedRequest<R>> {
        self.cache.get(&client)
    }

    pub fn finish<T>(&mut self, request: &Request<T>, reply: Reply<R>) {
        let last_request = self
            .cache
            .entry(request.client)
            .or_insert_with(|| CachedRequest::new(&request));

        last_request.reply = Some(reply);
    }

    pub fn start<T>(&mut self, request: &Request<T>) {
        self.cache
            .insert(request.client, CachedRequest::new(request));
    }
}

impl<R> PartialEq<RequestIdentifier> for CachedRequest<R> {
    fn eq(&self, other: &RequestIdentifier) -> bool {
        self.request == *other
    }
}
