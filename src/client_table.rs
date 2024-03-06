use crate::request::{ClientIdentifier, Reply, Request, RequestIdentifier};
use std::cmp::Ordering;
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

    pub fn reply(&self) -> Option<&Reply<R>> {
        self.reply.as_ref()
    }
}

pub struct ClientTable<R> {
    cache: HashMap<ClientIdentifier, CachedRequest<R>>,
}

impl<R> Default for ClientTable<R> {
    fn default() -> Self {
        Self {
            cache: Default::default(),
        }
    }
}

impl<R> ClientTable<R> {
    pub fn compare<T>(&self, request: &Request<T>) -> Ordering {
        self.cache
            .get(&request.client)
            .map(|cached| request.id.cmp(&cached.request))
            .unwrap_or(Ordering::Greater)
    }

    pub fn reply<T>(&self, request: &Request<T>) -> Option<&Reply<R>> {
        self.cache
            .get(&request.client)
            .and_then(CachedRequest::reply)
    }

    pub fn finish<T>(&mut self, request: &Request<T>, reply: Reply<R>) {
        let last_request = self
            .cache
            .entry(request.client)
            .or_insert_with(|| CachedRequest::new(request));

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::viewstamp::View;
    use crate::{Client, Configuration};

    #[test]
    fn requests() {
        let mut table = ClientTable::default();
        let mut client = Client::new(Configuration::from(3));
        let view = View::default();
        let oldest = client.new_request(1);
        let current = client.new_request(1);
        let newer = client.new_request(1);
        let reply = Reply {
            view,
            id: oldest.id,
            payload: (),
        };

        assert_eq!(table.compare(&oldest), Ordering::Greater);
        assert_eq!(table.reply(&oldest), None);

        table.start(&oldest);
        table.finish(&oldest, reply.clone());

        assert_eq!(table.compare(&current), Ordering::Greater);
        assert_eq!(table.reply(&oldest), Some(&reply));

        table.start(&current);

        assert_eq!(table.reply(&current), None);
        assert_eq!(table.compare(&oldest), Ordering::Less);
        assert_eq!(table.compare(&current), Ordering::Equal);
        assert_eq!(table.compare(&newer), Ordering::Greater);
    }
}
