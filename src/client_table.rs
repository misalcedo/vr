use crate::request::{ClientIdentifier, Reply, Request, RequestIdentifier};
use std::cmp::Ordering;
use std::collections::HashMap;

pub struct CachedRequest {
    request: RequestIdentifier,
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
    cache: HashMap<ClientIdentifier, CachedRequest>,
}

impl Default for ClientTable {
    fn default() -> Self {
        Self {
            cache: Default::default(),
        }
    }
}

impl ClientTable {
    pub fn compare(&self, request: &Request) -> Result<Ordering, RequestIdentifier> {
        match self.cache.get(&request.client) {
            None => Ok(Ordering::Greater),
            Some(cached) => match request.id.cmp(&cached.request) {
                Ordering::Greater if cached.reply.is_none() => Err(cached.request),
                ordering => Ok(ordering),
            },
        }
    }

    pub fn reply(&self, request: &Request) -> Option<&Reply> {
        self.cache
            .get(&request.client)
            .and_then(CachedRequest::reply)
    }

    pub fn finish(&mut self, request: &Request, reply: Reply) {
        let last_request = self
            .cache
            .entry(request.client)
            .or_insert_with(|| CachedRequest::new(request));

        last_request.reply = Some(reply);
    }

    pub fn start(&mut self, request: &Request) {
        self.cache
            .insert(request.client, CachedRequest::new(request));
    }
}

impl PartialEq<RequestIdentifier> for CachedRequest {
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
            payload: Default::default(),
        };

        assert_eq!(table.compare(&oldest), Ok(Ordering::Greater));
        assert_eq!(table.reply(&oldest), None);

        table.start(&oldest);
        table.finish(&oldest, reply.clone());

        assert_eq!(table.compare(&current), Ok(Ordering::Greater));
        assert_eq!(table.reply(&oldest), Some(&reply));

        table.start(&current);

        assert_eq!(table.reply(&current), None);
        assert_eq!(table.compare(&oldest), Ok(Ordering::Less));
        assert_eq!(table.compare(&current), Ok(Ordering::Equal));
        assert_eq!(table.compare(&newer), Err(current.id));
    }
}
