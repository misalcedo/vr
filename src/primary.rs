use crate::client_table::{CachedRequest, ClientTable};
use crate::configuration::Configuration;
use crate::log::{Entry, Log};
use crate::mail::Mailbox;
use crate::protocol::Prepare;
use crate::request::Request;
use crate::status::Status;
use crate::viewstamp::View;
use crate::Service;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

pub struct Primary<S>
where
    S: Service,
{
    configuration: Configuration,
    replica_number: usize,
    view: View,
    status: Status,
    log: Log<S::Request, S::Prediction>,
    committed: usize,
    client_table: ClientTable<S::Reply>,
    service: S,
}

impl<'a, S> Primary<S>
where
    S: Service,
    S::Request: Clone + Serialize + Deserialize<'a>,
    S::Prediction: Clone + Serialize + Deserialize<'a>,
    S::Reply: Serialize + Deserialize<'a>,
{
    pub fn invoke<M>(&mut self, request: Request<S::Request>, mailbox: &mut M)
    where
        M: Mailbox<Reply = S::Reply>,
    {
        let cached_request = self.client_table.get(&request.client);
        let comparison = cached_request
            .map(CachedRequest::request)
            .map(|id| request.id.cmp(&id))
            .unwrap_or(Ordering::Greater);

        match comparison {
            Ordering::Greater => {
                let prediction = self.service.predict(&request.payload);

                self.log.push(Entry::new(request, prediction));

                let entry = self.log.last();

                self.client_table.start(entry.request());
                mailbox.broadcast(&Prepare {
                    view: self.view,
                    op_number: self.log.op_number(),
                    request: entry.request().clone(),
                    prediction: entry.prediction().clone(),
                    committed: self.committed,
                });
            }
            Ordering::Equal => match cached_request.and_then(CachedRequest::reply) {
                Some(reply) => mailbox.reply(request.client, reply),
                _ => (),
            },
            _ => (),
        }
    }
}
