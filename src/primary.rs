use crate::client_table::{CachedRequest, ClientTable};
use crate::configuration::Configuration;
use crate::log::{Entry, Log};
use crate::mail::Outbox;
use crate::protocol::{Commit, Prepare, PrepareOk};
use crate::request::{Reply, Request};
use crate::role::Role;
use crate::status::Status;
use crate::viewstamp::View;
use crate::Service;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

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
    prepared: HashMap<usize, HashSet<usize>>,
}

impl<'a, S> Primary<S>
where
    S: Service,
    S::Request: Clone + Serialize + Deserialize<'a>,
    S::Prediction: Clone + Serialize + Deserialize<'a>,
    S::Reply: Serialize + Deserialize<'a>,
{
    fn prepare_request(
        &mut self,
        request: Request<S::Request>,
        outbox: &mut impl Outbox<Reply = S::Reply>,
    ) {
        let prediction = self.service.predict(&request.payload);

        self.log.push(Entry::new(request, prediction));

        let entry = self.log.last();

        self.client_table.start(entry.request());

        outbox.broadcast(&Prepare {
            view: self.view,
            op_number: self.log.op_number(),
            request: entry.request().clone(),
            prediction: entry.prediction().clone(),
            committed: self.committed,
        });
    }
}

impl<'a, S> Role<S> for Primary<S>
where
    S: Service,
    S::Request: Clone + Serialize + Deserialize<'a>,
    S::Prediction: Clone + Serialize + Deserialize<'a>,
    S::Reply: Serialize + Deserialize<'a>,
{
    fn request(
        &mut self,
        request: Request<S::Request>,
        outbox: &mut impl Outbox<Reply = S::Reply>,
    ) {
        let cached_request = self.client_table.get(&request.client);
        let comparison = cached_request
            .map(CachedRequest::request)
            .map(|id| request.id.cmp(&id))
            .unwrap_or(Ordering::Greater);

        match comparison {
            Ordering::Greater => self.prepare_request(request, outbox),
            Ordering::Equal => match cached_request.and_then(CachedRequest::reply) {
                Some(reply) => outbox.reply(request.client, reply),
                _ => (),
            },
            _ => (),
        }
    }

    fn prepare(
        &mut self,
        _: Prepare<S::Request, S::Prediction>,
        _: &mut impl Outbox<Reply = S::Reply>,
    ) {
    }

    fn prepare_ok(&mut self, prepare_ok: PrepareOk, outbox: &mut impl Outbox<Reply = S::Reply>) {
        let prepared = self.prepared.entry(prepare_ok.op_number).or_default();

        prepared.insert(prepare_ok.index);

        if self.configuration.sub_majority() <= prepared.len() {
            while self.committed < prepare_ok.op_number {
                let entry = &self.log[self.committed];
                let request = entry.request();
                let reply = Reply {
                    view: self.view,
                    id: request.id,
                    payload: self.service.invoke(&request.payload, entry.prediction()),
                };

                self.committed += 1;
                outbox.reply(request.client, &reply);
                self.client_table.finish(request, reply);
            }
        }
    }

    fn idle(&mut self, outbox: &mut impl Outbox<Reply = S::Reply>) {
        outbox.broadcast(&Commit {
            view: self.view,
            committed: self.committed,
        });
    }

    fn commit(&mut self, _: Commit, _: &mut impl Outbox<Reply = S::Reply>) {}
}
