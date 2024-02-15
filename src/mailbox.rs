use crate::mail::{Either, Inbox, Mailbox, Outbox};
use crate::protocol::{Message, Protocol};
use crate::request::{ClientIdentifier, Reply, Request};
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use std::collections::VecDeque;
use std::future::Future;

type Payload<R, P> = Either<Request<R>, Protocol<R, P>>;

#[repr(transparent)]
pub struct Sender<R, P>(mpsc::Sender<Payload<R, P>>);

impl<R, P> Sender<R, P> {
    pub fn send_request(&mut self, request: Request<R>) -> Result<(), Payload<R, P>> {
        match self.0.try_send(Either::Left(request)) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into_inner()),
        }
    }

    pub fn send_protocol(
        &mut self,
        index: usize,
        protocol: Protocol<R, P>,
    ) -> Result<(), Payload<R, P>> {
        match self.0.try_send(Either::Right(protocol)) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into_inner()),
        }
    }
}

pub struct LocalMailbox<Req, Pre, Rep> {
    sender: mpsc::Sender<Payload<Req, Pre>>,
    receiver: mpsc::Receiver<Payload<Req, Pre>>,
    inbound_requests: VecDeque<Request<Req>>,
    inbound_messages: VecDeque<Protocol<Req, Pre>>,
    outbound_replies: VecDeque<(ClientIdentifier, Reply<Rep>)>,
    outbound_messages: VecDeque<(usize, Protocol<Req, Pre>)>,
}

impl<Req, Pre, Rep> Default for LocalMailbox<Req, Pre, Rep> {
    fn default() -> Self {
        let (sender, receiver) = mpsc::channel(0);

        Self {
            sender,
            receiver,
            inbound_requests: Default::default(),
            inbound_messages: Default::default(),
            outbound_replies: Default::default(),
            outbound_messages: Default::default(),
        }
    }
}

impl<Req, Pre, Rep> LocalMailbox<Req, Pre, Rep> {
    pub fn sender(&self) -> Sender<Req, Pre> {
        Sender(self.sender.clone())
    }
}

impl<Req, Pre, Rep> Inbox for LocalMailbox<Req, Pre, Rep> {
    type Request = Req;
    type Prediction = Pre;

    async fn receive(
        &mut self,
    ) -> Either<Request<Self::Request>, Protocol<Self::Request, Self::Prediction>> {
        self.receiver.next().await.unwrap()
    }

    async fn receive_response<'a, M, F>(&mut self, predicate: F) -> M
    where
        M: Message<'a>
            + TryFrom<
                Protocol<Self::Request, Self::Prediction>,
                Error = Protocol<Self::Request, Self::Prediction>,
            > + Into<Protocol<Self::Request, Self::Prediction>>,
        F: Fn(&M) -> bool,
    {
        loop {
            match self.receiver.next().await.unwrap() {
                Either::Left(request) => self.inbound_requests.push_back(request),
                Either::Right(protocol) => match M::try_from(protocol) {
                    Ok(message) if predicate(&message) => return message,
                    Ok(message) => self.inbound_messages.push_back(message.into()),
                    Err(protocol) => self.inbound_messages.push_back(protocol),
                },
            }
        }
    }
}

impl<Req, Pre, Rep> Outbox for LocalMailbox<Req, Pre, Rep> {
    type Reply = ();

    fn send<'a, M>(&mut self, index: usize, message: &M)
    where
        M: Message<'a>,
    {
        self.outbound_messages
            .push_back((index, message.clone().into()))
    }

    fn broadcast<'a, M>(&mut self, message: &M)
    where
        M: Message<'a>,
    {
        todo!()
    }

    fn reply(&mut self, client: ClientIdentifier, reply: &Reply<Self::Reply>) {
        todo!()
    }
}

impl<Req, Pre, Rep> Mailbox for LocalMailbox<Req, Pre, Rep> {}
