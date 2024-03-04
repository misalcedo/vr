use log::{info, trace};
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::str::FromStr;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};
use viewstamped_replication::buffer::{BufferedMailbox, ProtocolPayload};
use viewstamped_replication::{
    Client, ClientIdentifier, Configuration, Protocol, Replica, Reply, Request, Service,
};

pub struct Adder(i32);

impl Protocol for Adder {
    type Request = i32;
    type Prediction = ();
    type Reply = i32;
    type Checkpoint = i32;
}

impl From<<Self as Protocol>::Checkpoint> for Adder {
    fn from(value: <Self as Protocol>::Checkpoint) -> Self {
        Adder(value)
    }
}

impl Service for Adder {
    fn predict(&self, _: &<Self as Protocol>::Request) -> <Self as Protocol>::Prediction {
        ()
    }

    fn checkpoint(&self) -> <Self as Protocol>::Checkpoint {
        self.0
    }

    fn invoke(
        &mut self,
        request: &<Self as Protocol>::Request,
        _: &<Self as Protocol>::Prediction,
    ) -> <Self as Protocol>::Reply {
        self.0 += *request;
        self.0
    }
}

pub enum Message<P>
where
    P: Protocol,
{
    Request(Request<P::Request>),
    Protocol(ProtocolPayload<P>),
}

impl<P, Req, Pre> Debug for Message<P>
where
    P: Protocol<Request = Req, Prediction = Pre>,
    Req: Debug,
    Pre: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Request(request) => write!(f, "{request:?}"),
            Message::Protocol(message) => write!(f, "{message:?}"),
        }
    }
}

pub struct Network<P>
where
    P: Protocol,
{
    configuration: Configuration,
    senders: Vec<Sender<Message<P>>>,
    clients: HashMap<ClientIdentifier, Sender<Reply<P::Reply>>>,
}

impl<P> Clone for Network<P>
where
    P: Protocol,
{
    fn clone(&self) -> Self {
        Self {
            configuration: self.configuration,
            senders: self.senders.clone(),
            clients: self.clients.clone(),
        }
    }
}

impl<P, Req, Pre, Rep> Network<P>
where
    P: Protocol<Request = Req, Prediction = Pre, Reply = Rep>,
    Req: Debug,
    Pre: Debug,
    Rep: Debug,
{
    pub fn new(configuration: Configuration) -> Self {
        let senders = Vec::with_capacity(configuration.replicas());

        Self {
            configuration,
            senders,
            clients: Default::default(),
        }
    }

    pub fn bind(&mut self) -> Receiver<Message<P>> {
        let (sender, receiver) = channel();

        self.senders.push(sender);

        receiver
    }

    pub fn bind_client(&mut self, identifier: ClientIdentifier) -> Receiver<Reply<P::Reply>> {
        let (sender, receiver) = channel();

        self.clients.insert(identifier, sender);

        receiver
    }

    pub fn send(&mut self, index: usize, request: Request<P::Request>) {
        if let Some(sender) = self.senders.get(index) {
            sender
                .send(Message::Request(request))
                .expect("unable to send message");
        }
    }

    pub fn requeue(&mut self, index: usize, inbox: &mut BufferedMailbox<P>) {
        if let Some(sender) = self.senders.get(index) {
            for message in inbox.drain_inbound() {
                trace!("Re-queuing {message:?} on replica {index}...");

                sender
                    .send(Message::Protocol(message))
                    .expect("unable to send message");
            }
        }
    }

    pub fn process_outbound(&mut self, source: usize, outbox: &mut BufferedMailbox<P>) {
        for message in outbox.drain_replies() {
            if let Some(sender) = self.clients.get(&message.destination) {
                trace!(
                    "Sending reply {:?} to client {:?} from replica {source}...",
                    &message.payload,
                    &message.destination
                );

                sender
                    .send(message.payload)
                    .expect("unable to send message");
            }
        }

        for message in outbox.drain_send() {
            if let Some(sender) = self.senders.get(message.destination) {
                trace!(
                    "Sending protocol message {:?} from {source} to {}...",
                    &message.payload,
                    &message.destination
                );

                sender
                    .send(Message::Protocol(message.payload))
                    .expect("unable to send message");
            }
        }

        for message in outbox.drain_broadcast() {
            trace!("Broadcasting message {message:?} from {source} to the group...");

            for (index, sender) in self.senders.iter().enumerate() {
                if source != index {
                    sender
                        .send(Message::Protocol(message.clone()))
                        .expect("unable to send message");
                }
            }
        }
    }
}

fn main() {
    env_logger::init();

    let configuration = Configuration::from(5);
    let checkpoint = 0;

    let mut network = Network::<Adder>::new(configuration);
    let mut receivers = VecDeque::with_capacity(configuration.replicas());

    for _ in 0..configuration.replicas() {
        receivers.push_back(network.bind());
    }

    let client_count: usize = std::env::args()
        .skip(1)
        .next()
        .as_ref()
        .map(String::as_str)
        .map(usize::from_str)
        .and_then(Result::ok)
        .unwrap_or(2);

    info!(
        "Running the simulation with {} replicas and {client_count} clients.",
        configuration.replicas()
    );

    let mut clients: Vec<(
        Client,
        Receiver<Reply<<Adder as Protocol>::Reply>>,
        Option<Instant>,
    )> = Vec::with_capacity(client_count);
    for _ in 0..client_count {
        let client = Client::new(configuration);
        let receiver = network.bind_client(client.identifier());

        clients.push((client, receiver, None));
    }

    for index in 0..configuration.replicas() {
        let receiver = receivers
            .pop_front()
            .expect("no receiver found for replica");
        let mut interface = network.clone();

        thread::spawn(move || {
            let mut mailbox = BufferedMailbox::default();
            let mut replica = Replica::new(configuration, index, Adder::from(checkpoint));

            loop {
                match receiver.recv_timeout(Duration::from_secs(3)) {
                    Ok(message) => {
                        interface.requeue(replica.index(), &mut mailbox);

                        trace!("Processing {message:?} on replica {index}...");

                        match message {
                            Message::Request(request) => {
                                replica.handle_request(request, &mut mailbox);
                            }
                            Message::Protocol(message) => match message {
                                ProtocolPayload::Prepare(message) => {
                                    replica.handle_prepare(message, &mut mailbox);
                                }
                                ProtocolPayload::PrepareOk(message) => {
                                    replica.handle_prepare_ok(message, &mut mailbox);
                                }
                                ProtocolPayload::Commit(message) => {
                                    replica.handle_commit(message, &mut mailbox);
                                }
                                ProtocolPayload::GetState(message) => {
                                    replica.handle_get_state(message, &mut mailbox);
                                }
                                ProtocolPayload::NewState(message) => {
                                    replica.handle_new_state(message, &mut mailbox);
                                }
                                ProtocolPayload::StartViewChange(message) => {
                                    replica.handle_start_view_change(message, &mut mailbox);
                                }
                                ProtocolPayload::DoViewChange(message) => {
                                    replica.handle_do_view_change(message, &mut mailbox);
                                }
                                ProtocolPayload::StartView(message) => {
                                    replica.handle_start_view(message, &mut mailbox);
                                }
                                ProtocolPayload::Recovery(message) => {
                                    replica.handle_recovery(message, &mut mailbox);
                                }
                                ProtocolPayload::RecoveryResponse(message) => {
                                    replica.handle_recovery_response(message, &mut mailbox);
                                }
                            },
                        }
                    }
                    Err(_) => {
                        trace!(
                            "Replica {} is idle in view {:?}...",
                            replica.index(),
                            replica.view()
                        );
                        replica.idle(&mut mailbox);
                    }
                }

                interface.process_outbound(replica.index(), &mut mailbox);
            }
        });
    }

    loop {
        for (client, receiver, timestamp) in clients.iter_mut() {
            match timestamp.take() {
                Some(start) => match receiver.try_recv() {
                    Ok(reply) => {
                        client.update_view(&reply);
                        info!(
                                "Received reply for request {:?} with view {:?} and payload {} after {} microseconds.",
                                reply.id, reply.view, reply.payload, start.elapsed().as_micros()
                            );
                    }
                    Err(_) => {
                        *timestamp = Some(start);
                    }
                },
                None => {
                    let request = client.new_request(1);
                    let primary = client.primary();

                    trace!("Sending request {request:?} to replica {primary}.");

                    network.send(primary, request);

                    *timestamp = Some(Instant::now());
                }
            }
        }
    }
}
