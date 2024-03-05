use log::{error, info, trace};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Debug, Formatter};
use std::io::stdin;
use std::num::NonZeroUsize;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};
use viewstamped_replication::buffer::{BufferedMailbox, ProtocolPayload};
use viewstamped_replication::{
    Client, ClientIdentifier, Configuration, Protocol, Replica, Reply, Request, Service,
};

#[derive(Copy, Clone, Debug)]
pub struct Options {
    f: usize,
    clients: usize,
    commit_timeout: Duration,
    view_timeout: Duration,
    reply_timeout: Duration,
    suffix: usize,
    checkpoint_threshold: usize,
    max_replies: usize,
}

#[derive(Default)]
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

pub enum Command<P>
where
    P: Protocol,
{
    Request(Request<P::Request>),
    Protocol(ProtocolPayload<P>),
    Checkpoint(usize),
    Crash,
    Recover,
}

impl<P, Req, Pre> Debug for Command<P>
where
    P: Protocol<Request = Req, Prediction = Pre>,
    Req: Debug,
    Pre: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request(request) => write!(f, "{request:?}"),
            Self::Protocol(message) => write!(f, "{message:?}"),
            Self::Checkpoint(suffix) => write!(f, "Checkpoint({suffix}"),
            Self::Crash => write!(f, "Kill"),
            Self::Recover => write!(f, "Recover"),
        }
    }
}

pub struct Network<P>
where
    P: Protocol,
{
    configuration: Configuration,
    senders: Vec<Sender<Command<P>>>,
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
    Req: Clone + Debug,
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

    pub fn bind(&mut self) -> Receiver<Command<P>> {
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
                .send(Command::Request(request))
                .expect("unable to send message");
        }
    }

    pub fn broadcast(&mut self, request: Request<P::Request>) {
        for sender in self.senders.iter() {
            sender
                .send(Command::Request(request.clone()))
                .expect("unable to send message");
        }
    }

    pub fn crash(&mut self, index: usize) {
        if let Some(sender) = self.senders.get(index) {
            sender.send(Command::Crash).expect("unable to send message");
        }
    }

    pub fn recover(&mut self, index: usize) {
        if let Some(sender) = self.senders.get(index) {
            sender
                .send(Command::Recover)
                .expect("unable to send message");
        }
    }

    pub fn checkpoint(&mut self, index: usize, suffix: usize) {
        if let Some(sender) = self.senders.get(index) {
            sender
                .send(Command::Checkpoint(suffix))
                .expect("unable to send message");
        }
    }

    pub fn requeue(&mut self, index: usize, inbox: &mut BufferedMailbox<P>) {
        if let Some(sender) = self.senders.get(index) {
            for message in inbox.drain_inbound() {
                trace!("Re-queuing {message:?} on replica {index}...");

                sender
                    .send(Command::Protocol(message))
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
                    .send(Command::Protocol(message.payload))
                    .expect("unable to send message");
            }
        }

        for message in outbox.drain_broadcast() {
            trace!("Broadcasting message {message:?} from {source} to the group...");

            for (index, sender) in self.senders.iter().enumerate() {
                if source != index {
                    sender
                        .send(Command::Protocol(message.clone()))
                        .expect("unable to send message");
                }
            }
        }
    }
}

fn main() {
    env_logger::init();

    let options = Options {
        f: 2,
        clients: 10,
        commit_timeout: Duration::from_millis(100),
        view_timeout: Duration::from_secs(1),
        reply_timeout: Duration::from_secs(1),
        suffix: 3,
        checkpoint_threshold: 1000,
        max_replies: 1000,
    };
    let configuration = Configuration::from(options.f * 2 + 1);

    let mut network = Network::<Adder>::new(configuration);
    let mut receivers = VecDeque::with_capacity(configuration.replicas());

    for _ in 0..configuration.replicas() {
        receivers.push_back(network.bind());
    }

    println!(
        "Running the simulation with {} replicas and {} clients.",
        configuration.replicas(),
        options.clients
    );

    let mut clients: Vec<(
        Client,
        Receiver<Reply<<Adder as Protocol>::Reply>>,
        Option<Instant>,
    )> = Vec::with_capacity(options.clients);
    for _ in 0..options.clients {
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
            let mut replica = Replica::new(configuration, index, Default::default());
            let mut checkpoint = replica.checkpoint(None);
            let mut crashed = false;

            loop {
                let timeout = if replica.is_primary() {
                    options.commit_timeout
                } else {
                    options.view_timeout
                };

                match receiver.recv_timeout(timeout) {
                    Ok(Command::Recover) if crashed => {
                        trace!("Recovering replica {}...", replica.index());

                        replica = Replica::recovering(
                            configuration,
                            index,
                            checkpoint.clone(),
                            &mut mailbox,
                        );
                        crashed = false;
                    }
                    Ok(_) if crashed => {}
                    Ok(Command::Recover) => {}
                    Ok(Command::Crash) => {
                        trace!("Crashing replica {}...", replica.index());
                        crashed = true;
                    }
                    Ok(Command::Checkpoint(suffix)) => {
                        trace!(
                            "Checkpointing replica {} with suffix {suffix}...",
                            replica.index()
                        );

                        checkpoint = replica.checkpoint(NonZeroUsize::new(suffix));

                        trace!(
                            "Checkpoint for replica {} includes up to op-number {:?}...",
                            replica.index(),
                            checkpoint.committed
                        );
                    }
                    Ok(Command::Request(request)) => {
                        trace!("Processing {request:?} on replica {index}...");

                        replica.handle_request(request, &mut mailbox);
                    }
                    Ok(Command::Protocol(message)) => {
                        interface.requeue(replica.index(), &mut mailbox);

                        trace!("Processing {message:?} on replica {index}...");

                        match message {
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
                        }
                    }
                    Err(_) => {
                        if !crashed {
                            trace!(
                                "Replica {} is idle in view {:?}...",
                                replica.index(),
                                replica.view()
                            );
                            replica.idle(&mut mailbox);
                        }
                    }
                }

                interface.process_outbound(replica.index(), &mut mailbox);
            }
        });
    }

    let mut interface = network.clone();
    thread::spawn(move || {
        let mut replies = HashSet::with_capacity(options.max_replies);
        let mut requests = HashSet::with_capacity(options.max_replies);

        loop {
            if !replies.is_empty() && replies.len() % options.checkpoint_threshold == 0 {
                for i in 0..configuration.replicas() {
                    interface.checkpoint(i, options.suffix);
                }
            }

            for (client, receiver, timestamp) in clients.iter_mut() {
                match timestamp.take() {
                    Some(start) => match receiver.try_recv() {
                        Ok(reply) => {
                            replies.insert(reply.id);
                            client.update_view(&reply);

                            info!(
                            "Client {:?} received reply #{} for request {:?} with view {:?} and payload {} after {} microseconds.",
                            client.identifier(), replies.len(), reply.id, reply.view, reply.payload, start.elapsed().as_micros()
                        );
                        }
                        Err(_) => {
                            let duration = start.elapsed();
                            if duration >= options.reply_timeout {
                                error!(
                                    "Timed-out waiting for reply on client {:?} after {}...",
                                    client.identifier(),
                                    duration.as_millis()
                                );
                                let request = client.last_request(1);
                                interface.broadcast(request);
                            }

                            *timestamp = Some(start);
                        }
                    },
                    None => {
                        if requests.len() < options.max_replies {
                            let request = client.new_request(1);
                            let primary = client.primary();

                            trace!("Sending request {request:?} to replica {primary}.");

                            requests.insert(request.id);
                            interface.send(primary, request);

                            *timestamp = Some(Instant::now());
                        }
                    }
                }
            }

            if replies.len() >= options.max_replies {
                println!(
                    "Received {} replies from the system. Exiting...",
                    replies.len()
                );
                std::process::exit(0);
            }
        }
    });

    let mut crashed = HashSet::new();
    let mut line = String::new();

    println!("Type commands: Q (Quit), C (Crash), R (Recover).");
    println!("For example, to crash the replica with index: C 1");

    loop {
        line.clear();

        stdin()
            .read_line(&mut line)
            .expect("unable to read from std-in");

        match line.trim().split_once(" ") {
            Some(("C" | "c", index)) => {
                if let Ok(index) = index.parse() {
                    crashed.insert(index);
                    network.crash(index);
                    println!("Crashed replicas: {crashed:?}.");
                }
            }
            Some(("R" | "r", index)) => {
                if let Ok(index) = index.parse() {
                    crashed.remove(&index);
                    network.recover(index);
                    println!("Crashed replicas: {crashed:?}.");
                }
            }
            None if line == "Q" || line == "q" => {
                std::process::exit(0);
            }
            _ => {}
        }
    }
}
