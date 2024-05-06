use clap::{command, Parser};
use log::{info, trace, warn};
use rand::{thread_rng, Rng};
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use tokio::task::JoinSet;
use viewstamped_replication::buffer::{BufferedMailbox, ProtocolPayload};
use viewstamped_replication::{
    Client, ClientIdentifier, Configuration, DataService, Replica, Reply, Request, Service,
};

#[derive(Copy, Clone, Debug, Parser)]
#[command(author, version, about, long_about)]
pub struct Options {
    /// The supported number of failures for this configuration.
    #[arg(short, long, default_value_t = 2)]
    f: usize,
    /// Total number of concurrent clients.
    #[arg(short, long, default_value_t = 1000)]
    clients: usize,
    #[arg(long, default_value_t = 50)]
    /// Timeout in milliseconds for the primary considering itself idle.
    commit_timeout: u64,
    /// Timeout in milliseconds for backups considering themselves idle.
    #[arg(long, default_value_t = 500)]
    view_timeout: u64,
    /// Timeout in milliseconds for clients to broadcast their request.
    #[arg(long, default_value_t = 1000)]
    reply_timeout: u64,
    /// Interval in milliseconds to print progress of processed requests.
    #[arg(long, default_value_t = 1000)]
    progress_internal: u64,
    /// Number of operations to maintain in the log.
    #[arg(short, long, default_value_t = 100)]
    suffix: usize,
    /// Total number of requests each client will make.
    #[arg(short, long, default_value_t = 1000)]
    requests_per_client: usize,
    /// Total number of requests each client will make.
    #[arg(short, long, default_value_t = 0.00)]
    network_drop_rate: f64,
}

#[derive(Default)]
pub struct Adder(i32);

impl DataService for Adder {
    type Request = i32;
    type Prediction = ();
    type Checkpoint = i32;
    type Reply = i32;

    fn restore(checkpoint: Self::Checkpoint) -> Self {
        Self(checkpoint)
    }

    fn predict(&self, _: Self::Request) -> Self::Prediction {
        ()
    }

    fn checkpoint(&self) -> Self::Checkpoint {
        self.0
    }

    fn invoke(&mut self, request: Self::Request, _: Self::Prediction) -> Self::Reply {
        self.0 += *request;
        self.0
    }
}

pub enum Command {
    Request(Request),
    Protocol(ProtocolPayload),
    Crash,
    Recover,
}

impl Debug for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request(request) => write!(f, "{request:?}"),
            Self::Protocol(message) => write!(f, "{message:?}"),
            Self::Crash => write!(f, "Kill"),
            Self::Recover => write!(f, "Recover"),
        }
    }
}

pub struct Network {
    configuration: Configuration,
    options: Options,
    senders: Vec<UnboundedSender<Command>>,
    clients: HashMap<ClientIdentifier, Sender<Reply>>,
}

impl Clone for Network {
    fn clone(&self) -> Self {
        Self {
            configuration: self.configuration,
            options: self.options,
            senders: self.senders.clone(),
            clients: self.clients.clone(),
        }
    }
}

impl Network {
    pub fn new(configuration: Configuration, options: Options) -> Self {
        let senders = Vec::with_capacity(configuration.replicas());

        Self {
            configuration,
            options,
            senders,
            clients: Default::default(),
        }
    }

    pub fn bind(&mut self) -> UnboundedReceiver<Command> {
        let (sender, receiver) = unbounded_channel();

        self.senders.push(sender);

        receiver
    }

    pub fn bind_client(&mut self, identifier: ClientIdentifier) -> Receiver<Reply> {
        let (sender, receiver) = channel(1);

        self.clients.insert(identifier, sender);

        receiver
    }

    pub async fn send(&mut self, index: usize, request: Request) {
        if self.should_drop() {
            return;
        }

        if let Some(sender) = self.senders.get(index) {
            if let Err(_) = sender.send(Command::Request(request.clone())) {
                warn!("unable to send message to {index}")
            }
        }
    }

    pub async fn broadcast(&mut self, request: Request) {
        if self.should_drop() {
            return;
        }

        for (index, sender) in self.senders.iter().enumerate() {
            if let Err(_) = sender.send(Command::Request(request.clone())) {
                warn!("unable to send message to {index}")
            }
        }
    }

    pub async fn crash(&mut self, index: usize) {
        self.send_to(index, Command::Crash);
    }

    fn send_to(&mut self, index: usize, command: Command) {
        if let Some(sender) = self.senders.get(index) {
            if let Err(_) = sender.send(command) {
                warn!("unable to send message to {index}")
            }
        }
    }

    pub async fn recover(&mut self, index: usize) {
        self.send_to(index, Command::Recover);
    }

    pub async fn requeue(&mut self, index: usize, inbox: &mut BufferedMailbox) {
        if let Some(sender) = self.senders.get(index) {
            for message in inbox.drain_inbound() {
                trace!("Re-queuing {message:?} on replica {index}...");

                if let Err(_) = sender.send(Command::Protocol(message)) {
                    warn!("unable to send message to {index}")
                }
            }
        }
    }

    pub async fn process_outbound(&mut self, source: usize, outbox: &mut BufferedMailbox) {
        for message in outbox.drain_replies() {
            if self.should_drop() {
                continue;
            }

            if let Some(sender) = self.clients.get(&message.destination) {
                trace!(
                    "Sending reply {:?} to client {:?} from replica {source}...",
                    &message.payload,
                    &message.destination
                );

                if let Err(_) = sender.send(message.payload).await {
                    warn!("unable to send message to client {:?}", message.destination)
                }
            }
        }

        for message in outbox.drain_send() {
            if self.should_drop() {
                continue;
            }

            if let Some(sender) = self.senders.get(message.destination) {
                trace!(
                    "Sending protocol message {:?} from {source} to {}...",
                    &message.payload,
                    &message.destination
                );

                if let Err(_) = sender.send(Command::Protocol(message.payload)) {
                    warn!("unable to send message to {:?}", message.destination)
                }
            }
        }

        for message in outbox.drain_broadcast() {
            trace!("Broadcasting message {message:?} from {source} to the group...");

            for (index, sender) in self.senders.iter().enumerate() {
                if self.should_drop() {
                    continue;
                }

                if source != index {
                    if let Err(_) = sender.send(Command::Protocol(message.clone())) {
                        warn!("unable to send message to {index}")
                    }
                }
            }
        }
    }

    fn should_drop(&self) -> bool {
        thread_rng().gen_bool(self.options.network_drop_rate)
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let options = Options::parse();
    let start = Instant::now();
    let configuration = Configuration::from(options.f * 2 + 1);

    let mut network = Network::new(configuration, options);
    let mut receivers = VecDeque::with_capacity(configuration.replicas());

    for _ in 0..configuration.replicas() {
        receivers.push_back(network.bind());
    }

    println!(
        "Running the simulation with {} replicas and {} clients.",
        configuration.replicas(),
        options.clients
    );

    let mut clients: Vec<(Client, Receiver<Reply>)> = Vec::with_capacity(options.clients);
    for _ in 0..options.clients {
        let client = Client::new(configuration);
        let receiver = network.bind_client(client.identifier());

        clients.push((client, receiver));
    }

    let mut replica_tasks = JoinSet::new();
    let mut client_tasks = JoinSet::new();

    for index in 0..configuration.replicas() {
        let receiver = receivers
            .pop_front()
            .expect("no receiver found for replica");

        replica_tasks.spawn(run_replica(
            options,
            Replica::new(configuration, index, Default::default()),
            receiver,
            network.clone(),
        ));
    }

    for (client, receiver) in clients {
        client_tasks.spawn(run_client(options, client, receiver, network.clone()));
    }

    let interval = Duration::from_millis(options.progress_internal);
    let mut total = 0;

    loop {
        match tokio::time::timeout(interval, client_tasks.join_next()).await {
            Ok(Some(Ok(client_total))) => {
                total += client_total;
            }
            Ok(Some(Err(e))) => {
                warn!("unable to join client task: {e}");
            }
            Ok(None) => {
                println!(
                    "Finished processing {total} requests in {} milliseconds",
                    start.elapsed().as_millis()
                );
                break;
            }
            Err(_) => {
                println!(
                    "Processed {total} requests in {} milliseconds",
                    start.elapsed().as_millis()
                );
            }
        }
    }

    replica_tasks.shutdown().await;
}

async fn run_replica(
    options: Options,
    mut replica: Replica<Adder>,
    mut receiver: UnboundedReceiver<Command>,
    mut network: Network,
) {
    let mut mailbox = BufferedMailbox::default();
    let mut checkpoint = replica.checkpoint();
    let mut crashed = false;
    let mut view = replica.view();
    let mut timeout = if replica.is_primary() {
        Duration::from_millis(options.commit_timeout)
    } else {
        Duration::from_millis(options.view_timeout)
    };

    loop {
        if let Some(new_checkpoint) = replica.checkpoint_with_suffix(options.suffix) {
            checkpoint = new_checkpoint;
            trace!(
                "Checkpoint for replica {} includes up to (and including) op-number {:?}.",
                replica.index(),
                checkpoint.committed
            );
        }

        match tokio::time::timeout(timeout, receiver.recv()).await {
            Ok(None) => {
                panic!("replica channel unexpected closed.")
            }
            Ok(Some(Command::Recover)) if crashed => {
                trace!("Recovering replica {}...", replica.index());

                replica = Replica::recovering(
                    replica.configuration(),
                    replica.index(),
                    checkpoint.clone(),
                    &mut mailbox,
                );
                crashed = false;
            }
            Ok(Some(_)) if crashed => {}
            Ok(Some(Command::Recover)) => {}
            Ok(Some(Command::Crash)) => {
                trace!("Crashing replica {}...", replica.index());
                crashed = true;
            }
            Ok(Some(Command::Request(request))) => {
                trace!("Processing {request:?} on replica {}...", replica.index());
                replica.handle_request(request, &mut mailbox);
            }
            Ok(Some(Command::Protocol(message))) => {
                network.requeue(replica.index(), &mut mailbox).await;

                trace!("Processing {message:?} on replica {}...", replica.index());

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
                    info!(
                        "Replica {} is idle in view {:?}...",
                        replica.index(),
                        replica.view()
                    );
                    replica.idle(&mut mailbox);
                }
            }
        }

        network
            .process_outbound(replica.index(), &mut mailbox)
            .await;

        let current_view = replica.view();
        if view != current_view {
            view = current_view;
            timeout = if replica.is_primary() {
                Duration::from_millis(options.commit_timeout)
            } else {
                Duration::from_millis(options.view_timeout)
            };
        }
    }
}

async fn run_client(
    options: Options,
    mut client: Client,
    mut receiver: Receiver<Reply>,
    mut network: Network,
) -> usize {
    if options.requests_per_client == 0 {
        return 0;
    }

    let mut replies = 0;

    let mut request = client.new_request(1);
    let mut primary = client.primary();
    let mut start = Instant::now();

    trace!("Sending request {request:?} to replica {primary}.");

    network.send(primary, request.clone()).await;

    let timeout = Duration::from_millis(options.reply_timeout);

    loop {
        match tokio::time::timeout(timeout, receiver.recv()).await {
            Ok(Some(reply)) => {
                info!(
                            "Client {:?} received reply #{} for request {:?} with view {:?} and payload {} after {} microseconds.",
                            client.identifier(), replies, reply.id, reply.view, reply.payload, start.elapsed().as_micros()
                        );

                client.update_view(&reply);

                replies += 1;
                request = client.new_request(1);
                primary = client.primary();
                start = Instant::now();

                trace!("Sending request {request:?} to replica {primary}.");

                network.send(primary, request.clone()).await;
            }
            Ok(None) => {
                panic!("client channel unexpected closed");
            }
            Err(_) => {
                warn!(
                    "Timed-out waiting for reply on client {:?} after {} milliseconds...",
                    client.identifier(),
                    options.reply_timeout
                );

                network.broadcast(request.clone()).await;
            }
        }

        if replies >= options.requests_per_client {
            info!(
                "Client {:?} received {} replies from the system.",
                client.identifier(),
                replies
            );

            return replies;
        }
    }
}
