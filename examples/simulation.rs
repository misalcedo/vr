use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use bytes::Bytes;
use std::io;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinSet;
use viewstamped_replication::message::{Message, ProtocolMessage, Request};
use viewstamped_replication::{Configuration, Mailbox, Replica, Service};

#[derive(Default)]
pub struct Adder(i32);

impl Service for Adder {
    fn invoke(&mut self, request: Bytes) -> Bytes {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(request.as_ref());

        let delta = i32::from_be_bytes(bytes);

        self.0 += delta;
        Bytes::from(self.0.to_be_bytes().to_vec())
    }
}

#[derive(Clone)]
pub struct Application {
    index: usize,
    sender: Sender<Message>,
}

#[tokio::main]
async fn main() {
    let configuration = Configuration::new(vec![
        "127.0.0.1:8378".parse().unwrap(),
        "127.0.0.2:8378".parse().unwrap(),
        "127.0.0.3:8378".parse().unwrap(),
    ]);

    let mut tasks = JoinSet::new();

    tasks.spawn(start_replica(configuration.clone(), 0));
    tasks.spawn(start_replica(configuration.clone(), 1));
    tasks.spawn(start_replica(configuration.clone(), 2));

    while let Some(_) = tasks.join_next().await {}
}

async fn start_replica(configuration: Configuration, index: usize) -> io::Result<()> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1024);
    let address = configuration[index];
    let app = Router::new()
        .route("/request", post(request))
        .route("/protocol", post(protocol))
        .with_state(Application { index, sender });

    let mut replica: Replica<Adder> = Replica::new(configuration, index);
    let mut mailbox = Mailbox::default();

    let receive = async move {
        while let Some(message) = receiver.recv().await {
            mailbox.push(message);
            replica.receive(&mut mailbox);
        }

        Ok(())
    };
    let serve = async move {
        let listener = tokio::net::TcpListener::bind(address).await?;
        axum::serve(listener, app).await
    };

    tokio::try_join!(receive, serve).map(|_| ())
}

async fn request(
    State(application): State<Application>,
    Json(message): Json<Request>,
) -> StatusCode {
    match application.sender.send(message.into()).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }

    // TODO: Implement a client.
}

async fn protocol(
    State(application): State<Application>,
    Json(message): Json<ProtocolMessage>,
) -> StatusCode {
    match application
        .sender
        .send(Message::Protocol(application.index, message.into()))
        .await
    {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}
