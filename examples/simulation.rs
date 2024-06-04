use axum::body::Body;
use axum::extract::State;
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use bytes::Bytes;
use std::collections::HashMap;
use std::env::args;
use std::io;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use viewstamped_replication::message::{OutboundMessage, ProtocolMessage, Reply, Request};
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
    sender: mpsc::Sender<HttpMessage>,
}

#[derive(Debug)]
pub enum HttpMessage {
    Request(oneshot::Sender<Reply>, Request),
    Protocol(ProtocolMessage),
}

#[tokio::main]
async fn main() {
    let configuration = Configuration::new(vec![
        "127.0.0.1:8378".parse().unwrap(),
        "127.0.0.1:8379".parse().unwrap(),
        "127.0.0.1:8380".parse().unwrap(),
    ]);

    let argument = args().skip(1).next().expect("must pass an index");
    let index = argument.parse().expect("invalid index argument");

    let mut tasks = JoinSet::new();

    tasks.spawn(start_replica(configuration.clone(), index));

    while let Some(_) = tasks.join_next().await {}
}

async fn start_replica(configuration: Configuration, index: usize) -> io::Result<()> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1024);
    let address = configuration[index];
    let app = Router::new()
        .route("/request", post(request))
        .route("/protocol", post(protocol))
        .with_state(Application { sender });

    let mut replica: Replica<Adder> = Replica::new(configuration.clone(), index);
    let mut mailbox = Mailbox::default();
    let mut clients = HashMap::new();
    let client = reqwest::Client::new();

    let receive = async move {
        while let Some(message) = receiver.recv().await {
            match message {
                HttpMessage::Request(sender, request) => {
                    eprintln!("{request:?}");
                    clients.insert(request.client, sender);
                    mailbox.push(request);
                }
                HttpMessage::Protocol(protocol) => {
                    eprintln!("{protocol:?}");
                    mailbox.push(protocol);
                }
            };

            replica.receive(&mut mailbox);

            while let Some(message) = mailbox.pop() {
                match message {
                    OutboundMessage::Reply(message) => {
                        if let Some(sender) = clients.remove(&message.client) {
                            if let Err(_) = sender.send(message) {
                                eprintln!("Unable to inform client of the reply.")
                            }
                        };
                    }
                    OutboundMessage::Protocol(to, message) => {
                        client
                            .post(format!("http://{}/protocol", configuration[to]))
                            .json(&message)
                            .send()
                            .await
                            .unwrap();
                    }
                }
            }
        }

        Ok(())
    };
    let serve = async move {
        let listener = tokio::net::TcpListener::bind(address).await?;
        axum::serve(listener, app).await
    };

    tokio::try_join!(receive, serve).map(|_| ())
}

// TODO: support detecting multiple requests per client.
async fn request(
    State(application): State<Application>,
    Json(message): Json<Request>,
) -> Response<Body> {
    let (sender, receiver) = oneshot::channel();
    if let Err(_) = application
        .sender
        .send(HttpMessage::Request(sender, message))
        .await
    {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    match receiver.await {
        Ok(reply) => Json(reply).into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn protocol(
    State(application): State<Application>,
    Json(message): Json<ProtocolMessage>,
) -> StatusCode {
    match application
        .sender
        .send(HttpMessage::Protocol(message))
        .await
    {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}
