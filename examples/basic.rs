use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};
use viewstamped_replication::client::Client;
use viewstamped_replication::driver::{Driver, LocalDriver};
use viewstamped_replication::health::LocalHealthDetector;
use viewstamped_replication::identifiers::GroupIdentifier;
use viewstamped_replication::model::Reply;
use viewstamped_replication::Service;

#[derive(Serialize, Deserialize)]
pub enum Request {
    Save(PathBuf, Vec<u8>),
    Load(PathBuf),
}

#[derive(Serialize, Deserialize)]
pub struct File {
    last_modified: Duration,
    contents: Vec<u8>,
}

#[derive(Default)]
pub struct FileSystem {
    files: HashMap<PathBuf, File>,
}

impl Service for FileSystem {
    fn predict(&mut self, _: &[u8]) -> Vec<u8> {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();

        postcard::to_allocvec(&timestamp).unwrap()
    }

    fn invoke(&mut self, payload: &[u8], prediction: &[u8]) -> Vec<u8> {
        let request: Request = postcard::from_bytes(payload).unwrap();
        let last_modified: Duration = postcard::from_bytes(prediction).unwrap();

        match request {
            Request::Save(path, contents) => {
                self.files.insert(
                    path,
                    File {
                        last_modified,
                        contents,
                    },
                );
                Vec::new()
            }
            Request::Load(path) => self
                .files
                .get(&path)
                .and_then(|f| postcard::to_allocvec(f).ok())
                .unwrap_or_default(),
        }
    }
}

fn main() {
    let start = Instant::now();
    let group = GroupIdentifier::new(5);
    let mut driver = LocalDriver::<FileSystem, LocalHealthDetector>::new(group);
    let mut client = Client::new(group);

    let save_message = client.new_message(
        postcard::to_allocvec(&Request::Save(
            PathBuf::from("/hello/world.txt"),
            Vec::from(b"Hello, World!"),
        ))
        .unwrap()
        .as_slice(),
    );

    driver.deliver(save_message);
    driver.drive_to_empty(group);

    let _save_response = driver.fetch(client.identifier());
    let load_message = client.new_message(
        postcard::to_allocvec(&Request::Load(PathBuf::from("/hello/world.txt")))
            .unwrap()
            .as_slice(),
    );

    driver.deliver(load_message);
    driver.drive_to_empty(group);

    let load_response = driver.fetch(client.identifier());
    let reply = load_response
        .into_iter()
        .next()
        .unwrap()
        .payload::<Reply>()
        .unwrap();
    let file: File = postcard::from_bytes(&reply.x).unwrap();

    println!("Elapsed: {:?}", start.elapsed());
    println!("Last Modified: {:?}", &file.last_modified);
    println!("{}", String::from_utf8_lossy(&file.contents));
}
