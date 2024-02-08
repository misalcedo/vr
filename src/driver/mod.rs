use crate::identifiers::{ClientIdentifier, ReplicaIdentifier};
use crate::model::Message;

mod local;

pub trait Driver {
    fn drive<I, II>(&mut self, replicas: II)
    where
        I: Iterator<Item = ReplicaIdentifier>,
        II: IntoIterator<Item = ReplicaIdentifier, IntoIter = I>;

    fn crash<I, II>(&mut self, replicas: II)
    where
        I: Iterator<Item = ReplicaIdentifier>,
        II: IntoIterator<Item = ReplicaIdentifier, IntoIter = I>;

    fn recover<I, II>(&mut self, replicas: II)
    where
        I: Iterator<Item = ReplicaIdentifier>,
        II: IntoIterator<Item = ReplicaIdentifier, IntoIter = I>;

    fn deliver(&mut self, message: Message);

    fn fetch(&mut self, client: ClientIdentifier) -> Vec<Message>;
}
