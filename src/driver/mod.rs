use crate::identifiers::{ClientIdentifier, ReplicaIdentifier};
use crate::model::Message;

mod local;

pub use local::LocalDriver;

pub trait Driver {
    fn drive<I, II>(&mut self, replicas: II)
    where
        I: Iterator<Item = ReplicaIdentifier>,
        II: IntoIterator<Item = ReplicaIdentifier, IntoIter = I>;

    fn drive_to_empty<I, II>(&mut self, replicas: II)
    where
        I: Iterator<Item = ReplicaIdentifier> + Clone,
        II: IntoIterator<Item = ReplicaIdentifier, IntoIter = I>;
}
