use crate::identifiers::ReplicaIdentifier;

mod local;

pub use local::LocalDriver;

pub trait Driver {
    fn drive<I, II>(&mut self, replicas: II)
    where
        I: Iterator<Item = ReplicaIdentifier> + DoubleEndedIterator,
        II: IntoIterator<Item = ReplicaIdentifier, IntoIter = I>;

    fn drive_to_empty<I, II>(&mut self, replicas: II)
    where
        I: Iterator<Item = ReplicaIdentifier> + DoubleEndedIterator + Clone,
        II: IntoIterator<Item = ReplicaIdentifier, IntoIter = I>;
}
