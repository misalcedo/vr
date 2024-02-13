use std::cmp::Ordering;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Configuration {
    index: usize,
    replicas: usize,
    group: Group,
}

impl PartialOrd for Configuration {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.index.partial_cmp(&other.index).filter(|_| self.group == other.group)
    }
}

impl Configuration {
    fn new(index: usize, replicas: usize, group: Group) -> Self {
        Self { index, replicas, group }
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn group(&self) -> Group {
        self.group
    }

    pub fn sub_majority(&self) -> usize {
        (self.replicas - 1) / 2
    }

    pub fn quorum(&self) -> usize {
        self.sub_majority() + 1
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Group(u128);

impl Default for Group {
    fn default() -> Self {
        Self(uuid::Uuid::now_v7().as_u128())
    }
}
