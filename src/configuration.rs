#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Configuration {
    replicas: usize,
    group: Group,
}

impl Configuration {
    fn new(replicas: usize, group: Group) -> Self {
        Self { replicas, group }
    }

    pub fn replicas(&self) -> usize {
        self.replicas
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

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Group(u128);

impl Default for Group {
    fn default() -> Self {
        Self(uuid::Uuid::now_v7().as_u128())
    }
}
