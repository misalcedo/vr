#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Configuration {
    replicas: usize,
    group: Group,
}

impl From<usize> for Configuration {
    fn from(value: usize) -> Self {
        Self::new(value, Default::default())
    }
}

impl Configuration {
    pub fn new(replicas: usize, group: Group) -> Self {
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
        Self(uuid::Uuid::new_v4().as_u128())
    }
}
