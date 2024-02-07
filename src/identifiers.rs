use crate::stamps::View;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct ReplicaIdentifier(GroupIdentifier, usize);

impl ReplicaIdentifier {
    pub fn group(&self) -> GroupIdentifier {
        self.0
    }

    pub fn primary(&self, view: View) -> Self {
        self.0.primary(view)
    }

    pub fn sub_majority(&self) -> usize {
        self.0.sub_majority()
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct GroupIdentifier(u128, usize);

impl Default for GroupIdentifier {
    fn default() -> Self {
        Self::new(3)
    }
}

impl GroupIdentifier {
    pub fn new(replicas: usize) -> Self {
        Self(uuid::Uuid::now_v7().as_u128(), replicas)
    }

    pub fn primary(&self, view: View) -> ReplicaIdentifier {
        ReplicaIdentifier(*self, (view.as_u128() % (self.1 as u128)) as usize)
    }

    pub fn replicas(&self) -> impl Iterator<Item = ReplicaIdentifier> {
        let clone = *self;
        (0..self.1)
            .into_iter()
            .map(move |i| ReplicaIdentifier(clone, i))
    }

    pub fn sub_majority(&self) -> usize {
        (self.1 - 1) / 2
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct ClientIdentifier(u128);

impl Default for ClientIdentifier {
    fn default() -> Self {
        Self(uuid::Uuid::now_v7().as_u128())
    }
}

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct RequestIdentifier(u128);

impl RequestIdentifier {
    pub fn increment(&mut self) -> Self {
        self.0 += 1;
        *self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sub_majority() {
        assert_eq!(GroupIdentifier::new(3).sub_majority(), 1);
        assert_eq!(GroupIdentifier::new(4).sub_majority(), 1);
        assert_eq!(GroupIdentifier::new(5).sub_majority(), 2);
    }
}
