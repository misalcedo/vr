use crate::stamps::View;
use std::cmp::Ordering;

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

impl PartialOrd for ReplicaIdentifier {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.1.partial_cmp(&other.1).filter(|_| self.0 == other.0)
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

    pub fn size(&self) -> usize {
        self.1
    }

    pub fn primary(&self, view: View) -> ReplicaIdentifier {
        ReplicaIdentifier(*self, (view.as_u128() % (self.1 as u128)) as usize)
    }

    pub fn replicas(
        &self,
        view: View,
    ) -> impl Iterator<Item = ReplicaIdentifier> + DoubleEndedIterator + Clone {
        let primary = self.primary(view);

        self.clone().into_iter().filter(move |r| r != &primary)
    }

    pub fn sub_majority(&self) -> usize {
        (self.1 - 1) / 2
    }
}

impl IntoIterator for GroupIdentifier {
    type Item = ReplicaIdentifier;
    type IntoIter = std::vec::IntoIter<ReplicaIdentifier>;

    fn into_iter(self) -> Self::IntoIter {
        let replicas: Vec<ReplicaIdentifier> =
            (0..self.1).map(|i| ReplicaIdentifier(self, i)).collect();

        replicas.into_iter()
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

    #[test]
    fn into_iter() {
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.into_iter().collect();

        assert_eq!(
            replicas,
            vec![
                ReplicaIdentifier(group, 0),
                ReplicaIdentifier(group, 1),
                ReplicaIdentifier(group, 2)
            ]
        );
    }

    #[test]
    fn reversed() {
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.into_iter().rev().collect();

        assert_eq!(
            replicas,
            vec![
                ReplicaIdentifier(group, 2),
                ReplicaIdentifier(group, 1),
                ReplicaIdentifier(group, 0)
            ]
        );
    }

    #[test]
    fn primary() {
        let group = GroupIdentifier::new(3);
        let replicas: Vec<ReplicaIdentifier> = group.into_iter().collect();

        let mut view = View::default();

        for i in 0..10 {
            assert_eq!(group.primary(view), replicas[i % group.size()]);
            view.increment()
        }
    }

    #[test]
    fn replicas() {
        let group = GroupIdentifier::new(3);

        let mut view = View::default();

        for _ in 0..10 {
            let primary = group.primary(view);
            assert!(group.replicas(view).all(|r| r != primary));
            view.increment();
        }
    }
}
