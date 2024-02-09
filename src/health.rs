use crate::identifiers::ReplicaIdentifier;
use crate::stamps::View;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub enum HealthStatus {
    #[default]
    Normal,
    Suspect,
    Unhealthy,
}

// TODO: Tests a real implementation of a health detector.
pub trait HealthDetector {
    fn detect(&mut self, view: View, replica: ReplicaIdentifier) -> HealthStatus;
}

impl HealthDetector for HealthStatus {
    fn detect(&mut self, _: View, _: ReplicaIdentifier) -> HealthStatus {
        *self
    }
}

#[derive(Debug, Default)]
pub struct Suspect;

impl HealthDetector for Suspect {
    fn detect(&mut self, _: View, _: ReplicaIdentifier) -> HealthStatus {
        HealthStatus::Suspect
    }
}

#[derive(Debug, Default)]
pub struct Unhealthy;

impl HealthDetector for Unhealthy {
    fn detect(&mut self, _: View, _: ReplicaIdentifier) -> HealthStatus {
        HealthStatus::Unhealthy
    }
}

#[derive(Clone, Debug, Default)]
pub struct LocalHealthDetector {
    status: Rc<RefCell<HashMap<ReplicaIdentifier, HealthStatus>>>,
}

impl LocalHealthDetector {
    pub fn set_status(&mut self, replica: ReplicaIdentifier, status: HealthStatus) {
        self.status.borrow_mut().insert(replica, status);
    }
}

impl HealthDetector for LocalHealthDetector {
    fn detect(&mut self, view: View, replica: ReplicaIdentifier) -> HealthStatus {
        self.status
            .borrow()
            .get(&replica.primary(view))
            .copied()
            .unwrap_or(HealthStatus::Normal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identifiers::GroupIdentifier;

    #[test]
    fn order() {
        assert!(HealthStatus::Normal < HealthStatus::Suspect);
        assert!(HealthStatus::Suspect < HealthStatus::Unhealthy);
        assert!(HealthStatus::Normal < HealthStatus::Unhealthy);
    }

    #[test]
    fn local() {
        let group = GroupIdentifier::new(3);
        let view = View::default();
        let replica = group.replicas(view).next().unwrap();
        let mut detector = LocalHealthDetector::default();
        let mut clone = detector.clone();

        clone.set_status(group.primary(view), HealthStatus::Unhealthy);

        assert_eq!(detector.detect(view, replica), HealthStatus::Unhealthy);
        assert_eq!(detector.detect(view.next(), replica), HealthStatus::Normal);
    }
}
