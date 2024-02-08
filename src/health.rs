use crate::identifiers::ReplicaIdentifier;
use crate::stamps::View;

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub enum HealthStatus {
    #[default]
    Normal,
    Suspect,
    Unhealthy,
}

// TODO: Tests a real implementation of a health detector.
// TODO: Add a local implementation of a health detector that works with the local driver's crash method.
pub trait HealthDetector {
    fn detect(&mut self, view: View, replica: ReplicaIdentifier) -> HealthStatus;

    // TODO: Remove this method as the replicas should not be in charge of giving the detector information about the health of other replicas.
    fn notify(&mut self, view: View, replica: ReplicaIdentifier);
}

#[cfg(test)]
mod tests {
    use super::*;

    impl HealthDetector for HealthStatus {
        fn detect(&mut self, _: View, _: ReplicaIdentifier) -> HealthStatus {
            *self
        }

        fn notify(&mut self, _: View, _: ReplicaIdentifier) {
            *self = HealthStatus::Normal;
        }
    }

    #[test]
    fn order() {
        assert!(HealthStatus::Normal < HealthStatus::Suspect);
        assert!(HealthStatus::Suspect < HealthStatus::Unhealthy);
        assert!(HealthStatus::Normal < HealthStatus::Unhealthy);
    }
}
