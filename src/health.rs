use crate::model::ReplicaIdentifier;
use crate::stamps::View;

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

    fn notify(&mut self, view: View, replica: ReplicaIdentifier);
}

#[cfg(test)]
mod tests {
    use super::*;

    impl HealthDetector for HealthStatus {
        fn detect(&mut self, view: View, replica: ReplicaIdentifier) -> HealthStatus {
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
