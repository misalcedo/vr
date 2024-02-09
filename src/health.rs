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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order() {
        assert!(HealthStatus::Normal < HealthStatus::Suspect);
        assert!(HealthStatus::Suspect < HealthStatus::Unhealthy);
        assert!(HealthStatus::Normal < HealthStatus::Unhealthy);
    }
}
