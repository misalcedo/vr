use crate::new_model::{Message, ReplicaIdentifier, View};

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub enum HealthStatus {
    #[default]
    Normal,
    Suspect,
    Unhealthy,
}

pub trait HealthDetector {
    fn detect(&mut self, view: View, replica: ReplicaIdentifier) -> HealthStatus;

    fn notify(&mut self, view: View, message: Message);
}

#[cfg(test)]
mod tests {
    use super::*;

    impl HealthDetector for HealthStatus {
        fn detect(&mut self, view: View, replica: ReplicaIdentifier) -> HealthStatus {
            *self
        }

        fn notify(&mut self, _: View, _: Message) {}
    }
}
