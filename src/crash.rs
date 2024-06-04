use crate::message::ProtocolMessage;
use crate::Configuration;

/// Detects whether the current replica crashed or is part of a new deployment.
pub struct CrashDetector {
    configuration: Configuration,
    state: Vec<Option<u128>>,
}

impl CrashDetector {
    pub fn new(configuration: Configuration, index: usize, nonce: u128) -> Self {
        let mut state = vec![None; configuration.len()];

        state[index] = Some(nonce);

        Self {
            configuration,
            state,
        }
    }

    /// A non-fused crash detector.
    /// Returns None when a decision cannot yet be made.
    /// Does not guarantee to always return the same decision after the first decision is returned.
    pub fn update(&mut self, message: impl Into<ProtocolMessage>) -> Option<bool> {
        match message.into() {
            ProtocolMessage::Recover(request) => {
                self.state[request.index] = Some(request.nonce);

                // SAFETY: we cannot make a decision unless all replicas are recovering.
                if self.state.contains(&None) {
                    return None;
                }

                Some(false)
            }
            // SAFETY: we know we crashed if we receive a protocol message besides RECOVER.
            _ => Some(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{PrepareOk, Recover};

    #[test]
    fn crashed() {
        let configuration = Configuration::new([
            "127.0.0.1".parse().unwrap(),
            "127.0.0.2".parse().unwrap(),
            "127.0.0.3".parse().unwrap(),
        ]);
        let mut detector = CrashDetector::new(configuration, 1, 0);

        let decision = detector.update(PrepareOk {
            view: 1,
            op_number: 5,
            index: 2,
        });

        assert_eq!(decision, Some(true));
    }

    #[test]
    fn above_threshold() {
        let configuration = Configuration::new([
            "127.0.0.1".parse().unwrap(),
            "127.0.0.2".parse().unwrap(),
            "127.0.0.3".parse().unwrap(),
        ]);
        let mut detector = CrashDetector::new(configuration, 1, 0);

        let decision = detector.update(Recover { index: 2, nonce: 1 });
        assert_eq!(decision, None);

        let decision = detector.update(PrepareOk {
            view: 1,
            op_number: 5,
            index: 2,
        });
        assert_eq!(decision, Some(true));
    }

    #[test]
    fn fresh() {
        let configuration = Configuration::new([
            "127.0.0.1".parse().unwrap(),
            "127.0.0.2".parse().unwrap(),
            "127.0.0.3".parse().unwrap(),
        ]);
        let mut detector = CrashDetector::new(configuration, 1, 1);

        let decision1 = detector.update(Recover { index: 0, nonce: 0 });
        assert_eq!(decision1, None);

        let decision2 = detector.update(Recover { index: 2, nonce: 2 });
        assert_eq!(decision2, Some(false));
    }

    #[test]
    fn duplicate() {
        let configuration = Configuration::new([
            "127.0.0.1".parse().unwrap(),
            "127.0.0.2".parse().unwrap(),
            "127.0.0.3".parse().unwrap(),
        ]);
        let mut detector = CrashDetector::new(configuration, 1, 1);

        let decision1 = detector.update(Recover { index: 1, nonce: 0 });
        assert_eq!(decision1, None);

        let decision2 = detector.update(Recover { index: 2, nonce: 2 });
        assert_eq!(decision2, None);
    }
}
