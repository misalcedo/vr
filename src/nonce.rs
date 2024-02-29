use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Nonce(u128);

impl Default for Nonce {
    fn default() -> Self {
        Self(uuid::Uuid::new_v4().as_u128())
    }
}
