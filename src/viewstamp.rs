use crate::configuration::Configuration;
use serde::{Deserialize, Serialize};
use std::ops::Rem;

#[derive(
    Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize,
)]
#[repr(transparent)]
pub struct View(u128);

impl Rem<View> for Configuration {
    type Output = usize;

    fn rem(self, rhs: View) -> Self::Output {
        (rhs.0 % (self.replicas() as u128)) as usize
    }
}

impl View {
    pub fn increment(&mut self) {
        self.0 = 1 + self.0;
    }

    pub fn next(&self) -> Self {
        Self(1 + self.0)
    }
}
