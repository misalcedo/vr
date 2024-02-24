use crate::configuration::Configuration;
use serde::{Deserialize, Serialize};
use std::ops::{Rem, Sub};

#[derive(
    Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize,
)]
#[repr(transparent)]
pub struct OpNumber(u128);

impl Sub for OpNumber {
    type Output = usize;

    fn sub(self, rhs: Self) -> Self::Output {
        (self.0 - rhs.0) as usize
    }
}

impl OpNumber {
    pub fn increment(&mut self) {
        self.0 += 1;
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

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
        self.0 += 1;
    }

    pub fn next(&self) -> Self {
        Self(1 + self.0)
    }
}
