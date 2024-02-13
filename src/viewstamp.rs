use std::ops::Rem;
use crate::configuration::Configuration;

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct View(u128);

impl Rem<Configuration> for View {
    type Output = usize;

    fn rem(self, rhs: Configuration) -> Self::Output {
        (self.0 % (rhs.index() as u128)) as usize
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
