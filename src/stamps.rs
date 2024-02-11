use std::num::NonZeroUsize;

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct OpNumber(Option<NonZeroUsize>);

impl OpNumber {
    pub(crate) fn new(value: usize) -> Self {
        Self(NonZeroUsize::new(value))
    }

    pub(crate) fn as_usize(&self) -> usize {
        self.0.map(NonZeroUsize::get).unwrap_or_default()
    }
}

impl OpNumber {
    pub fn increment(&mut self) {
        self.0 = NonZeroUsize::new(1 + self.0.map(NonZeroUsize::get).unwrap_or(0))
    }

    pub fn next(&self) -> Self {
        Self(NonZeroUsize::new(
            1 + self.0.map(NonZeroUsize::get).unwrap_or(0),
        ))
    }
}

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct View(u128);

impl View {
    pub fn increment(&mut self) {
        self.0 = 1 + self.0;
    }

    pub fn next(&self) -> Self {
        Self(1 + self.0)
    }

    pub(crate) fn as_u128(&self) -> u128 {
        self.0
    }
}
