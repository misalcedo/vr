use std::net::SocketAddr;
use std::ops::Range;

#[derive(Clone)]
pub struct Configuration {
    addresses: Vec<SocketAddr>,
}

impl IntoIterator for &Configuration {
    type Item = usize;
    type IntoIter = Range<usize>;

    fn into_iter(self) -> Self::IntoIter {
        0..self.addresses.len()
    }
}

impl Configuration {
    pub fn new(addresses: impl Into<Vec<SocketAddr>>) -> Self {
        Self {
            addresses: addresses.into(),
        }
    }

    pub fn len(&self) -> usize {
        self.addresses.len()
    }

    pub fn threshold(&self) -> usize {
        (self.addresses.len() - 1) / 2
    }
}
