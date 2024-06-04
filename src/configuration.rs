use std::net::SocketAddr;
use std::ops::{Index, Range};

#[derive(Clone)]
pub struct Configuration {
    addresses: Vec<SocketAddr>,
}

impl Index<usize> for Configuration {
    type Output = SocketAddr;

    fn index(&self, index: usize) -> &Self::Output {
        self.addresses.index(index)
    }
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
