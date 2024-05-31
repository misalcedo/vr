use std::net::IpAddr;
use std::ops::Range;

pub struct Configuration {
    addresses: Vec<IpAddr>,
}

impl IntoIterator for &Configuration {
    type Item = usize;
    type IntoIter = Range<usize>;

    fn into_iter(self) -> Self::IntoIter {
        0..self.addresses.len()
    }
}

impl Configuration {
    pub fn len(&self) -> usize {
        self.addresses.len()
    }

    pub fn threshold(&self) -> usize {
        (self.addresses.len() - 1) / 2
    }
}
