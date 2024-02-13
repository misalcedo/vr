use crate::request::Request;
use std::ops::{Index, IndexMut};

pub struct Entry<R, P> {
    request: Request<R>,
    prediction: P,
}

#[derive(Default)]
pub struct Log<R, P> {
    entries: Vec<Entry<R, P>>,
}

impl<R, P> Log<R, P> {
    pub fn push(&mut self, entry: Entry<R, P>) -> usize {
        self.entries.push(entry);
        self.entries.len()
    }
}

impl<R, P> Index<usize> for Log<R, P> {
    type Output = Entry<R, P>;

    fn index(&self, index: usize) -> &Self::Output {
        self.entries.index(index)
    }
}

impl<R, P> IndexMut<usize> for Log<R, P> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.entries.index_mut(index)
    }
}
