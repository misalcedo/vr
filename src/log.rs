use crate::request::Request;
use serde::{Deserialize, Serialize};
use std::ops::{Index, IndexMut};

#[derive(Clone, Serialize, Deserialize)]
pub struct Entry<R, P> {
    request: Request<R>,
    prediction: P,
}

impl<R, P> Entry<R, P> {
    pub fn new(request: Request<R>, prediction: P) -> Self {
        Self {
            request,
            prediction,
        }
    }

    pub fn request(&self) -> &Request<R> {
        &self.request
    }
    pub fn prediction(&self) -> &P {
        &self.prediction
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct Log<R, P> {
    entries: Vec<Entry<R, P>>,
}

impl<R, P> Log<R, P>
where
    R: Clone,
    P: Clone,
{
    pub fn after(&self, latest: usize) -> Self {
        Self {
            entries: self.entries.iter().skip(latest).cloned().collect(),
        }
    }
}

impl<R, P> Log<R, P> {
    pub fn push(&mut self, entry: Entry<R, P>) -> (&Entry<R, P>, usize) {
        let index = self.entries.len();

        self.entries.push(entry);
        (&self.entries[index], index + 1)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn get(&self, index: usize) -> Option<&Entry<R, P>> {
        self.entries.get(index)
    }

    pub fn truncate(&mut self, last: usize) {
        self.entries.truncate(last)
    }

    pub fn extend(&mut self, tail: Self) {
        self.entries.extend(tail.entries);
    }
}

impl<R, P> Index<usize> for Log<R, P> {
    type Output = Entry<R, P>;

    fn index(&self, index: usize) -> &Self::Output {
        self.entries.index(index.checked_sub(1).unwrap_or_default())
    }
}

impl<R, P> IndexMut<usize> for Log<R, P> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.entries
            .index_mut(index.checked_sub(1).unwrap_or_default())
    }
}
