use crate::request::Request;
use crate::viewstamp::{OpNumber, View};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::{Index, IndexMut};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Entry<R, P> {
    request: Request<R>,
    prediction: P,
}

impl<R, P> Entry<R, P> {
    fn new(request: Request<R>, prediction: P) -> Self {
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Log<R, P> {
    view: View,
    range: (OpNumber, OpNumber),
    entries: VecDeque<Entry<R, P>>,
}

impl<R, P> Default for Log<R, P> {
    fn default() -> Self {
        Self {
            view: Default::default(),
            range: (Default::default(), Default::default()),
            entries: Default::default(),
        }
    }
}

impl<R, P> Eq for Log<R, P> {}

impl<R, P> PartialEq for Log<R, P> {
    fn eq(&self, other: &Self) -> bool {
        self.view == other.view && self.range == other.range
    }
}

impl<R, P> Ord for Log<R, P> {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.view, self.range.1).cmp(&(other.view, other.range.1))
    }
}

impl<R, P> PartialOrd for Log<R, P> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.view, self.range.1).partial_cmp(&(other.view, other.range.1))
    }
}

impl<R, P> Log<R, P>
where
    R: Clone,
    P: Clone,
{
    pub fn after(&self, latest: OpNumber) -> Self {
        let entries = latest - self.range.0;

        Self {
            view: self.view,
            range: (latest.next(), self.range.1),
            entries: self.entries.iter().skip(entries + 1).cloned().collect(),
        }
    }
}

impl<R, P> Log<R, P> {
    pub fn contains(&self, op_number: &OpNumber) -> bool {
        (self.range.0..=self.range.1).contains(op_number)
    }

    pub fn push(
        &mut self,
        view: View,
        request: Request<R>,
        prediction: P,
    ) -> (&Entry<R, P>, OpNumber) {
        self.view = view;
        self.range.1.increment();

        if self.entries.is_empty() {
            self.range.0.increment();
        }

        let entry = Entry::new(request, prediction);
        let index = self.entries.len();

        self.entries.push_back(entry);

        (&self.entries[index], self.range.1)
    }

    pub fn first_op_number(&self) -> OpNumber {
        self.range.0
    }

    pub fn last_op_number(&self) -> OpNumber {
        self.range.1
    }

    pub fn last_normal_view(&self) -> View {
        self.view
    }

    pub fn next_op_number(&self) -> OpNumber {
        self.range.1.next()
    }

    pub fn get(&self, index: OpNumber) -> Option<&Entry<R, P>> {
        self.entries.get(index - self.range.0)
    }

    pub fn compact(&mut self, start: OpNumber) {
        if start == self.range.0 {
            return;
        }

        let old_start = self.range.0;
        let index = (start - old_start) - 1;

        self.range.0 = start;
        self.entries.drain(..index);
    }

    pub fn truncate(&mut self, last: OpNumber) {
        self.range.1 = last;
        self.entries.truncate((last - self.range.0) + 1);
    }

    pub fn extend(&mut self, tail: Self) {
        self.view = tail.view;
        self.range.1 = tail.range.1;
        self.entries.extend(tail.entries);
    }
}

impl<R, P> Index<OpNumber> for Log<R, P> {
    type Output = Entry<R, P>;

    fn index(&self, index: OpNumber) -> &Self::Output {
        let offset = index - self.first_op_number();
        self.entries.index(offset)
    }
}

impl<R, P> IndexMut<OpNumber> for Log<R, P> {
    fn index_mut(&mut self, index: OpNumber) -> &mut Self::Output {
        let offset = index - self.first_op_number();
        self.entries.index_mut(offset)
    }
}
