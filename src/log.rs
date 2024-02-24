use crate::request::Request;
use crate::viewstamp::{OpNumber, View, Viewstamp};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::ops::{Index, IndexMut, RangeInclusive};

#[derive(Clone, Serialize, Deserialize)]
pub struct Entry<R, P> {
    viewstamp: Viewstamp,
    request: Request<R>,
    prediction: P,
}

impl<R, P> Eq for Entry<R, P> {}

impl<R, P> PartialEq for Entry<R, P> {
    fn eq(&self, other: &Self) -> bool {
        self.viewstamp.eq(&other.viewstamp)
    }
}

impl<R, P> Entry<R, P> {
    fn new(viewstamp: Viewstamp, request: Request<R>, prediction: P) -> Self {
        Self {
            viewstamp,
            request,
            prediction,
        }
    }

    pub fn viewstamp(&self) -> &Viewstamp {
        &self.viewstamp
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
    op_number: OpNumber,
    entries: Vec<Entry<R, P>>,
}

impl<R, P> Eq for Log<R, P> {}

impl<R, P> PartialEq for Log<R, P> {
    fn eq(&self, other: &Self) -> bool {
        self.entries.iter().eq(other.entries.iter())
    }
}

impl<R, P> Ord for Log<R, P> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.entries
            .last()
            .map(Entry::viewstamp)
            .cmp(&other.entries.last().map(Entry::viewstamp))
    }
}

impl<R, P> PartialOrd for Log<R, P> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.entries
            .last()
            .map(Entry::viewstamp)
            .partial_cmp(&other.entries.last().map(Entry::viewstamp))
    }
}

impl<R, P> Log<R, P>
where
    R: Clone,
    P: Clone,
{
    pub fn after(&self, latest: OpNumber) -> Self {
        Self {
            op_number: self.op_number,
            entries: self
                .entries
                .iter()
                .skip(latest - self.first_op_number() + 1)
                .cloned()
                .collect(),
        }
    }
}

impl<R, P> Log<R, P> {
    pub fn contains(&self, op_number: &OpNumber) -> bool {
        self.range().contains(op_number)
    }

    pub fn push(&mut self, view: View, request: Request<R>, prediction: P) -> &Entry<R, P> {
        self.op_number.increment();

        let entry = Entry::new(Viewstamp::new(view, self.op_number), request, prediction);

        self.entries.push(entry);
        &self[self.op_number]
    }

    pub fn first_op_number(&self) -> OpNumber {
        self.entries
            .first()
            .map(Entry::viewstamp)
            .map(Viewstamp::op_number)
            .unwrap_or_default()
    }

    pub fn last_op_number(&self) -> OpNumber {
        self.op_number
    }

    pub fn next_op_number(&self) -> OpNumber {
        self.op_number.next()
    }

    pub fn get(&self, index: OpNumber) -> Option<&Entry<R, P>> {
        self.entries.get(index - self.first_op_number())
    }

    pub fn truncate(&mut self, last: OpNumber) {
        self.entries.truncate(last - self.first_op_number() + 1);
        self.op_number = self
            .entries
            .last()
            .map(Entry::viewstamp)
            .map(Viewstamp::op_number)
            .unwrap_or_default();
    }

    pub fn extend(&mut self, tail: Self) {
        self.entries.extend(tail.entries);
        self.op_number = tail.op_number;
    }

    fn range(&self) -> RangeInclusive<OpNumber> {
        self.first_op_number()..=self.last_op_number()
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
