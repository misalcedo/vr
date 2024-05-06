use crate::request::{Payload, Request};
use crate::viewstamp::{OpNumber, View};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::{Index, IndexMut};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    request: Request,
    prediction: Payload,
}

impl Entry {
    fn new(request: Request, prediction: Payload) -> Self {
        Self {
            request,
            prediction,
        }
    }

    pub fn request(&self) -> &Request {
        &self.request
    }

    pub fn prediction(&self) -> &Payload {
        &self.prediction
    }
}

#[derive(Clone, Debug, Eq, Serialize, Deserialize)]
pub struct Log {
    view: View,
    range: (OpNumber, OpNumber),
    entries: VecDeque<Entry>,
}

impl Default for Log {
    fn default() -> Self {
        Self {
            view: Default::default(),
            range: (Default::default(), Default::default()),
            entries: Default::default(),
        }
    }
}

impl PartialEq for Log {
    fn eq(&self, other: &Self) -> bool {
        self.view == other.view && self.range == other.range
    }
}

impl Ord for Log {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.view, self.range.1).cmp(&(other.view, other.range.1))
    }
}

impl PartialOrd for Log {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Log {
    pub fn contains(&self, op_number: &OpNumber) -> bool {
        !self.entries.is_empty() && (self.range.0..=self.range.1).contains(op_number)
    }

    pub fn push(
        &mut self,
        view: View,
        request: Request,
        prediction: Payload,
    ) -> (&Entry, OpNumber) {
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

    pub fn get(&self, index: OpNumber) -> Option<&Entry> {
        self.entries.get(index - self.range.0)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn constrain(&mut self, length: usize) {
        if self.entries.len() < length {
            return;
        }

        let drop = self.entries.len() - length;

        self.entries.drain(..drop).count();

        if self.entries.is_empty() {
            self.range.0 = self.range.1;
        } else {
            self.range.0.increment_by(drop);
        }
    }

    pub fn cut(&mut self, end: OpNumber) {
        let offset = end - self.range.0;

        self.entries.drain(..=offset);

        if self.entries.is_empty() {
            self.range = (end, end);
        } else {
            self.range.0 = end.next();
        }
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

    pub fn after(&self, latest: OpNumber) -> Self {
        let index = latest - self.range.0;

        Self {
            view: self.view,
            range: (latest.next(), self.range.1),
            entries: self.entries.iter().skip(index + 1).cloned().collect(),
        }
    }
}

impl Index<OpNumber> for Log {
    type Output = Entry;

    fn index(&self, index: OpNumber) -> &Self::Output {
        let offset = index - self.range.0;
        self.entries.index(offset)
    }
}

impl IndexMut<OpNumber> for Log {
    fn index_mut(&mut self, index: OpNumber) -> &mut Self::Output {
        let offset = index - self.range.0;
        self.entries.index_mut(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::RequestIdentifier;
    use crate::ClientIdentifier;

    #[test]
    fn constrain() {
        let view = View::default();
        let request = Request {
            payload: Default::default(),
            client: ClientIdentifier::default(),
            id: RequestIdentifier::default(),
        };

        let mut log = Log::default();
        let mut new_start = OpNumber::default();

        new_start.increment_by(300);

        for _ in 1..=1000 {
            log.push(view, request.clone(), Default::default());
        }

        let end = log.range.1;

        log.constrain(700);

        assert_eq!(log.range, (new_start.next(), end));
        assert_eq!(log.entries.len(), end - new_start);

        new_start.increment_by(300);
        log.constrain(400);

        assert_eq!(log.range, (new_start.next(), end));
        assert_eq!(log.entries.len(), end - new_start);
    }

    #[test]
    fn constrain_empty() {
        let mut log = Log::default();

        assert!(!log.contains(&OpNumber::default()));

        log.constrain(0);
    }

    #[test]
    fn constrain_to_empty() {
        let view = View::default();
        let request = Request {
            payload: Default::default(),
            client: ClientIdentifier::default(),
            id: RequestIdentifier::default(),
        };

        let mut log = Log::default();

        for _ in 1..=300 {
            log.push(view, request.clone(), Default::default());
        }

        let end = log.range.1;

        log.constrain(0);

        assert_eq!(log.range, (end, end));
        assert_eq!(log.entries.len(), 0);
        assert!(!log.contains(&end));

        log.push(view, request.clone(), Default::default());

        assert_eq!(log.range, (end.next(), end.next()));
        assert_eq!(log.entries.len(), 1);

        log.push(view, request.clone(), Default::default());

        assert_eq!(log.range, (end.next(), end.next().next()));
        assert_eq!(log.entries.len(), 2);
    }
}
