#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Default)]
pub struct ViewStamp {
    view_id: ViewIdentifier,
    timestamp: Timestamp
}

impl ViewStamp {
    pub fn new(view: ViewIdentifier, timestamp: Timestamp) -> Self {
        Self { view_id: view, timestamp }
    }
}

impl Iterator for ViewStamp {
    type Item = Self;

    fn next(&mut self) -> Option<Self::Item> {
        match self.timestamp.next()  {
            None => {
                let view = self.view_id.next()?;
                Some(Self { view_id: view, timestamp: Timestamp::default() })
            }
            Some(timestamp) => Some(Self { view_id: self.view_id, timestamp }),
        }
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Default)]
#[repr(transparent)]
pub struct ViewIdentifier(u128);

impl From<u128> for ViewIdentifier {
    fn from(value: u128) -> Self {
        Self(value)
    }
}

impl Iterator for ViewIdentifier {
    type Item = Self;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.checked_add(1).map(Self)
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Default)]
#[repr(transparent)]
pub struct Timestamp(u128);

impl From<u128> for Timestamp {
    fn from(value: u128) -> Self {
        Self(value)
    }
}

impl Iterator for Timestamp {
    type Item = Self;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.checked_add(1).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_view() {
        let mut view = ViewIdentifier::default();

        assert_eq!(view.next(), Some(ViewIdentifier::from(1)));
    }

    #[test]
    fn max_view() {
        let mut view = ViewIdentifier::from(u128::MAX);

        assert_eq!(view.next(), None);
    }

    #[test]
    fn next_timestamp() {
        let mut timestamp = Timestamp::default();

        assert_eq!(timestamp.next(), Some(Timestamp::from(1)));
    }

    #[test]
    fn max_timestamp() {
        let mut timestamp = Timestamp::from(u128::MAX);

        assert_eq!(timestamp.next(), None);
    }

    #[test]
    fn view_stamp() {
        let a = ViewStamp::default();
        let b = ViewStamp::new(ViewIdentifier::from(1), Timestamp::default());
        let c = ViewStamp::new(ViewIdentifier::from(1), Timestamp::from(1));

        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn next_view_stamp() {
        let mut view_stamp = ViewStamp::default();

        assert_eq!(view_stamp.next(), Some(ViewStamp::new(ViewIdentifier::default(), Timestamp::from(1))));
    }

    #[test]
    fn carry_over_view_stamp() {
        let mut view_stamp = ViewStamp::new(ViewIdentifier::default(), Timestamp::from(u128::MAX));

        assert_eq!(view_stamp.next(), Some(ViewStamp::new(ViewIdentifier::from(1), Timestamp::default())));
    }

    #[test]
    fn max_view_stamp() {
        let mut view_stamp = ViewStamp::new(ViewIdentifier::from(u128::MAX), Timestamp::from(u128::MAX));

        assert_eq!(view_stamp.next(), None);
    }
}