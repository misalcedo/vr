use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::num::NonZeroU128;

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct OpNumber(Option<NonZeroU128>);

impl From<u128> for OpNumber {
    fn from(value: u128) -> Self {
        Self(NonZeroU128::new(value))
    }
}

impl From<usize> for OpNumber {
    fn from(value: usize) -> Self {
        Self(NonZeroU128::new(value as u128))
    }
}

impl From<i32> for OpNumber {
    fn from(value: i32) -> Self {
        Self(NonZeroU128::new(value as u128))
    }
}

impl From<OpNumber> for u128 {
    fn from(value: OpNumber) -> Self {
        value.0.map(NonZeroU128::get).unwrap_or(0)
    }
}

impl OpNumber {
    pub fn increment(&mut self) {
        self.0 = NonZeroU128::new(1 + u128::from(*self))
    }

    pub fn next(&self) -> Self {
        Self(NonZeroU128::new(1 + u128::from(*self)))
    }
}

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct View(u128);

impl From<u128> for View {
    fn from(value: u128) -> Self {
        Self(value)
    }
}

impl From<View> for u128 {
    fn from(value: View) -> Self {
        value.0
    }
}

impl View {
    pub fn increment(&mut self) {
        self.0 = 1 + self.0;
    }

    pub fn primary_index(&self, group_size: usize) -> usize {
        (self.0 % (group_size as u128)) as usize
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ViewTable {
    table: BTreeMap<View, OpNumber>,
}

impl PartialOrd for ViewTable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.table
            .last_key_value()
            .partial_cmp(&other.table.last_key_value())
    }
}

impl Ord for ViewTable {
    fn cmp(&self, other: &Self) -> Ordering {
        self.table
            .last_key_value()
            .cmp(&other.table.last_key_value())
    }
}

impl ViewTable {
    pub fn insert_view(&mut self, view: View, op_number: OpNumber) {
        self.table.insert(view, op_number);
    }

    pub fn last_entry(&self) -> (View, OpNumber) {
        self.table
            .last_key_value()
            .map(|kv| (kv.0.clone(), kv.1.clone()))
            .unwrap_or_default()
    }

    pub fn last_op_number(&self) -> OpNumber {
        self.table
            .last_key_value()
            .map(|kv| kv.1.clone())
            .unwrap_or_default()
    }
}
