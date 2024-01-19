use std::cmp::Ordering;
use std::collections::HashMap;
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
    pub fn increment(&self) -> Self {
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
    pub fn increment(&self) -> Self {
        Self(1 + self.0)
    }

    pub fn primary_index(&self, group_size: usize) -> usize {
        (self.0 % (group_size as u128)) as usize
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ViewTable {
    table: HashMap<View, OpNumber>,
    view: View,
    op_number: OpNumber
}

impl PartialOrd for ViewTable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.view, self.op_number).partial_cmp(&(other.view, other.op_number))
    }
}

impl Ord for ViewTable {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.view, self.op_number).cmp(&(other.view, other.op_number))
    }
}

impl ViewTable {
    pub fn view(&self) -> View {
        self.view
    }

    pub fn op_number(&self) -> OpNumber {
        self.op_number
    }

    pub fn primary_index(&self, group_size: usize) -> usize {
        self.view.primary_index(group_size)
    }

    pub fn next_view(&mut self) -> View {
        self.table.insert(self.view, self.op_number);
        self.view = self.view.increment();
        self.view
    }

    pub fn next_op_number(&mut self) -> OpNumber {
        self.op_number = self.op_number.increment();
        self.op_number
    }
}