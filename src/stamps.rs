use std::collections::HashMap;
use std::num::NonZeroU128;

#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
#[repr(transparent)]
pub struct OpNumber(Option<NonZeroU128>);

impl From<u128> for OpNumber {
    fn from(value: u128) -> Self {
        Self(NonZeroU128::new(value))
    }
}

impl From<OpNumber> for u128 {
    fn from(value: OpNumber) -> Self {
        value.0.map(NonZeroU128::get).unwrap_or(0)
    }
}

impl OpNumber {
    pub fn increment(&self) -> Self {
        Self(NonZeroU128::new(1 + u128::from(self)))
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
}

#[derive(Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct ViewTable {
    table: HashMap<View, OpNumber>,
    view: View,
    op_number: OpNumber
}

impl ViewTable {
    pub fn view(&self) -> View {
        self.view
    }

    pub fn op_number(&self) -> OpNumber {
        self.op_number
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