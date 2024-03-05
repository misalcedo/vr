#[derive(Copy, Clone, Eq, PartialEq)]
pub enum Status {
    Normal,
    ViewChange,
    Recovering,
}
