#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct ClientIdentifier(u128);

impl Default for ClientIdentifier {
    fn default() -> Self {
        Self(uuid::Uuid::new_v4().as_u128())
    }
}

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct RequestIdentifier(u128);

impl Default for RequestIdentifier {
    fn default() -> Self {
        Self(uuid::Uuid::now_v7().as_u128())
    }
}

impl RequestIdentifier {
    pub fn next(&mut self) -> Self {
        self.0 += 1;
        *self
    }
}