use crate::identifiers::ReplicaIdentifier;
use crate::replica::Status;
use crate::stamps::View;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NonVolatileState {
    identifier: ReplicaIdentifier,
    latest_view: Option<View>,
}

impl From<ReplicaIdentifier> for NonVolatileState {
    fn from(identifier: ReplicaIdentifier) -> Self {
        Self {
            identifier,
            latest_view: None,
        }
    }
}

impl NonVolatileState {
    pub fn new(identifier: ReplicaIdentifier, view: View) -> Self {
        Self {
            identifier,
            latest_view: Some(view),
        }
    }

    pub fn identifier(&self) -> ReplicaIdentifier {
        self.identifier
    }

    pub fn view(&self) -> View {
        match self.latest_view {
            None => View::default(),
            Some(view) if self.identifier == self.identifier.primary(view) => view.next(),
            Some(view) => view,
        }
    }

    pub fn status(&self) -> Status {
        match self.latest_view {
            None => Status::Normal,
            Some(_) => Status::Recovering,
        }
    }
}
