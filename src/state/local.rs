use crate::state::State;

#[derive(Debug, Default)]
pub struct LocalState<S> {
    state: S,
}

impl<S> LocalState<S> {
    pub fn new(state: S) -> Self {
        Self { state }
    }
}

impl<S: Clone> State<S> for LocalState<S> {
    fn load(&mut self) -> S {
        self.state.clone()
    }

    fn save(&mut self, state: S) {
        self.state = state;
    }
}
