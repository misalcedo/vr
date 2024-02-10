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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stamps::View;

    #[test]
    fn views() {
        let view = View::default();
        let mut state: LocalState<View> = LocalState::default();

        assert_eq!(state.load(), view);

        state.save(view.next());

        assert!(state.load() > view);
    }
}
