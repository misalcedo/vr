mod local;

pub use local::LocalState;

pub trait State<S> {
    fn load(&mut self) -> S;

    fn save(&mut self, state: S);
}
