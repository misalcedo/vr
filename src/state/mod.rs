mod local;

pub use local::LocalState;

// TODO: Avoid Non-volatile Storage
pub trait State<S> {
    fn load(&mut self) -> S;

    fn save(&mut self, state: S);
}
