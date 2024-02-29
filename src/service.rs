use crate::protocol::Protocol;

pub trait Service<P>: From<P::Checkpoint>
where
    P: Protocol,
{
    fn predict(&self, request: &P::Request) -> P::Prediction;

    fn checkpoint(&self) -> P::Checkpoint;

    fn invoke(&mut self, request: &P::Request, prediction: &P::Prediction) -> P::Reply;
}
