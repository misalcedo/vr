use crate::protocol::Protocol;

pub trait Service<P>: From<P::Checkpoint>
where
    P: Protocol,
{
    fn predict(&mut self, request: &P::Request) -> P::Prediction;

    fn checkpoint(&mut self) -> P::Checkpoint;

    fn invoke(&mut self, request: &P::Request, prediction: &P::Prediction) -> P::Reply;
}
