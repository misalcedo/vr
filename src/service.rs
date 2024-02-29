use serde::{Deserialize, Serialize};

pub trait Service<'a>: From<Self::Checkpoint> {
    type Request: Clone + Serialize + Deserialize<'a>;
    type Prediction: Clone + Serialize + Deserialize<'a>;
    type Reply: Clone + Serialize + Deserialize<'a>;
    type Checkpoint: Clone + Serialize + Deserialize<'a>;

    fn predict(&mut self, request: &Self::Request) -> Self::Prediction;

    fn checkpoint(&mut self) -> Self::Checkpoint;

    fn invoke(&mut self, request: &Self::Request, prediction: &Self::Prediction) -> Self::Reply;
}
