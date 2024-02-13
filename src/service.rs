pub trait Service {
    type Request;
    type Prediction;
    type Reply;

    fn predict(&mut self, request: &Self::Request) -> Self::Prediction;

    fn invoke(&mut self, request: &Self::Request, prediction: &Self::Prediction) -> Self::Reply;
}
