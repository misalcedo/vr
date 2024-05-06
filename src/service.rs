use crate::request::{CustomPayload, Payload};

pub trait Service {
    fn restore(payload: &Payload) -> Self;

    fn predict(&self, request: &Payload) -> Payload;

    fn checkpoint(&self) -> Payload;

    fn invoke(&mut self, request: &Payload, prediction: &Payload) -> Payload;
}

pub trait DataService {
    type Request: CustomPayload;
    type Prediction: CustomPayload;
    type Checkpoint: CustomPayload;
    type Reply: CustomPayload;

    fn restore(checkpoint: Self::Checkpoint) -> Self;

    fn predict(&self, request: Self::Request) -> Self::Prediction;

    fn checkpoint(&self) -> Self::Checkpoint;

    fn invoke(&mut self, request: Self::Request, prediction: Self::Prediction) -> Self::Reply;
}

impl<S> Service for S
where
    S: DataService,
{
    fn restore(payload: &Payload) -> Self {
        S::restore(S::Checkpoint::from_payload(payload))
    }

    fn predict(&self, request: &Payload) -> Payload {
        self.predict(S::Request::from_payload(request)).to_payload()
    }

    fn checkpoint(&self) -> Payload {
        self.checkpoint().to_payload()
    }

    fn invoke(&mut self, request: &Payload, prediction: &Payload) -> Payload {
        self.invoke(
            S::Request::from_payload(request),
            S::Prediction::from_payload(prediction),
        )
        .to_payload()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::CustomPayload;

    impl CustomPayload for i32 {
        fn to_payload(&self) -> Payload {
            Payload::from(self.to_be_bytes())
        }

        fn from_payload(payload: Payload) -> Self {
            Self::from_be_bytes(payload.try_into().unwrap_or_default())
        }
    }

    impl DataService for i32 {
        type Request = Self;
        type Prediction = ();
        type Checkpoint = Self;
        type Reply = Self;

        fn restore(checkpoint: Self::Checkpoint) -> Self {
            checkpoint
        }

        fn predict(&self, _: Self::Request) -> Self::Prediction {
            ()
        }

        fn checkpoint(&self) -> Self::Checkpoint {
            *self
        }

        fn invoke(&mut self, request: Self::Request, _: Self::Prediction) -> Self::Reply {
            *self += request;
            *self
        }
    }

    #[test]
    fn adder() {
        let mut service = 0;

        assert_eq!(service.predict(&42), ());
        assert_eq!(service.checkpoint(), service);
        assert_eq!(service.invoke(&45, &()), 45);
        assert_eq!(service.invoke(&-3, &()), 42);
        assert_eq!(service.checkpoint(), service);
        assert_eq!(service.checkpoint(), 42);
    }
}
