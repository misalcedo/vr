use serde::{Deserialize, Serialize};

pub trait Payload: Clone + Serialize + Deserialize<'static> {}

impl<P> Payload for P where P: Clone + Serialize + Deserialize<'static> {}

/// A trait to associate all the necessary types together.
/// All associated types must be serializable and not borrow data since replicas need to store these values.
pub trait Protocol {
    type Request: Payload;
    type Prediction: Payload;
    type Reply: Payload;
    type Checkpoint: Payload;
}

pub trait Service: Protocol + From<<Self as Protocol>::Checkpoint> {
    fn predict(&self, request: &<Self as Protocol>::Request) -> <Self as Protocol>::Prediction;

    fn checkpoint(&self) -> <Self as Protocol>::Checkpoint;

    fn invoke(
        &mut self,
        request: &<Self as Protocol>::Request,
        prediction: &<Self as Protocol>::Prediction,
    ) -> <Self as Protocol>::Reply;
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Protocol for i32 {
        type Request = Self;
        type Prediction = ();
        type Reply = Self;
        type Checkpoint = Self;
    }

    impl Service for i32 {
        fn predict(&self, _: &<Self as Protocol>::Request) -> <Self as Protocol>::Prediction {
            ()
        }

        fn checkpoint(&self) -> <Self as Protocol>::Checkpoint {
            *self
        }

        fn invoke(
            &mut self,
            request: &<Self as Protocol>::Request,
            _: &<Self as Protocol>::Prediction,
        ) -> <Self as Protocol>::Reply {
            *self += *request;
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
