use crate::protocol::Protocol;

pub trait Service<P>: From<P::Checkpoint>
where
    P: Protocol,
{
    fn predict(&self, request: &P::Request) -> P::Prediction;

    fn checkpoint(&self) -> P::Checkpoint;

    fn invoke(&mut self, request: &P::Request, prediction: &P::Prediction) -> P::Reply;
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

    impl Service<i32> for i32 {
        fn predict(&self, request: &<i32 as Protocol>::Request) -> <i32 as Protocol>::Prediction {
            ()
        }

        fn checkpoint(&self) -> <i32 as Protocol>::Checkpoint {
            *self
        }

        fn invoke(
            &mut self,
            request: &<i32 as Protocol>::Request,
            prediction: &<i32 as Protocol>::Prediction,
        ) -> <i32 as Protocol>::Reply {
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
