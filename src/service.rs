use bytes::Bytes;

pub trait Service {
    fn invoke(&mut self, request: Bytes) -> Bytes;
}
