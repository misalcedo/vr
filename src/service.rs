pub trait Service {
    fn predict(&mut self, payload: &[u8]) -> Vec<u8>;

    fn invoke(&mut self, payload: &[u8], prediction: &[u8]) -> Vec<u8>;
}

impl<F> Service for F
where
    F: FnMut(&[u8]) -> Vec<u8>,
{
    fn predict(&mut self, _: &[u8]) -> Vec<u8> {
        Vec::new()
    }

    fn invoke(&mut self, payload: &[u8], _: &[u8]) -> Vec<u8> {
        self(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Service for usize {
        fn predict(&mut self, _: &[u8]) -> Vec<u8> {
            Vec::new()
        }

        fn invoke(&mut self, payload: &[u8], _: &[u8]) -> Vec<u8> {
            *self += payload.len();
            self.to_be_bytes().to_vec()
        }
    }
}
