pub trait Service {
    fn invoke(&mut self, payload: &[u8]) -> Vec<u8>;
}

impl<F> Service for F where F: FnMut(&[u8]) -> Vec<u8> {
    fn invoke(&mut self, payload: &[u8]) -> Vec<u8> {
        self(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Service for usize {
        fn invoke(&mut self, payload: &[u8]) -> Vec<u8> {
            *self += payload.len();
            self.to_be_bytes().to_vec()
        }
    }
}