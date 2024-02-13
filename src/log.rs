use std::ops::Index;
use crate::request::Request;
use crate::stamps::OpNumber;

pub struct Entry<R, P> {
    request: Request<R>,
    prediction: P,
}

pub struct Log<R, P> {
    entries: Vec<Entry<R, P>>
}
