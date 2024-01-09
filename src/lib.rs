//! A Primary Copy Method to Support Highly-Available Distributed Systems.

use crate::group::{Event, Message};
use crate::order::ViewStamp;

mod network;
mod group;
mod order;




#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simulate() {

    }
}

