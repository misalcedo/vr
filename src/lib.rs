//! A Primary Copy Method to Support Highly-Available Distributed Systems.

use crate::group::{Event, Message};
use crate::order::ViewStamp;

mod network;
mod group;
mod order;


#[cfg(test)]
mod tests {
    use crate::group::{Cohort, Configuration, Group, GroupIdentifier, ModuleIdentifier};
    use crate::network::Network;
    use super::*;

    #[test]
    fn simulate() {
        let size = 3;
        let group = Group::new(Configuration::with_cohorts(size));
        let mut network = Network::default();
        let mut cohorts = Vec::with_capacity(size);

        for cohort in group.cohorts() {
            cohorts.push(Cohort::new(group.id(), cohort, network.bind(cohort).unwrap()));
        }
    }
}

