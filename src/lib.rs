//! A Primary Copy Method to Support Highly-Available Distributed Systems.

pub mod client;
mod client_table;
pub mod driver;
pub mod health;
pub mod identifiers;
mod mailbox;
pub mod model;
pub mod replica;
mod service;
mod stamps;
mod state;

pub use service::Service;
pub use state::State;

mod request;
mod protocol;
