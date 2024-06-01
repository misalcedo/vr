//! A Primary Copy Method to Support Highly-Available Distributed Systems.

mod client;
mod configuration;
mod mail;
pub mod message;
mod replica;
mod table;

pub use configuration::Configuration;
pub use mail::Mailbox;
pub use replica::Replica;
