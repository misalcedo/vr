//! A Primary Copy Method to Support Highly-Available Distributed Systems.

pub mod buffer;
mod client;
mod client_table;
mod configuration;
mod log;
mod mail;
mod nonce;
mod protocol;
mod replica;
mod request;
mod service;
mod status;
mod viewstamp;

pub use client::Client;
pub use configuration::Configuration;
pub use replica::Replica;
pub use service::{Protocol, Service};
