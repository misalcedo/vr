//! A Primary Copy Method to Support Highly-Available Distributed Systems.

mod client_table;
mod mail;
mod service;
mod status;

pub use service::Service;

mod backup;
mod configuration;
mod log;
mod primary;
mod protocol;
mod request;
mod role;
mod state;
mod viewstamp;
