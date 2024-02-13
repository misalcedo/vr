//! A Primary Copy Method to Support Highly-Available Distributed Systems.

mod client_table;
mod service;
mod state;
mod status;

pub use service::Service;
pub use state::State;

mod configuration;
mod log;
mod primary;
mod protocol;
mod request;
mod viewstamp;
