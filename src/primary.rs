use crate::client_table::ClientTable;
use crate::configuration::Configuration;
use crate::log::Log;
use crate::status::Status;
use crate::viewstamp::View;

pub struct Primary<Request, Response, Prediction> {
    configuration: Configuration,
    replica_number: usize,
    view_number: View,
    status: Status,
    log: Log<Request, Prediction>,
    committed: usize,
    client_table: ClientTable<Response>,
}
