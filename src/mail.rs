use crate::protocol::{
    Commit, DoViewChange, GetState, NewState, Prepare, PrepareOk, Recovery, RecoveryResponse,
    StartView, StartViewChange,
};
use crate::request::{ClientIdentifier, Reply};

pub trait Outbox {
    fn prepare(&mut self, message: Prepare);

    fn prepare_ok(&mut self, index: usize, message: PrepareOk);

    fn commit(&mut self, message: Commit);

    fn get_state(&mut self, index: usize, message: GetState);

    fn new_state(&mut self, index: usize, message: NewState);

    fn start_view_change(&mut self, message: StartViewChange);

    fn do_view_change(&mut self, index: usize, message: DoViewChange);

    fn start_view(&mut self, message: StartView);

    fn recovery(&mut self, message: Recovery);

    fn recovery_response(&mut self, index: usize, message: RecoveryResponse);

    fn reply(&mut self, client: ClientIdentifier, reply: &Reply);
}

pub trait Inbox {
    fn push_prepare(&mut self, message: Prepare);

    fn push_prepare_ok(&mut self, message: PrepareOk);

    fn push_commit(&mut self, message: Commit);

    fn push_get_state(&mut self, message: GetState);

    fn push_new_state(&mut self, message: NewState);

    fn push_start_view_change(&mut self, message: StartViewChange);

    fn push_do_view_change(&mut self, message: DoViewChange);

    fn push_start_view(&mut self, message: StartView);

    fn push_recovery(&mut self, message: Recovery);

    fn push_recovery_response(&mut self, message: RecoveryResponse);
}

pub trait Mailbox: Inbox + Outbox {}
