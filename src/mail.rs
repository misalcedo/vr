use crate::protocol::{
    Commit, DoViewChange, GetState, NewState, Prepare, PrepareOk, Protocol, Recovery,
    RecoveryResponse, StartView, StartViewChange,
};
use crate::request::{ClientIdentifier, Reply};

pub trait Outbox<P>
where
    P: Protocol,
{
    fn prepare(&mut self, message: Prepare<P::Request, P::Prediction>);

    fn prepare_ok(&mut self, index: usize, message: PrepareOk);

    fn commit(&mut self, message: Commit);

    fn get_state(&mut self, index: usize, message: GetState);

    fn new_state(&mut self, index: usize, message: NewState<P::Request, P::Prediction>);

    fn start_view_change(&mut self, message: StartViewChange);

    fn do_view_change(&mut self, index: usize, message: DoViewChange<P::Request, P::Prediction>);

    fn start_view(&mut self, message: StartView<P::Request, P::Prediction>);

    fn recovery(&mut self, message: Recovery);

    fn recovery_response(
        &mut self,
        index: usize,
        message: RecoveryResponse<P::Request, P::Prediction>,
    );

    fn reply(&mut self, client: ClientIdentifier, reply: &Reply<P::Reply>);
}
