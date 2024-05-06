use viewstamped_replication::buffer::BufferedMailbox;
use viewstamped_replication::{
    Client, Configuration, CustomPayload, DataService, Payload, Replica,
};

pub struct Adder(i32);

#[derive(Copy, Clone, Debug)]
pub struct Amount(i32);

impl CustomPayload for Amount {
    fn to_payload(&self) -> Payload {
        Payload::from(self.0.to_be_bytes())
    }

    fn from_payload(payload: &Payload) -> Self {
        Self(i32::from_be_bytes(
            payload.try_into().expect("invalid payload"),
        ))
    }
}

impl DataService for Adder {
    type Request = Amount;
    type Prediction = ();
    type Checkpoint = Amount;
    type Reply = Amount;

    fn restore(checkpoint: Self::Checkpoint) -> Self {
        Self(checkpoint.0)
    }

    fn predict(&self, _: Self::Request) -> Self::Prediction {
        ()
    }

    fn checkpoint(&self) -> Self::Checkpoint {
        Amount(self.0)
    }

    fn invoke(&mut self, request: Self::Request, _: Self::Prediction) -> Self::Reply {
        self.0 += request.0;
        Amount(self.0)
    }
}

fn main() {
    let configuration = Configuration::from(3);
    let checkpoint = Amount(0);

    let mut client = Client::new(configuration);

    let mut primary = Replica::new(configuration, 0, Adder::restore(checkpoint));
    let mut backup1 = Replica::new(configuration, 1, Adder::restore(checkpoint));
    let mut backup2 = Replica::new(configuration, 2, Adder::restore(checkpoint));

    let mut mailbox = BufferedMailbox::default();

    let delta = Amount(1);
    let request = client.new_request(delta);

    primary.handle_request(request.clone(), &mut mailbox);

    let mut messages = Vec::from_iter(mailbox.drain_broadcast());
    let prepare = messages.pop().unwrap().unwrap_prepare();

    assert!(messages.is_empty());

    backup1.handle_prepare(prepare.clone(), &mut mailbox);
    backup2.handle_prepare(prepare.clone(), &mut mailbox);

    let mut messages = Vec::from_iter(mailbox.drain_send());
    let _ = messages.pop().unwrap().payload.unwrap_prepare_ok();
    let prepare_ok2 = messages.pop().unwrap().payload.unwrap_prepare_ok();

    assert!(messages.is_empty());

    primary.handle_prepare_ok(prepare_ok2, &mut mailbox);

    let mut replies = Vec::from_iter(mailbox.drain_replies());
    let reply = replies.pop().unwrap();

    assert!(mailbox.is_empty());
    assert_eq!(reply.destination, request.client);
    assert_eq!(reply.payload.payload, delta.to_payload());
    assert_eq!(reply.payload.view, primary.view());
    assert_eq!(reply.payload.id, request.id);
}
