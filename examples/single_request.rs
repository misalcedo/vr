use viewstamped_replication::local::BufferedOutbox;
use viewstamped_replication::{Client, Configuration, Protocol, Replica, Service};

pub struct Adder(i32);

impl Protocol for Adder {
    type Request = i32;
    type Prediction = ();
    type Reply = i32;
    type Checkpoint = i32;
}

impl From<<Self as Protocol>::Checkpoint> for Adder {
    fn from(value: <Self as Protocol>::Checkpoint) -> Self {
        Adder(value)
    }
}

impl Service for Adder {
    fn predict(&self, _: &<Self as Protocol>::Request) -> <Self as Protocol>::Prediction {
        ()
    }

    fn checkpoint(&self) -> <Self as Protocol>::Checkpoint {
        self.0
    }

    fn invoke(
        &mut self,
        request: &<Self as Protocol>::Request,
        _: &<Self as Protocol>::Prediction,
    ) -> <Self as Protocol>::Reply {
        self.0 += *request;
        self.0
    }
}

fn main() {
    let configuration = Configuration::from(3);
    let checkpoint = 0;

    let mut client = Client::new(configuration);

    let mut primary = Replica::new(configuration, 0, Adder::from(checkpoint));
    let mut backup1 = Replica::new(configuration, 1, Adder::from(checkpoint));
    let mut backup2 = Replica::new(configuration, 2, Adder::from(checkpoint));

    let mut outbox = BufferedOutbox::default();

    let delta = 1;
    let request = client.new_request(delta);

    primary.handle_request(request, &mut outbox);

    let mut messages = Vec::from_iter(outbox.drain_broadcast());
    let prepare = messages.pop().unwrap().unwrap_prepare();

    assert!(messages.is_empty());

    backup1.handle_prepare(prepare.clone(), &mut outbox);
    backup2.handle_prepare(prepare.clone(), &mut outbox);

    let mut messages = Vec::from_iter(outbox.drain_send());
    let _ = messages.pop().unwrap().payload.unwrap_prepare_ok();
    let prepare_ok2 = messages.pop().unwrap().payload.unwrap_prepare_ok();

    assert!(messages.is_empty());

    primary.handle_prepare_ok(prepare_ok2, &mut outbox);

    let mut replies = Vec::from_iter(outbox.drain_replies());
    let reply = replies.pop().unwrap();

    assert!(outbox.is_empty());
    assert_eq!(reply.destination, client.identifier());
    assert_eq!(reply.payload.payload, delta);
    assert_eq!(reply.payload.view, primary.view());
    assert_eq!(Some(reply.payload.id), client.last_request());
}
