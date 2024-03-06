# Viewstamped Replication

A Rust-based implementation of the viewstamped replication consensus protocol.

## Concurrency

The basic protocol assumes a single concurrent request per client.
Client applications can achieve higher concurrency by utilizing multiple client identifiers for a single client
application instance.

## Non-determinism

Supports non-determinism by querying the service from the primary for a predicted value and passing that value on
prepare to all replicas.
Currently, there is no support for other solutions mentioned in the literature such as sending a predict message,
waiting for `f` responses and deterministically merging the values.

## Log Compaction

Supports log compaction by regularly taking checkpoints of the service state to be durably stored.
A service must be restore-able from a checkpoint.
Once a large enough suffix of checkpoints exists, the log may be compacted to remove all operations included in the last
checkpoint before the suffix.

For example, imagine a configuration that takes a checkpoint every 5 minutes and keeps the last 3 checkpoints.
The log will be compacted on the 4th checkpoint and any operations whose application state is reflected in 1st
checkpoint will be removed from the log.

## Simulation

A simulation of the protocol using async tasks and channels is included in the examples.

```console
cargo run --example simulation
```

## State Transfers

- The protocol does not state what to do when a replica receives a `GetState` message for a newer operation than is in
  its log. For now, we drop the message.

## TODOs

- Define mechanism for recovering replicas to fetch configuration upon receiving a protocol message.
- Support an optional pre-step for non-determinism that fetches predictions from `f` backups.
- Support stale read-only requests on backups.
- Support for configuration changes.
- Support for networked communication.
- Support fetching the checkpoints from other replicas when recovering.
- Support copy-on-write semantics in log compaction to reduce the cost of checkpoints.
- Make non-determinism and checkpointing optional for services to implement.
- Evicting client table to limit memory usage.
- Support dropped messages.

## Links

- [Viewstamped Replication](https://pmg.csail.mit.edu/papers/vr.pdf)
- [From Viewstamped Replication to Byzantine Fault Tolerance](https://pmg.csail.mit.edu/papers/vr-to-bft.pdf)
- [Viewstamped Replication Revisited](https://pmg.csail.mit.edu/papers/vr-revisited.pdf)
- [UUID RFC](https://www.ietf.org/archive/id/draft-peabody-dispatch-new-uuid-format-04.html)
