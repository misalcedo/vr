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

## TODOs

- Define mechanism for recovering replicas to fetch configuration upon receiving a protocol message.
- Support an optional pre-step for non-determinism that fetches predictions from `f` backups.
- Support stale read-only requests on backups.
- Support for configuration changes.
- Support for networked communication.
- Log compaction.

## Links

- [Viewstamped Replication](https://pmg.csail.mit.edu/papers/vr.pdf)
- [From Viewstamped Replication to Byzantine Fault Tolerance](https://pmg.csail.mit.edu/papers/vr-to-bft.pdf)
- [Viewstamped Replication Revisited](https://pmg.csail.mit.edu/papers/vr-revisited.pdf)
- [UUID RFC](https://www.ietf.org/archive/id/draft-peabody-dispatch-new-uuid-format-04.html)
