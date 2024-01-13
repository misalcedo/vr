# Viewstamped Replication
A Rust-based implementation of the viewstamped replication consensus protocol.

## Concurrency
The basic protocol assumes a single concurrent request per client.
Client applications can achieve higher concurrency by utilizing multiple client identifiers for a single client application instance. 

## Links
- [Viewstamped Replication](https://pmg.csail.mit.edu/papers/vr.pdf)
- [From Viewstamped Replication to Byzantine Fault Tolerance](https://pmg.csail.mit.edu/papers/vr-to-bft.pdf)
- [Viewstamped Replication Revisited](https://pmg.csail.mit.edu/papers/vr-revisited.pdf)