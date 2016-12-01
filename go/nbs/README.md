# Noms Block Store
An experimental storage layer for [noms](https://github.com/attic-labs/noms).

- Provides storage for a content-addressed DAG of nodes (with exactly one root), where each node is encoded as a sequence of bytes and addressed by a 20-byte hash of the byte-sequence.
- There is no `update` or `delete`, only `insert`, `update root` and `garbage collect`.
- Insertion of any novel byte-sequence is durable only upon updating the root.
- File-level multiprocess concurrency is supported, with optimistic locking for multiple writers.
- Writers need not worry about re-writing duplicate chunks. NBS will efficiently detect and drop (most) duplicates.
