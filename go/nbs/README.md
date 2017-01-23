# Noms Block Store
An experimental storage layer for [noms](https://github.com/attic-labs/noms).

- Provides storage for a content-addressed DAG of nodes (with exactly one root), where each node is encoded as a sequence of bytes and addressed by a 20-byte hash of the byte-sequence.
- There is no `update` or `delete`, only `insert`, `update root` and `garbage collect`.
- Insertion of any novel byte-sequence is durable only upon updating the root.
- File-level multiprocess concurrency is supported, with optimistic locking for multiple writers.
- Writers need not worry about re-writing duplicate chunks. NBS will efficiently detect and drop (most) duplicates.

# Status
NBS is more-or-less "alpha". There's still work we want to do, but it basically works. The most obvious things that aren't implemented are Compaction and GC. In particular, there's currently no upper bound on the set of "tables" which comprise a store and now way to reduce the number.

However, there are two full "back-ends", one for storage on a file-system and one for storage in AWS. The later requires a dynamo table and an s3 bucket.

For the file back-end, perf is substantially better than LevelDB for two reasons (1) LDB does quite alot of compaction which burns IO but doesn't benenfit noms at all. (2) NBS locates related chunks together and thus reading data from a NBS store can be done quite alot faster (on a current Macbook Pro, you can read large blobs at 100s of MB/s, whereas LDB tops out at about 75 MB/s).
