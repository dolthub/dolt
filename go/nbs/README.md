# Noms Block Store
An experimental storage layer for [noms](https://github.com/attic-labs/noms).

- Provides storage for a content-addressed DAG of nodes (with exactly one root), where each node is encoded as a sequence of bytes and addressed by a 20-byte hash of the byte-sequence.
- There is no `update` or `delete`, only `insert`, `update root` and `garbage collect`.
- Insertion of any novel byte-sequence is durable only upon updating the root.
- File-level multiprocess concurrency is supported, with optimistic locking for multiple writers.
- Writers need not worry about re-writing duplicate chunks. NBS will efficiently detect and drop (most) duplicates.

# Status
NBS is more-or-less "alpha". There's still [work we want to do](https://github.com/attic-labs/noms/issues?q=is%3Aopen+is%3Aissue+label%3ANBS), but it basically works. The most obvious things that aren't implemented are Compaction and GC. In particular, there's currently no upper bound on the set of "tables" which comprise a store and no way to reduce the number.

There are two full back-ends for nbs, one for storage on a file-system and one for storage in AWS. The later requires a dynamo table and an s3 bucket.

For the file back-end, perf is substantially better than LevelDB for two reasons (1) LDB does quite alot of compaction which burns IO but doesn't benenfit noms at all. (2) NBS locates related chunks together and thus reading data from a NBS store can be done quite alot faster. As an example, storing & retrieving a 1.1GB MP4 video file on a MBP i5 2.9Ghz:

 * LDB
   * Initial import: 44 MB/s, size on disk: 1.1 GB. 
   * Import exact same bytes: 35 MB/s, size on disk: 1.4 GB.
   * Export: 60 MB/s
 * NBS
   * Initial import: 72 MB/s, size on disk: 1.1 GB.
   * Import exact same bytes: 92 MB/s, size on disk: 1.1GB.
   * Export: 300 MB/s

NBS is currently available via the `nbs:` scheme. I.e. `./csv-import foo.csv nbs:/Users/bob/csv-store::data`. When nbs is deemed stable, we will likely make it the default for file system storage.
