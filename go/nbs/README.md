# Noms Block Store

A horizontally-scalable storage backend for Noms.

## Overview

NBS is a storage layer optimized for the needs of the [Noms](https://github.com/attic-labs/noms) database.

NBS can run in two configurations: either backed by local disk, or [backed by Amazon AWS](https://github.com/attic-labs/noms/blob/master/go/nbs/NBS-on-AWS.md).

When backed by local disk, NBS is significantly faster than LevelDB for our workloads and supports full multiprocess concurrency.

When backed by AWS, NBS stores its data mainly in S3, along with a single DynamoDB item. This configuration makes Noms "[effectively CA](https://research.google.com/pubs/pub45855.html)", in the sense that Noms is always consistent, and Noms+NBS is as available as DynamoDB and S3 are. This configuration also gives Noms the cost profile of S3 with power closer to that of a traditional database.

## Details

* NBS provides storage for a content-addressed DAG of nodes (with exactly one root), where each node is encoded as a sequence of bytes and addressed by a 20-byte hash of the byte-sequence.
* There is no `update` or `delete` -- only `insert`, `update root` and `garbage collect`.
* Insertion of any novel byte-sequence is durable only upon updating the root.
* File-level multiprocess concurrency is supported, with optimistic locking for multiple writers.
* Writers need not worry about re-writing duplicate chunks. NBS will efficiently detect and drop (most) duplicates.

## Perf

For the file back-end, perf is substantially better than LevelDB mainly because LDB spends substantial IO with the goal of keeping KV pairs in key-order which doesn't benenfit Noms at all. NBS locates related chunks together and thus reading data from a NBS store can be done quite alot faster. As an example, storing & retrieving a 1.1GB MP4 video file on a MBP i5 2.9Ghz:

 * LDB
   * Initial import: 44 MB/s, size on disk: 1.1 GB. 
   * Import exact same bytes: 35 MB/s, size on disk: 1.4 GB.
   * Export: 60 MB/s
 * NBS
   * Initial import: 72 MB/s, size on disk: 1.1 GB.
   * Import exact same bytes: 92 MB/s, size on disk: 1.1GB.
   * Export: 300 MB/s

## Status

NBS is more-or-less "beta". There's still [work we want to do](https://github.com/attic-labs/noms/issues?q=is%3Aopen+is%3Aissue+label%3ANBS), but it now works better than LevelDB for our purposes and so we have made it the default local backend for Noms:

```
# This uses nbs locally:
./csv-import foo.csv /Users/bob/csv-store::data
```

The AWS backend is available via the `aws:` scheme:

```
./csv-import foo.csv aws://table:bucket::data
```
