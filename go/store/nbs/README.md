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

## Storage locking

Every file-backed NBS store takes exactly one exclusive advisory file lock:

```
<database dir>/.dolt/noms/LOCK
```

The lock is taken through [`github.com/dolthub/file-locks`](https://github.com/dolthub/file-locks), which uses `flock(2)` on unix and `LockFileEx` on Windows. It is *advisory*: it only excludes processes that ask for the same lock, and it carries no information a non-locking process can read.

### Lifetime

`newJournalLock` takes the lock when a store is opened, and `journalManifest.Close` (reached from `NomsBlockStore.Close`) releases it. There is no maximum hold time and it is not a critical section -- a `dolt sql-server` holds the lock for its whole lifetime, because that is the lifetime of the store it opened. The correct thing to wait for is therefore "the holding process has exited or closed its store", not a fixed timeout.

### Acquire timeout and the read-only fallback

Dolt waits only `lockFileTimeout`, 100ms (`file_manifest.go`), before giving up. What happens next depends on the caller:

* By default the store opens **read-only**. Writes then fail with a read-only error, which is why a restart that races the previous server's release yields a server that accepts connections but rejects writes, rather than an error naming the lock.
* Embedded-driver callers can pass `fail_on_journal_lock_timeout` to get `ErrDatabaseLocked` ("the database is locked by another dolt process") instead of a silent read-only open.
* `skip_journal_lock_timeout` collapses the wait to a single non-blocking attempt.

### Crash and staleness

`flock` locks belong to the open file rather than the process, and the kernel releases them when the process dies -- including on `kill -9`. A hard crash does **not** leave a stale lock behind, and there is no operator step needed to clear one.

The `LOCK` file itself, however, always remains: it is created on open and deliberately left in place on close. Its presence says nothing about lock state -- only holding the flock does.

Do not delete the `LOCK` file to "clear" a lock. A lock held on an unlinked file excludes nobody, so removing the file while a process holds the lock silently breaks mutual exclusion.

### External waiters

An external process can take the same `flock`, but this is not a supported way to coordinate. Dolt does not publish its lock state anywhere and does not watch the file, so an external flock cannot tell a supervisor *when* a database was released -- it only changes whether Dolt's own next open succeeds, and there is no supported query for who holds it.

To coordinate a restart, wait on the previous process itself: make sure it has exited or closed its store before starting the next one.

### Caveats

These guarantees come from the underlying OS primitive and are advisory: over NFS or SMB they are only as good as the mount, so keep database directories on local storage. Waiters are not queued, and a `Lock` is not recursive.

## Perf

For the file back-end, perf is substantially better than LevelDB mainly because LDB spends substantial IO with the goal of keeping KV pairs in key-order which doesn't benefit Noms at all. NBS locates related chunks together and thus reading data from an NBS store can be done quite a lot faster. As an example, storing & retrieving a 1.1GB MP4 video file on a MBP i5 2.9Ghz:

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

```shell
# This uses nbs locally:
./csv-import foo.csv /Users/bob/csv-store::data
```

The AWS backend is available via the `aws:` scheme:

```shell
./csv-import foo.csv aws://[table:bucket]::data
```
