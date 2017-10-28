# Badger [![GoDoc](https://godoc.org/github.com/dgraph-io/badger?status.svg)](https://godoc.org/github.com/dgraph-io/badger) [![Go Report Card](https://goreportcard.com/badge/github.com/dgraph-io/badger)](https://goreportcard.com/report/github.com/dgraph-io/badger) [![Build Status](https://travis-ci.org/dgraph-io/badger.svg?branch=master)](https://travis-ci.org/dgraph-io/badger) ![Appveyor](https://ci.appveyor.com/api/projects/status/github/dgraph-io/badger?branch=master&svg=true) [![Coverage Status](https://coveralls.io/repos/github/dgraph-io/badger/badge.svg?branch=master)](https://coveralls.io/github/dgraph-io/badger?branch=master)

An embeddable, persistent, simple and fast key-value (KV) store, written purely in Go. It's meant to be a performant alternative to non Go based key-value stores like [RocksDB](https://github.com/facebook/rocksdb).

![Badger sketch](/images/sketch.jpg)

## About

Badger is written out of frustration with existing KV stores which are either written in pure Go and slow, or fast but require usage of Cgo.
Badger aims to provide an equal or better speed compared to industry leading KV stores (like RocksDB), while maintaining the entire code base in pure Go.

## Related Blog Posts

1. [Introducing Badger: A fast key-value store written natively in Go](https://open.dgraph.io/post/badger/)
2. [Make Badger crash resilient with ALICE](https://blog.dgraph.io/post/alice/)

## Video Tutorials

- [Getting Started](https://www.youtube.com/watch?v=XBKq39caRZ8) by [1lann](https://github.com/1lann)

## Installation and Usage

`go get -v github.com/dgraph-io/badger`

If you want to run tests, also get testing dependencies by passing in `-t` flag.

`go get -t -v github.com/dgraph-io/badger`

From here, follow [docs](https://godoc.org/github.com/dgraph-io/badger) for usage.

## Documentation

Badger documentation is located at [godoc.org](https://godoc.org/github.com/dgraph-io/badger).

## Design Goals

Badger has these design goals in mind:

- Write it purely in Go.
- Use latest research to build the fastest KV store for data sets spanning terabytes.
- Keep it simple, stupid. No support for transactions, versioning or snapshots -- anything that can be done outside of the store should be done outside.
- Optimize for SSDs (more below).

### Non-Goals

- Try to be a database.

## Users

Badger is currently being used by [Dgraph](https://github.com/dgraph-io/dgraph).

*If you're using Badger in a project, let us know.*

## Design

Badger is based on [WiscKey paper by University of Wisconsin, Madison](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf).

In simplest terms, keys are stored in LSM trees along with pointers to values, which would be stored in write-ahead log files, aka value logs.
Keys would typically be kept in RAM, values would be served directly off SSD.

### Optimizations for SSD

SSDs are best at doing serial writes (like HDD) and random reads (unlike HDD).
Each write reduces the lifecycle of an SSD, so Badger aims to reduce the write amplification of a typical LSM tree.

It achieves this by separating the keys from values. Keys tend to be smaller in size and are stored in the LSM tree.
Values (which tend to be larger in size) are stored in value logs, which also double as write ahead logs for fault tolerance.

Only a pointer to the value is stored along with the key, which significantly reduces the size of each KV pair in LSM tree.
This allows storing lot more KV pairs per table. For e.g., a table of size 64MB can store 2 million KV pairs assuming an average key size of 16 bytes, and a value pointer of 16 bytes (with prefix diffing in Badger, the average key sizes stored in a table would be lower).
Thus, lesser compactions are required to achieve stability for the LSM tree, which results in fewer writes (all writes being serial).

It might be a good idea on ext4 to periodically invoke `fstrim` in case the file system [does not quickly reuse space from deleted files](http://www.ogris.de/blkalloc/).

### Nature of LSM trees

Because only keys (and value pointers) are stored in LSM tree, Badger generates much smaller LSM trees.
Even for huge datasets, these smaller trees can fit nicely in RAM allowing for lot quicker accesses to and iteration through keys.
For random gets, keys can be quickly looked up from within RAM, giving access to the value pointer.
Then only a single pointed read from SSD (random read) is done to retrieve value.
This improves random get performance significantly compared to traditional LSM tree design used by other KV stores.

## Comparisons

| Feature             | Badger                                       | RocksDB                       | BoltDB    |
| -------             | ------                                       | -------                       | ------    |
| Design              | LSM tree with value log                      | LSM tree only                 | B+ tree   |
| High RW Performance | Yes                                          | Yes                           | No        |
| Designed for SSDs   | Yes (with latest research <sup>1</sup>)      | Not specifically <sup>2</sup> | No        |
| Embeddable          | Yes                                          | Yes                           | Yes       |
| Sorted KV access    | Yes                                          | Yes                           | Yes       |
| Pure Go (no Cgo)    | Yes                                          | No                            | Yes       |
| Transactions        | No (but provides compare and set operations) | Yes (but non-ACID)            | Yes, ACID |
| Snapshots           | No                                           | Yes                           | Yes       |

<sup>1</sup> Badger is based on a paper called [WiscKey by University of Wisconsin, Madison](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf), which saw big wins with separating values from keys, significantly reducing the write amplification compared to a typical LSM tree.

<sup>2</sup> RocksDB is an SSD optimized version of LevelDB, which was designed specifically for rotating disks.
As such RocksDB's design isn't aimed at SSDs.

## Benchmarks

### RocksDB ([Link to blog post](https://blog.dgraph.io/post/badger/))

![RocksDB Benchmarks](/images/benchmarks-rocksdb.png)

## Crash Consistency

Badger is crash resilient. Any update which was applied successfully before a crash, would be available after the crash.
Badger achieves this via its value log.

Badger's value log is a write-ahead log (WAL). Every update to Badger is written to this log first, before being applied to the LSM tree.
Badger maintains a monotonically increasing pointer (head) in the LSM tree, pointing to the last update offset in the value log.
As and when LSM table is persisted, the head gets persisted along with.
Thus, the head always points to the latest persisted offset in the value log.
Every time Badger opens the directory, it would first replay the updates after the head in order, bringing the updates back into the LSM tree; before it allows any reads or writes.
This technique ensures data persistence in face of crashes.

Furthermore, Badger can be run with `SyncWrites` option, which would open the WAL with O_DSYNC flag, hence syncing the writes to disk on every write.

## Frequently Asked Questions

- **My writes are really slow. Why?**

You're probably doing writes serially, using `Set`. To get the best write performance, use `BatchSet`, and call it
concurrently from multiple goroutines.

- **I don't see any disk write. Why?**

If you're using Badger with `SyncWrites=false`, then your writes might not be written to value log
and won't get synced to disk immediately. Writes to LSM tree are done inmemory first, before they
get compacted to disk. The compaction would only happen once `MaxTableSize` has been reached. So, if
you're doing a few writes and then checking, you might not see anything on disk. Once you `Close`
the store, you'll see these writes on disk.

- **Which instances should I use for Badger?**

We recommend using instances which provide local SSD storage, without any limit
on the maximum IOPS. In AWS, these are storage optimized instances like i3. They
provide local SSDs which clock 100K IOPS over 4KB blocks easily.

- **Are there any Go specific settings that I should use?**

We *highly* recommend setting a high number for GOMAXPROCS, which allows Go to
observe the full IOPS throughput provided by modern SSDs. In Dgraph, we have set
it to 128. For more details, [see this
thread](https://groups.google.com/d/topic/golang-nuts/jPb_h3TvlKE/discussion).

## Contact
- Please use [discuss.dgraph.io](https://discuss.dgraph.io) for questions, feature requests and discussions.
- Please use [Github issue tracker](https://github.com/dgraph-io/badger/issues) for filing bugs or feature requests.
- Join [![Slack Status](http://slack.dgraph.io/badge.svg)](http://slack.dgraph.io).
- Follow us on Twitter [@dgraphlabs](https://twitter.com/dgraphlabs).
