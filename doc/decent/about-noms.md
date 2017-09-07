# About Noms

[Noms](http://noms.io) is a database for decentralized
applications. It stores structured data (ints, strings, blobs, maps,
lists, structs, etc) and like most databases it features atomic
transactions, efficient searches, scans, reads, and updates.

Unlike any other database, Noms is built for the decentralized
web. Any number of dapp peers can concurrently modify their own copies
of the same Noms database and continuously sync their changes with
each other. In this sense noms works like git: changes are bundled as
commits which reference previous states of the database. Apps pull
changes from peers and merge them using a principled set of APIs and
strategies. Except that rather than users manually pulling and
merging, applications typically do this continuously, automatically
converging to a shared state.

Noms stores data in the blockstore of your choice. For example you
could back Noms with a decentralized blockstore like IPFS. In this
configuration, read and write load is spread throughout the
network. If for some reason you wanted to sync to a centralized 
service you could do that to (for example, S3).

Your application uses a [Go client library](https://github.com/attic-labs/noms/blob/master/doc/go-tour.md) to interact with Noms
data. There is also a command-line interface for working with data and
initial support for a GraphQL-based query language.

Some additional features include:
* **Versioning**: noms is git-like, so it’s easy to use, compare, or revert to older database versions
* **Efficient diffs**: diffing even huge datasets is efficient due to
  noms’ use of a novel BTree-like data structure called a [Prolly
  Tree](../intro.md#prolly-trees-probabilistic-b-trees)
* **Efficient storage**: data are chunked and content-addressable, so
  there is exactly one copy of each chunk in the database, shared by
  other data that reference it. Small changes to massive data
  structures always result in small operations.
* **Verifiable**: The entire database rolls up to a single 20-byte hash
 that uniquely represents the database at that moment - anyone can
 verify that a particular database hashes to the same value

Read the [Noms design overview](../intro.md).
