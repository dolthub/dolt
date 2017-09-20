**Hosted Use Case:** [About](about.md)

<br><br>
[![Build Status](http://jenkins3.noms.io/buildStatus/icon?job=NomsMasterBuilder)](http://jenkins3.noms.io/job/NomsMasterBuilder/)
[![codecov](https://codecov.io/gh/attic-labs/noms/branch/master/graph/badge.svg)](https://codecov.io/gh/attic-labs/noms)
[![GoDoc](https://godoc.org/github.com/attic-labs/noms?status.svg)](https://godoc.org/github.com/attic-labs/noms)
[![Slack](http://slack.noms.io/badge.svg)](http://slack.noms.io)

# Noms -- The OLAP Database in a Bucket

Hosting [Noms](http://noms.io) in the cloud allows you to deploy a large,
horizontally-scalable [OLAP](http://olap.com/olap-definition/) database on top
of cheap storage, only paying for compute resources when you want to run
queries.

Like most OLAP databases, Noms offers support for large-scale tabular data,
atomic transactions, and efficient searches, scans and reads. Unlike most
other such databases, Noms also provides a rich data model (maps, sets, lists,
structs), efficient versioned updates, built-in sync, and the ability to
perform diff-based incremental transformations as new data flows through the
system.

## How it Works

Think of Noms like a programmable Git: changes are bundled as commits which
reference previous states of the database. As new raw data is "checked in" to
the database, the Noms client libraries make it easy to run *just* the novel
bytes through your custom ingestion logic. You can run normalization and
cleanup code, again against only new entries, and the fact that Noms has
built-in versioning makes it so you can trivially roll back to the original,
untouched data and try again if anything goes awry.

These features all derive from way Noms stores data. All data in a Noms DB is
chunked, content-addressable, and organized into a novel BTree-like data
structure called a [Prolly Tree](../intro.md#prolly-trees-probabilistic-b-trees).

This storage format implies that
* there is exactly one copy of each chunk in the database, shared by all data
 structures that reference it,
* the entire database rolls up to a single 20-byte hash
 that uniquely represents the database at that moment - anyone can
 verify that a particular database hashes to the same value, and
* it's easy to use, diff against, or revert to older database versions by
 referencing the appropriate hash.

Your ingestion pipeline uses a [Go client library](https://github.com/attic-labs/noms/blob/master/doc/go-tour.md) to interact with Noms data. There is
also a [command-line interface](https://github.com/attic-labs/noms/blob/master/doc/cli-tour.md) for working with data and initial
support for a [GraphQL-based query language](https://github.com/attic-labs/noms/blob/master/go/ngql/README.md) and querying with Presto.

Read the [Noms design overview](../intro.md).

**_If you’d like to use noms in your project we’d love to hear from you_**:
drop us an email ([noms@attic.io](mailto:noms@attic.io)) or send us a
message in slack ([slack.noms.io](http://slack.noms.io)).
