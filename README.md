#Store All the Things

*Noms* is a content-addressed, immutable, decentralized, strongly-typed database.

In other words, Noms is Git for data.

## Setup

1. Install [Go 1.6+](https://golang.org/dl/)
2. Ensure your [$GOPATH](https://github.com/golang/go/wiki/GOPATH) is configured
3. Type type type:
```
git clone https://github.com/attic-labs/noms $GOPATH/src/github.com/attic-labs/noms
go install github.com/attic-labs/noms/cmd/...

noms log http://demo.noms.io/cli-tour:film-locations
```
[Samples](samples/)&nbsp; | &nbsp;[Command-Line Tour](doc/cli-tour.md)&nbsp; | &nbsp;[JavaScript SDK Tour](doc/js-tour.md)&nbsp; | &nbsp;[Intro to Noms](doc/intro.md)


## Features

<table>
  <tr>
    <td><b>Versioning</b><br>
        Each commit is retained and can be viewed or reverted
    <td><b>Type inference</b><br>
        Each dataset has a precise schema that is automatically inferred
    <td><b>Atomic commits</b><br>
        Immutability enables atomic commits of any size
  <tr>
    <td><b>Diff</b><br>
      Compare structured datasets of any size efficiently
    <td><b>Schema versioning</b><br>
      Narrow or widen schemas instantly, without rewriting data
    <td><b>Sorted indexes</b><br>
      Fast range queries, on a single or a combination of attributes
  <tr>
    <td><b>Fork</b><br>
      Create your own isolated branch of a dataset to work on
    <td><b>Schema validation</b> (soon)<br>
      Optionally constrain commit types on a per-dataset basis
    <td><b>Insanely easy import</b><br>
      Noms auto-dedupes snapshots and generates a precise changelog
  <tr>
    <td><b>Sync</b><br>
      Sync disconnected database instances efficiently and correctly
    <td><b>Structural typing</b><br>
      Index, search, and match data by structure shape
    <td><b>Awesome export</b><br>
      Use dataset history to precisely apply sync changes out of Noms
</table>


## Use Cases

We're just getting started, but here are a few use cases we think Noms is especially well-suited for:

* **Data collaboration**—Work on data together. Track changes, fork, merge, sync, etc. The entire Git workflow, but on large-scale, structured data.
* **ETL**—ETL based on Noms is inherently incremental, undoable, idempotent, and auditable.
* **Data integration and enrichment**—A content-addressed database should be a really nice place to do data integration. Enrichments can be modeled as extensions to source data which are trivially undoable.
* **Decentralized database**—Noms is a natural fit to move structured data around certain kinds of widely decentralized applications.


## Get Involved

Noms is developed in the open. Come say hi.

- [Mailing List](nomsdb@googlegroups.com)
- [Slack](atticlabs.slack.com/messages/dev)
- [Twitter](https://twitter.com/nomsdb)
