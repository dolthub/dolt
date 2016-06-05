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
[Samples](TODO)&nbsp; | &nbsp;[Command-Line Tour](doc/cli-tour.md)&nbsp; | &nbsp;[JavaScript SDK Tour](doc/js-tour.md)&nbsp; | &nbsp;[Intro to Noms](doc/intro.md)


## Features

<table>
  <tr>
    <td><b><a href="doc/cli-tour.md#noms-log">Versioning</a></b><br>
        Each commit is retained and can be viewed or reverted
    <td><b><a href="doc/intro.md#type-accretion">Type inference</a></b><br>
        Each dataset has a precise schema that is automatically inferred
    <td><b><a href="doc/intro.md#databases-and-datasets">Atomic commits</a></b><br>
        Immutability enables atomic commits of any size
  <tr>
    <td><b><a href="doc/cli-tour.md#noms-diff">Diff</a></b><br>
    	Compare structured datasets of any size efficiently
    <td><b><a href="doc/intro.md#type-accretion">Schema versioning</a></b><br>
    	Narrow or widen schemas instantly, without rewriting data
    <td><b><a href="doc/intro.md#indexing-and-searching-with-prolly-trees">Sorted indexes</a></b><br>
    	Fast range queries, on a single or a combination of attributes
  <tr>
    <td><b><a href="doc/cli-tour.md#noms-sync">Fork</a></b><br>
      Create your own isolated branch of a dataset to work in
    <td><b><a href="doc/intro.md#types">Schema validation</a></b> (soon)<br>
      Optionally constrain commit types on a per-dataset basis
    <td><b><a href="samples/js/fb">Insanely easy import</a></b><br>
      Continuous import from anywhere with automatic deduplication
  <tr>
    <td><b><a href="doc/cli-tour.md#noms-sync">Sync</a></b><br>
      Sync disconnected database instances efficiently and correctly
    <td><b><a href="doc/intro.md#types">Structural typing</a></b><br>
      Index, search, and match data by structure shape
    <td><b>Awesome export</b><br>
      Continuously and efficiently export from Noms to anywhere
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
