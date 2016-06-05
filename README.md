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
[**Samples**](TODO)&nbsp; | &nbsp;[**Command-Line Tour**](doc/cli-tour.md)&nbsp; | &nbsp;[**JavaScript SDK Tour**](doc/js-tour.md)&nbsp; | &nbsp;[**Intro to Noms**](doc/intro.md)


## Features

<table>
  <tr>
    <td><a href="doc/cli-tour.md#noms-log">Versioning</a><br>
        Each commit is retained and can be viewed or reverted
    <td><a href="doc/intro.md#types">Type inference</a><br>
        Each dataset has a precise schema that is automatically inferred
    <td><a href="doc/intro.md#databases-and-datasets">Atomic commits</a><br>
        Immutability enables atomic commits of any size
  <tr>
    <td><a href="doc/cli-tour.md#noms-diff">Diff</a><br>
    	Compare structured datasets of any size efficiently
    <td><a href="doc/intro.md#type-accretion.md">Schema versioning</a><br>
    	Narrow or widen schemas instantly, without rewriting data
    <td><a href="#doc/intro.md#indexing-and-searching-with-prolly-trees">Sorted indexes</a><br>
    	Fast range queries, on a single or a combination of attributes
  <tr>
    <td><a href="doc/cli-tour.md#noms-sync">Fork</a><br>
      Create your own isolated branch of a dataset to work in
    <td><a href="doc/intro.md#types">Schema validation</a> (soon)<br>
      Optionally constrain commit types on a per-dataset basis
    <td><a href="samples/js/fb">Insanely easy import</a><br>
      Import snapshots—Noms auto-dedupes and generates a precise changelog
  <tr>
    <td><a href="doc/cli-tour.md#noms-sync">Sync</a><br>
      Sync disconnected database instances efficiently and correctly
    <td><a href="doc/intro.md#types">Structural typing</a><br>
      Index, search, and match data by structure shape
    <td>Awesome export<br>
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
