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
        Each commit is <a href="doc/cli-tour.md#noms-log">retained</a> and can be viewed or reverted
    <td><b>Type inference</b><br>
        Each dataset has a precise schema that is automatically <a href="doc/intro.md#types">inferred</a>
    <td><b>Atomic commits</b><br>
        Immutability enables <a href="doc/intro.md#databases-and-datasets">atomic commits</a> of any size
  <tr>
    <td><b>Diff</b><br>
    	<a href="doc/cli-tour.md#noms-diff">Compare</a> structured datasets of any size efficiently
    <td><b>Schema versioning</b><br>
    	<a href="doc/intro.md#type-accretion.md">Narrow or widen</a> schemas instantly, without rewriting data
    <td><b>Sorted indexes</b><br>
    	Fast <a href="a href="#doc/intro.md#indexing-and-searching-with-prolly-trees">range queries</a>, on a single or a combination of attributes
  <tr>
    <td><b>Fork</b><br>
      Create your own isolated <a href="doc/cli-tour.md#noms-sync">branch</a> of a dataset to work in
    <td><b>Schema validation</b> (soon)<br>
      Optionally <a href="doc/intro.md#types">constrain</a> commit types on a per-dataset basis
    <td><b>Insanely easy import</b><br>
      Import <a href="samples/js/fb">snapshots</a>—Noms auto-dedupes and generates a precise changelog
  <tr>
    <td><b>Sync</b><br>
      <a href="doc/cli-tour.md#noms-sync">Sync</a> disconnected database instances efficiently and correctly
    <td><b>Structural typing</b><br>
      Index, search, and match data by structure <a href="doc/intro.md#types">shape</a>
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
