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

#### Data Collaboration

Work on data together. Track changes, fork, merge, sync, etc. The entire Git workflow, but on large-scale, structured or unstructured data. Useful for teams doing data analysis, cleansing, enrichment, etc.

#### ETL

Noms should work really well as a backing store for ETL pipelines. Noms-backed ETL is naturally:

* **Incremental:** Noms datasets can be efficiently diffed, so only the changed data needs to be run through the pipeline.
* **Versioned:** Any transform can be compared to the previous run and trivially undone or re-applied.
* **Idempotent:** If a transform fails in the middle for any reason, it can simply be re-run. A transform's result will always be the same no matter how many times it is run.
* **Auditable:** Content-addressing enables precisely tracking inputs to each transform and result.

#### Data Integration and Enrichment

Similar to ETL, Noms makes a natural store for data aggregation, integration, and enrichment. Data integration and enrichment can be modeled in a non-destructive way as metadata assertions from content to attribute. Assertions can be owned by the creating application and undone en-masse by deleting a single Noms dataset.

#### Decentralized database

Noms should be a natural fit to move data around certain kinds of widely decentralized applications. Rather than just moving raw files, e.g., with rsync, you can move around structured data which is immediately queryable and usable by the applciation.


## Get Involved

Noms is developed in the open. Come say hi.

- [Mailing List](nomsdb@googlegroups.com)
- [Slack](atticlabs.slack.com/messages/dev)
- [Twitter](https://twitter.com/nomsdb)
