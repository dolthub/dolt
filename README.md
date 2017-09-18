<img src='doc/nommy_cropped_smaller.png' width='350' title='Nommy, the snacky otter'>

[Technical Overview](doc/intro.md)&nbsp; | &nbsp;[Use Case: Decentralization](doc/decent/about.md)&nbsp; | &nbsp;[FAQ](doc/faq.md)&nbsp; | &nbsp;[Command-Line Interface](doc/cli-tour.md)&nbsp; | &nbsp;[Go SDK](doc/go-tour.md)&nbsp; | &nbsp;[Path Syntax](doc/spelling.md)
<br><br>
[![Build Status](http://jenkins3.noms.io/buildStatus/icon?job=NomsMasterBuilder)](http://jenkins3.noms.io/job/NomsMasterBuilder/)
[![codecov](https://codecov.io/gh/attic-labs/noms/branch/master/graph/badge.svg)](https://codecov.io/gh/attic-labs/noms)
[![GoDoc](https://godoc.org/github.com/attic-labs/noms?status.svg)](https://godoc.org/github.com/attic-labs/noms)
[![Slack](http://slack.noms.io/badge.svg)](http://slack.noms.io)

*Noms* is a decentralized database philosophically descendant from the Git version control system.

Like Git, Noms is:

* **Versioned:** By default, all previous versions of the database are retained. You can trivially track how the database evolved to its current state, easily and efficiently compare any two versions, or even rewind and branch from any previous version.
* **Synchronizable:** Instances of a single Noms database can be disconnected from each other for any amount of time, then later reconcile their changes efficiently and correctly.

Unlike Git, Noms is a database, so it also:

* Primarily **stores structured data**, not files and directories (see: [the Noms type system](https://github.com/attic-labs/noms/blob/master/doc/intro.md#types))
* **Scales well** to large amounts of data and concurrent clients (TODO: benchmarks)
* Supports **atomic transactions** (a single instance of Noms is CP, but Noms is typically run in production backed by S3, in which case it is "[effectively CA](https://cloud.google.com/spanner/docs/whitepapers/SpannerAndCap.pdf)")
* Supports **efficient indexes** (see: [Noms prolly-trees](https://github.com/attic-labs/noms/blob/master/doc/intro.md#prolly-trees-probabilistic-b-trees))
* Features a **flexible query model** (see: [GraphQL](./go/ngql/README.md))

Finally, because Noms is content-addressed, it yields a very pleasant programming model.

Working with Noms is ***declarative***. You don't `INSERT` new data, `UPDATE` existing data, or `DELETE` old data. You simply *declare* what the data ought to be right now. If you commit the same data twice, it will be deduplicated because of content-addressing. If you commit _almost_ the same data, only the part that is different will be written.

<br>

## Learn More About Noms

For the decentralized web: [The Decentralized Database](doc/decent/about.md)

Learn the basics: [Technical Overview](doc/intro.md)

Tour the CLI: [Command-Line Interface Tour](doc/cli-tour.md)

Tour the Go API: [Go SDK Tour](doc/go-tour.md)

<br>

### API

The Public API will continue to evolve. Pull requests which represent breaking API changes should be marked with `APIChange` and sent to the slack channel and mailing list below for advance warning and feedback.

<br>

## Talk

If you'd like to use Noms for something, we'd love to hear from
you. Contact us at [noms@attic.io](mailto:noms@attic.io) or via:

- [Slack](http://slack.noms.io)
- [Mailing List](https://groups.google.com/forum/#!forum/nomsdb)
- [Twitter](https://twitter.com/nomsdb)
