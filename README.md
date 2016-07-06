# Store All the Things

*Noms* is a content-addressed, immutable, decentralized, strongly-typed database.

In other words, Noms is Git for data.

This repository contains two reference implementations of the database—one in Go, and one in JavaScript. It also includes a number of tools and sample applications.


## Setup

Noms is supported on Mac OS X and Linux. Windows is not currently supported.

1. Install [Go 1.6+](https://golang.org/dl/)
2. Ensure your [$GOPATH](https://github.com/golang/go/wiki/GOPATH) is configured
3. Type type type:
```
git clone https://github.com/attic-labs/noms $GOPATH/src/github.com/attic-labs/noms
go install github.com/attic-labs/noms/cmd/...

noms log http://demo.noms.io/cli-tour::film-locations
```
[Command-Line Tour](doc/cli-tour.md)&nbsp; | &nbsp;[Go SDK Tour](doc/go-tour.md)&nbsp; | &nbsp;[JavaScript SDK Tour](doc/js-tour.md)&nbsp; | &nbsp;[Intro to Noms](doc/intro.md)

<br/>
## What Is Noms Good For?

#### Data Version Control

Noms gives you the entire Git workflow, but for large-scale structured (or unstructured) data. Fork, merge, track history, efficiently synchronize changes, etc.

[<img src="doc/data-version-control.png" width="320" height="180">](https://www.youtube.com/watch?v=ONByMptWa2A)<br/>
*[`noms diff` and `noms log` on large datasets](https://www.youtube.com/watch?v=ONByMptWa2A)*


#### An Application Database with History

A database where every change is automatically and efficiently preserved. Instantly revert to, fork, or work from any historical commit.

[<img src="doc/versioned-database.png" width="320" height="180">](https://www.youtube.com/watch?v=JDO3z0vHEso)<br/>
*[Versioning, Diffing, and Syncing with Noms](https://www.youtube.com/watch?v=JDO3z0vHEso)*


#### An Archival Database

Trivially import snapshots from any format or API. Data is automatically versioned and deduplicated. Track the history of each datasource. Search across data sources.

*TODO: Sample and video*


#### More!

On our better days, we think of a decentralized, synchronizing database like Noms as an important new primitive in an increasing distributed computing environment. And we imagine many fascinating ways it could be used.

But we must crawl before we run. Right now, we are putting the basics in place: Efficient updates and range scans. Efficient diff and sync. Data integrity.

[Let us know](https://groups.google.com/forum/#!forum/nomsdb) what ideas you have for Noms, or better yet [help us](TODO - help) build them.

<br>
## Status

**Noms is in beta. Please keep a backup of important data in some other store.**

We are working toward a 1.0, after which time we will remove this warning and guarantee format stability.

<br>
## Get Involved

- [Mailing List](https://groups.google.com/forum/#!forum/nomsdb)
- [Slack](atticlabs.slack.com/messages/dev)
- [Twitter](https://twitter.com/nomsdb)
