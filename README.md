# Noms

Noms is a content-addressable, append-only, peer-to-peer, structured data store.

In other words, *noms is git for data*.

This repository contains two reference implementations of the noms protocol - one in Go, and one in JavaScript. It also includes a number of tools and sample applications.

# Prerequisites

* [Go 1.4+](https://golang.org/dl/)
* [Python 2.7+](https://www.python.org/downloads/) (Note: Python 2.x only, not Python 3.x)
* [Node.js 5.3+](https://nodejs.org/download/)

# Get the code

First, ensure `$GOPATH` is [set correctly](https://golang.org/doc/code.html#GOPATH). Then:

```
go get -u -t github.com/attic-labs/noms/...
```

# Build

```
go build ./...
go test ./...
```

# Run

```
cd <noms>/clients/counter
go build
./counter -ldb=/tmp/foo -ds=foo
./counter -ldb=/tmp/foo -ds=foo
./counter -ldb=/tmp/foo -ds=foo
```

# What next?

* Learn the core tools: [`server`](clients/server/README.md), [`splore`](clients/splore/README.md), [`shove`](clients/shove/README.md), [`csv_importer`](clients/csv_importer/README.md), [`json_importer`](clients/json_importer), [`xml_importer`](clients/xml_importer)
* Run sample apps: [`sfcrime`](clients/sfcrime/README.md), [`tagshow` photo viewer](clients/tagshow/README.md)
* NomDL reference
* Go SDK
* JavaScript SDK
