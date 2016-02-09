# Noms

Noms is a content-addressable, append-only, peer-to-peer, structured data store.

In other words, *noms is git for data*.

This repository contains two reference implementations of the noms protocol - one in Go, and one in JavaScript. It also includes a number of tools and sample applications.

# Prerequisites

* [Go 1.4+](https://golang.org/dl/)
* [Python 2.7+](https://www.python.org/downloads/) (Note: Python 2.x only, not Python 3.x)
* [Node.js 5.3+](https://nodejs.org/download/)

# Set environment variables

* Ensure [`$GOPATH` is set correctly](https://golang.org/doc/code.html#GOPATH)
* Set `GO15VENDOREXPERIMENT=1` in your environment - all our code requires this

# Get the code

```
go get -u -t github.com/attic-labs/noms/...
```

# Build

```
go install `go list ./... | grep -v /vendor/`
go test `go list ./... | grep -v /vendor/`
```

# Run

```
cd "$GOPATH/src/github.com/attic-labs/noms/clients/counter"
go build
./counter -ldb=/tmp/foo -ds=foo
./counter -ldb=/tmp/foo -ds=foo
./counter -ldb=/tmp/foo -ds=foo
```

# What next?

* Learn the core tools: [`server`](clients/server), [`splore`](clients/splore), [`shove`](clients/shove), [`csv_importer`](clients/csv_importer), [`json_importer`](clients/json_importer), [`xml_importer`](clients/xml_importer)
* Run sample apps: [`sfcrime`](clients/sfcrime), [`tagshow`](clients/tagshow) (photo viewer)
* NomDL reference (TODO)
* Go SDK reference (TODO)
* JavaScript SDK reference (TODO)
