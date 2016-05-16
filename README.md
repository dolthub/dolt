# Noms

Noms is a content-addressable, append-only, peer-to-peer, structured data store.

In other words, *noms is git for data*.

This repository contains two reference implementations of the noms protocol - one in Go, and one in JavaScript. It also includes a number of tools and sample applications.

# Prerequisites

* [Go 1.6+](https://golang.org/dl/)
* [Python 2.7+](https://www.python.org/downloads/) (Note: Python 2.x only, not Python 3.x)
* [Node.js 5.11+](https://nodejs.org/download/)

# Set environment variables

* Ensure [`$GOPATH` is set correctly](https://golang.org/doc/code.html#GOPATH)
* Set `GO15VENDOREXPERIMENT=1` in your environment - all our code requires this

# Get the code

```
mkdir -p $GOPATH/src/github.com/attic-labs
cd $GOPATH/src/github.com/attic-labs
git clone https://github.com/attic-labs/noms
```

# Build

```
go install `go list ./... | grep -v /vendor/`
go test `go list ./... | grep -v /vendor/`
```

# Run

```
cd "$GOPATH/src/github.com/attic-labs/noms/clients/go/counter"
go build
./counter ldb:/tmp/foo:foo
./counter ldb:/tmp/foo:foo
./counter ldb:/tmp/foo:foo
```

# What next?

* Learn the core tools: [`serve`](cmd/noms-serve), [`splore`](clients/js/splore), [`sync`](cmd/noms-sync), [`csv import/export`](clients/go/csv), [`json-import`](clients/go/json-import), [`xml-import`](clients/go/xml-import)
* Run sample apps: (TODO)
* NomDL reference (TODO)
* Go SDK reference (TODO)
* JavaScript SDK reference (TODO)

## Recommended Chrome extensions

* Hides generated (Go) files from GitHub pull requests: [GitHub PR gen hider](https://chrome.google.com/webstore/detail/mhemmopgidccpkibohejfhlbkggdcmhf).
