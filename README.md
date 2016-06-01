# Hello!

Noms is a content-addressable, append-only, peer-to-peer, structured data store.

In other words, *noms is git for data*.

This repository contains two reference implementations of the noms protocol - one in Go, and one in JavaScript. It also includes a number of tools and sample applications.

# Get Noms

First make sure you have these prerequisites installed:

* [Git](https://git-scm.com/)
* [Go 1.6+](https://golang.org/dl/)
* [Node.js 5.11+](https://nodejs.org/download/)

Then, fetch and build Noms:

```
git clone https://github.com/attic-labs/noms $GOPATH/src/github.com/attic-labs/noms
go install github.com/attic-labs/noms/cmd/...
```

# Run

```
cd "$GOPATH/src/github.com/attic-labs/noms/clients/go/counter"
go build
./counter ldb:/tmp/nomsdb:counter
./counter ldb:/tmp/nomsdb:counter
./counter ldb:/tmp/nomsdb:counter

noms log ldb:/tmp/nomsdb:counter
```

# What next?

* Learn the core tools: [`serve`](cmd/noms-serve), [`splore`](clients/js/splore), [`sync`](cmd/noms-sync), [`csv import/export`](clients/go/csv), [`json-import`](clients/go/json-import), [`xml-import`](clients/go/xml-import)
* Run sample apps: (TODO)
* NomDL reference (TODO)
* Go SDK reference (TODO)
* JavaScript SDK reference (TODO)

## Recommended Chrome extensions

* Hides generated (Go) files from GitHub pull requests: [GitHub PR gen hider](https://chrome.google.com/webstore/detail/mhemmopgidccpkibohejfhlbkggdcmhf).
