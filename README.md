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

cd $GOPATH/src/github.com/attic-labs/noms/clients/go/counter
go build
./counter ldb:/tmp/nomsdb:counter
./counter ldb:/tmp/nomsdb:counter
./counter ldb:/tmp/nomsdb:counter

noms log ldb:/tmp/nomsdb:counter
```

# What next?

* [Introduction to Noms](intro.md)
* [Command Line Tour](cli-tour.md)
* [JavaScript SDK Tour](js-tour.md)
* Check out our demos (TODO)
