# Noms

Noms is a content-addressable, immutable, peer-to-peer datastore for structured data.

In other words, *noms is git for structured data*.

This repository will contain the reference implementation of the noms protocol, and will eventually be open sourced. 

This includes:

* Go wrappers for all the core Nom types
* Support for generating Go types from Nom schema definitions
* Chunking and dechunking
* Serialization and deserialization
* Chunkstore interface as well as several sample implementations
* Search support
* Sample applications

# Get the code

`go get -u -t github.com/attic-labs/noms/...`

# Build

```
go build ./...
go test ./...
```

# Run

```
cd <noms>/clients/counter
go build
./counter -file-store="/tmp/foo"
./counter -file-store="/tmp/foo"
./counter -file-store="/tmp/foo"
```

rejoice!

You can see the raw data:

```
ls /tmp/foo
cat /tmp/foo/root
```

You can also explore the data visually. Follow the instructions in `clients/explore`.
