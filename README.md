# Noms

Noms is a content-addressable, immutable, peer-to-peer datastore for structured data.

In other words, *noms is git for data*.

This repository will contain the reference implementation of the noms protocol, and will eventually be open sourced. 

This includes:

* Go wrappers for all the core noms types
* Support for generating Go types from schema definitions (aka 'nomdl')
* Chunking and dechunking
* Serialization and deserialization
* Chunkstore interface as well as several sample implementations
* Search support
* Sample applications

# Get

```
git clone https://github.com/attic-labs/noms
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

# Rejoice!

You can see the raw data:

```
ls /tmp/foo
cat /tmp/foo/*.log | strings
```

You can also explore the data visually. Follow the instructions in `clients/splore`.

There are lots of other sample programs in `clients/` and they usually have `README`s. Have fun...

TODO: There needs to be more of a big-picture introduction.
