# noms-serve

noms-serve implements a noms database over HTTP.

## Example

```
cd $GOPATH/src/github.com/attic-labs/noms/clients/go/counter
go build
./counter ldb:/tmp/nomsdb:counter
./counter ldb:/tmp/nomsdb:counter
./counter ldb:/tmp/nomsdb:counter

noms serve ldb:/tmp/nomsdb:counter
```

Then, in a separate shell:

```
./counter http://localhost:8000:counter

noms ds http://localhost:8000
```

## About

Server is not commonly used directly by users, but is a building block used by other tools. For example, you can connect the counter application to your running server like so:

```
./counter http://localhost:8000:counter
./counter http://localhost:8000:counter
```
