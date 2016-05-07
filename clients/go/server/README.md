# Server

Server implements a noms database over HTTP.

## Example

```
cd $GOPATH/src/github.com/attic-labs/noms/clients/go/counter
go build
./counter ldb:/tmp/servertest:counter
./counter ldb:/tmp/servertest:counter
./counter ldb:/tmp/servertest:counter

cd ../server
go build
./server ldb:/tmp/servertest
```

Then navigate a web browser to [http://localhost:8000/root](http://localhost:8000/root). You should see a string starting with `sha1-...`. This _ref_ is the unique identifier for the current state of the database. You can explore it further by fetching URLs like http://localhost:8000/ref/sha1-...

## About

Server is not commonly used directly by users, but is a building block used by other tools. For example, you can connect the counter application to your running server like so:

```
./counter http://localhost:8000:counter
./counter http://localhost:8000:counter
```
