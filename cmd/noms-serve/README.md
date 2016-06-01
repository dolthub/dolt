# noms-serve

noms-serve implements a noms database over HTTP.

## Example

```
cd $GOPATH/src/github.com/attic-labs/noms/clients/go/counter
go build
./counter ldb:/tmp/nomsdb:counter
./counter ldb:/tmp/nomsdb:counter
./counter ldb:/tmp/nomsdb:counter

noms serve ldb:/tmp/nomsdb
```

Then, in a separate shell:

```
# This starts where the previous count left off because we're serving the same database
./counter http://localhost:8000:counter

# Display the datasets at this server
noms ds http://localhost:8000

# Print the history of the counter dataset
noms log http://localhost:8000:counter
```
