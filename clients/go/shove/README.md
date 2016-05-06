# Shove

Shove syncs between datastores and datasets. It is the noms equivalent of Git's `push` and `pull` commands.

## Example

```
cd $GOPATH/src/github.com/attic-labs/noms/clients/go/counter
go build
./counter ldb:/tmp/shovetest1:counter
./counter ldb:/tmp/shovetest1:counter
./counter ldb:/tmp/shovetest1:counter

cd ../shove
go build
./shove ldb:/tmp/shovetest1:counter ldb:/tmp/shovetest2:counter2
../counter/counter ldb:/tmp/shovetest2:counter2

# Shove can also connect to http datastores
cd ../server
go build
./server ldb:/tmp/shovetest2 &

../shove/shove http://localhost:8000:counter2 ldb:/tmp/shovetest3:counter3
../counter/counter ldb:/tmp/shovetest3:counter3
```
