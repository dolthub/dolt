# Shove

Shove syncs between databases and datasets. It is the noms equivalent of Git's `push` and `pull` commands.

## Example

```
cd $GOPATH/src/github.com/attic-labs/noms/clients/counter
go build
./counter -ldb="/tmp/shovetest1" -ds="counter"
./counter -ldb="/tmp/shovetest1" -ds="counter"
./counter -ldb="/tmp/shovetest1" -ds="counter"

cd ../shove
go build
./shove -source-ldb="/tmp/shovetest1" -source="counter" -sink-ldb="/tmp/shovetest2" -sink-ds="counter2"
../counter/counter -ldb="/tmp/shovetest2" -ds="counter2"

# Shove can also connect to http databases
cd ../server
go build
./server -ldb="/tmp/shovetest2" &

../shove/shove -source-h="http://localhost:8000" -source="counter2" -sink-ldb="/tmp/shovetest3" -sink-ds="counter3"
../counter/counter -ldb="/tmp/shovetest3" -ds="counter3"
```

## Sample data

There are currently a small collection of datasets you can sync available at `-h="ds.noms.io"`. You can browse them at [http://splore.noms.io](http://splore.noms.io) (username: attic, password: labs).
