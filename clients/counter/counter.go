package main

import (
	"flag"
	"fmt"

	"github.com/attic-labs/noms/datastore"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/user"
)

var (
	appDataStoreFlags = user.AppDataFlags()
)

func main() {
	flag.Parse()

	ds := appDataStoreFlags.CreateStore()
	if ds == nil {
		flag.Usage()
		return
	}

	lastVal := uint64(0)
	roots := ds.Roots()
	if roots.Len() > uint64(0) {
		lastVal = uint64(roots.Any().Value().(types.UInt64))
	}
	newVal := lastVal + 1
	ds.Commit(datastore.NewRootSet().Insert(
		datastore.NewRoot().SetParents(
			roots.NomsValue()).SetValue(
			types.UInt64(newVal))))

	fmt.Println(newVal)
}
