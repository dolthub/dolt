package main

import (
	"flag"
	"fmt"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datastore"
	"github.com/attic-labs/noms/types"
)

func main() {
	flags := chunks.NewFlags()
	flag.Parse()
	cs := flags.CreateStore()
	if cs == nil {
		flag.Usage()
		return
	}
	ds := datastore.NewDataStore(cs, cs.(chunks.RootTracker))

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
