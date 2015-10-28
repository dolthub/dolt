package main

import (
	"flag"
	"fmt"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
)

var (
	dsFlags = dataset.NewFlags()
)

func main() {
	flag.Parse()

	ds := dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		return
	}
	defer ds.Close()

	lastVal := uint64(0)
	if commit, ok := ds.MaybeHead(); ok {
		lastVal = uint64(commit.Value().(types.UInt64))
	}
	newVal := lastVal + 1
	_, ok := ds.Commit(types.UInt64(newVal))
	d.Exp.True(ok, "Could not commit due to conflicting edit")

	fmt.Println(newVal)
}
