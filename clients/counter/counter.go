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
		lastVal = uint64(commit.Value().(types.Uint64))
	}
	newVal := lastVal + 1
	_, err := ds.Commit(types.Uint64(newVal))
	d.Exp.NoError(err)

	fmt.Println(newVal)
}
