package main

import (
	"flag"
	"fmt"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
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
	defer ds.DB().Close()

	lastVal := uint64(0)
	if commit, ok := ds.MaybeHead(); ok {
		lastVal = uint64(commit.Get(datas.ValueField).(types.Number))
	}
	newVal := lastVal + 1
	_, err := ds.Commit(types.Number(newVal))
	d.Exp.NoError(err)

	fmt.Println(newVal)
}
