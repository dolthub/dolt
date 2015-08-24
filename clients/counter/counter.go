package main

import (
	"flag"
	"fmt"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
)

func main() {
	dsFlags := dataset.NewFlags()
	flag.Parse()

	ds := dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		return
	}

	lastVal := uint64(0)
	commit := ds.Head()
	if !commit.Equals(datas.EmptyCommit) {
		lastVal = uint64(commit.Value().(types.UInt64))
	}
	newVal := lastVal + 1
	_, ok := ds.Commit(datas.NewCommit().SetParents(ds.HeadAsSet()).SetValue(types.UInt64(newVal)))
	d.Exp.True(ok, "Could not commit due to conflicting edit")

	fmt.Println(newVal)
}
