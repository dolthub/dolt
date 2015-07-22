package main

import (
	"flag"
	"fmt"

	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
)

func main() {
	dsFlags := dataset.Flags()
	flag.Parse()

	ds := dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		return
	}

	lastVal := uint64(0)
	commits := ds.Heads()
	if commits.Len() > uint64(0) {
		lastVal = uint64(commits.Any().Value().(types.UInt64))
	}
	newVal := lastVal + 1
	ds.Commit(datas.NewCommitSet().Insert(
		datas.NewCommit().SetParents(
			commits.NomsValue()).SetValue(
			types.UInt64(newVal))))

	fmt.Println(newVal)
}
