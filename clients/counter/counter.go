package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/attic-labs/noms/clients/flags"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/types"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [options] <dataset>\n", os.Args[0])
		flag.PrintDefaults()
	}

	flags.RegisterDataStoreFlags()
	flag.Parse()

	if flag.NArg() != 1 {
		util.CheckError(errors.New("expected dataset arg"))
	}

	spec, err := flags.ParseDatasetSpec(flag.Arg(0))
	util.CheckError(err)
	ds, err := spec.Dataset()
	util.CheckError(err)

	defer ds.Store().Close()

	lastVal := uint64(0)
	if commit, ok := ds.MaybeHead(); ok {
		lastVal = uint64(commit.Get(datas.ValueField).(types.Number))
	}
	newVal := lastVal + 1
	_, err = ds.Commit(types.Number(newVal))
	d.Exp.NoError(err)

	fmt.Println(newVal)
}
