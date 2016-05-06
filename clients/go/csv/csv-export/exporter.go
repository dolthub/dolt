package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/attic-labs/noms/clients/go/csv"
	"github.com/attic-labs/noms/clients/go/flags"
	"github.com/attic-labs/noms/clients/go/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
)

var (
	// Actually the delimiter uses runes, which can be multiple characters long.
	// https://blog.golang.org/strings
	delimiter = flag.String("delimiter", ",", "field delimiter for csv file, must be exactly one character long.")
)

func main() {
	flags.RegisterDataStoreFlags()
	cpuCount := runtime.NumCPU()
	runtime.GOMAXPROCS(cpuCount)

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: csv-export [options] dataset > filename")
		flag.PrintDefaults()
	}

	flag.Parse()

	if flag.NArg() != 1 {
		util.CheckError(errors.New("expected dataset arg"))
	}

	spec, err := flags.ParseDatasetSpec(flag.Arg(0))
	util.CheckError(err)

	ds, err := spec.Dataset()
	util.CheckError(err)

	defer ds.Store().Close()

	comma, err := csv.StringToRune(*delimiter)
	util.CheckError(err)

	err = d.Try(func() {
		nomsList, structDesc := csv.ValueToListAndElemDesc(ds.Head().Get(datas.ValueField), ds.Store())
		csv.Write(nomsList, structDesc, comma, os.Stdout)
	})
	if err != nil {
		fmt.Println("Failed to export dataset as CSV:")
		fmt.Println(err)
	}
}
