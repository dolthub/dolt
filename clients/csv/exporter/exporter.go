package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/attic-labs/noms/clients/csv"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
)

var (
	dsFlags = dataset.NewFlags()
	// Actually the delimiter uses runes, which can be multiple characters long.
	// https://blog.golang.org/strings
	delimiter = flag.String("delimiter", ",", "field delimiter for csv file, must be exactly one character long.")
)

func main() {
	cpuCount := runtime.NumCPU()
	runtime.GOMAXPROCS(cpuCount)

	flag.Usage = func() {
		fmt.Println("Usage: csv_exporter [options] > filename")
		flag.PrintDefaults()
	}

	flag.Parse()
	ds := dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		return
	}
	defer ds.DB().Close()

	comma, err := csv.StringToRune(*delimiter)
	if err != nil {
		fmt.Println(err.Error())
		flag.Usage()
		return
	}

	err = d.Try(func() {
		nomsList, structDesc := csv.ValueToListAndElemDesc(ds.Head().Get(datas.ValueField), ds.DB())
		csv.Write(nomsList, structDesc, comma, os.Stdout)
	})
	if err != nil {
		fmt.Println("Failed to export dataset as CSV:")
		fmt.Println(err)
	}
}
