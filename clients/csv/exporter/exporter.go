package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/attic-labs/noms/clients/csv"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
)

var (
	p       = flag.Int("p", 512, "parallelism")
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
	defer ds.Store().Close()

	comma, err := csv.StringToRune(*delimiter)
	if err != nil {
		fmt.Println(err.Error())
		flag.Usage()
		return
	}

	err = d.Try(func() {
		csv.Write(ds, comma, *p, os.Stdout)
	})
	if err != nil {
		fmt.Println("Failed to export dataset as CSV:")
		fmt.Println(err)
	}
}
