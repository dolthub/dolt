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
	dsFlags = dataset.NewFlags()
	// Actually the delimiter uses runes, which can be multiple characters long.
	// https://blog.golang.org/strings
	delimiter = flag.String("delimiter", ",", "field delimiter for csv file, must be exactly one character long.")
	header    = flag.String("header", "", "header row. If empty, we'll use the first row of the file")
	name      = flag.String("name", "Row", "struct name. The user-visible name to give to the struct type that will hold each row of data.")
)

func main() {
	cpuCount := runtime.NumCPU()
	runtime.GOMAXPROCS(cpuCount)

	flag.Usage = func() {
		fmt.Println("Usage: csv_importer [options] file\n")
		flag.PrintDefaults()
	}

	flag.Parse()
	ds := dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		return
	}
	defer ds.Store().Close()

	if flag.NArg() != 1 {
		fmt.Printf("Expected exactly one parameter (path) after flags, but you have %d. Maybe you put a flag after the path?\n", flag.NArg())
		flag.Usage()
		return
	}

	path := flag.Arg(0)
	if path == "" {
		flag.Usage()
		return
	}

	res, err := os.Open(path)
	d.Exp.NoError(err)
	defer res.Close()

	comma, err := csv.StringToRune(*delimiter)
	if err != nil {
		fmt.Println(err.Error())
		flag.Usage()
		return
	}

	value, _, _ := csv.Read(res, *name, *header, comma, ds.Store())
	_, err = ds.Commit(value)
	d.Exp.NoError(err)
}
