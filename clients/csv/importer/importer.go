package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/attic-labs/noms/clients/csv"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
)

var (
	dsFlags = dataset.NewFlags()
	// Actually the delimiter uses runes, which can be multiple characters long.
	// https://blog.golang.org/strings
	delimiter   = flag.String("delimiter", ",", "field delimiter for csv file, must be exactly one character long.")
	header      = flag.String("header", "", "header row. If empty, we'll use the first row of the file")
	name        = flag.String("name", "Row", "struct name. The user-visible name to give to the struct type that will hold each row of data.")
	reportTypes = flag.Bool("report-types", false, "read the entire file and report which types all values in each column would occupy safely.")
	columnTypes = flag.String("column-types", "", "a comma-separated list of types representing the desired type of each column. if absent all types default to be String")
)

func main() {
	cpuCount := runtime.NumCPU()
	runtime.GOMAXPROCS(cpuCount)

	flag.Usage = func() {
		fmt.Println("Usage: csv_importer [options] file\n")
		flag.PrintDefaults()
	}

	flag.Parse()

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

	r := csv.NewCSVReader(res, comma)

	var headers []string
	if *header == "" {
		headers, err = r.Read()
		d.Exp.NoError(err)
	} else {
		headers = strings.Split(*header, string(comma))
	}

	if *reportTypes {
		kinds := csv.ReportValidFieldTypes(r, headers)
		d.Chk.Equal(len(headers), len(kinds))
		fmt.Println("Possible types for each column:")
		for i, key := range headers {
			fmt.Printf("%s: %s\n", key, strings.Join(csv.KindsToStrings(kinds[i]), ","))
		}
		return
	}

	ds := dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		return
	}
	defer ds.DB().Close()

	kinds := []types.NomsKind{}
	if *columnTypes != "" {
		kinds = csv.StringsToKinds(strings.Split(*columnTypes, ","))
	}

	value, _ := csv.Read(r, *name, headers, kinds, ds.DB())
	_, err = ds.Commit(value)
	d.Exp.NoError(err)
}
