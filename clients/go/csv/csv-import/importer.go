package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/attic-labs/noms/clients/go/csv"
	"github.com/attic-labs/noms/clients/go/flags"
	"github.com/attic-labs/noms/clients/go/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/types"
)

var (
	// Actually the delimiter uses runes, which can be multiple characters long.
	// https://blog.golang.org/strings
	delimiter   = flag.String("delimiter", ",", "field delimiter for csv file, must be exactly one character long.")
	header      = flag.String("header", "", "header row. If empty, we'll use the first row of the file")
	name        = flag.String("name", "Row", "struct name. The user-visible name to give to the struct type that will hold each row of data.")
	reportTypes = flag.Bool("report-types", false, "read the entire file and report which types all values in each column would occupy safely.")
	columnTypes = flag.String("column-types", "", "a comma-separated list of types representing the desired type of each column. if absent all types default to be String")
)

func main() {
	flags.RegisterDatabaseFlags()
	cpuCount := runtime.NumCPU()
	runtime.GOMAXPROCS(cpuCount)

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: csv-import [options] <dataset> <csvfile>\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if flag.NArg() != 2 {
		err := errors.New("Expected exactly two parameters (dataset and path) after flags, but you have %d. Maybe you put a flag after the path?")
		util.CheckError(err)
	}

	path := flag.Arg(1)

	res, err := os.Open(path)
	d.Exp.NoError(err)
	defer res.Close()

	comma, err := csv.StringToRune(*delimiter)
	if err != nil {
		fmt.Println(err.Error())
		flag.Usage()
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

	spec, err := flags.ParseDatasetSpec(flag.Arg(0))
	util.CheckError(err)
	ds, err := spec.Dataset()
	util.CheckError(err)
	defer ds.Store().Close()

	kinds := []types.NomsKind{}
	if *columnTypes != "" {
		kinds = csv.StringsToKinds(strings.Split(*columnTypes, ","))
	}

	value, _ := csv.Read(r, *name, headers, kinds, ds.Store())
	_, err = ds.Commit(value)
	d.Exp.NoError(err)
}
