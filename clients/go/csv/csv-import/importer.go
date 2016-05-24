package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/attic-labs/noms/clients/go/csv"
	"github.com/attic-labs/noms/clients/go/flags"
	"github.com/attic-labs/noms/clients/go/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/util/progressreader"
	"github.com/attic-labs/noms/util/status"

	humanize "github.com/dustin/go-humanize"
)

var (
	// Actually the delimiter uses runes, which can be multiple characters long.
	// https://blog.golang.org/strings
	delimiter   = flag.String("delimiter", ",", "field delimiter for csv file, must be exactly one character long.")
	header      = flag.String("header", "", "header row. If empty, we'll use the first row of the file")
	name        = flag.String("name", "Row", "struct name. The user-visible name to give to the struct type that will hold each row of data.")
	reportTypes = flag.Bool("report-types", false, "read the entire file and report which types all values in each column would occupy safely.")
	columnTypes = flag.String("column-types", "", "a comma-separated list of types representing the desired type of each column. if absent all types default to be String")
	noProgress  = flag.Bool("no-progress", false, "prevents progress from being output if true")
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

	fi, err := res.Stat()
	d.Chk.NoError(err)

	var r io.Reader = res
	if !*noProgress {
		r = progressreader.New(r, getStatusPrinter(uint64(fi.Size())))
	}
	cr := csv.NewCSVReader(r, comma)

	var headers []string
	if *header == "" {
		headers, err = cr.Read()
		d.Exp.NoError(err)
	} else {
		headers = strings.Split(*header, string(comma))
	}

	if *reportTypes {
		kinds := csv.ReportValidFieldTypes(cr, headers)
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
	defer ds.Database().Close()

	kinds := []types.NomsKind{}
	if *columnTypes != "" {
		kinds = csv.StringsToKinds(strings.Split(*columnTypes, ","))
	}

	value, _ := csv.Read(cr, *name, headers, kinds, ds.Database())
	_, err = ds.Commit(value)
	if !*noProgress {
		status.Clear()
	}
	d.Exp.NoError(err)
}

func getStatusPrinter(expected uint64) progressreader.Callback {
	startTime := time.Now()
	return func(seen uint64) {
		percent := float64(seen) / float64(expected) * 100
		elapsed := time.Now().Sub(startTime)
		rate := float64(seen) / elapsed.Seconds()

		status.Printf("%.2f%% of %s (%s/s)...",
			percent,
			humanize.Bytes(expected),
			humanize.Bytes(uint64(rate)))
	}
}
