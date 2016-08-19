// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/profile"
	"github.com/attic-labs/noms/samples/go/csv"
	flag "github.com/juju/gnuflag"
)

func main() {
	// Actually the delimiter uses runes, which can be multiple characters long.
	// https://blog.golang.org/strings
	delimiter := flag.String("delimiter", ",", "field delimiter for csv file, must be exactly one character long.")
	header := flag.String("header", "", "header row. If empty, we'll use the first row of the file")
	skipRecords := flag.Uint("skip-records", 0, "number of records to skip at beginning of file")
	detectColumnTypes := flag.Bool("detect-column-types", false, "detect column types by analyzing a portion of csv file")
	detectPrimaryKeys := flag.Bool("detect-pk", false, "detect primary key candidates by analyzing a portion of csv file")
	numSamples := flag.Int("num-samples", 1000000, "number of records to use for samples")
	numFieldsInPK := flag.Int("num-fields-pk", 3, "maximum number of columns to consider when detecting PKs")

	profile.RegisterProfileFlags(flag.CommandLine)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: csv-analyze [options] <csvfile>\n\n")
		flag.PrintDefaults()
	}

	flag.Parse(true)

	if flag.NArg() != 1 {
		flag.Usage()
		return
	}

	defer profile.MaybeStartProfile().Stop()

	var r io.Reader
	var filePath string

	filePath = flag.Arg(0)
	res, err := os.Open(filePath)
	d.CheckError(err)
	defer res.Close()
	r = res

	comma, err := csv.StringToRune(*delimiter)
	d.CheckErrorNoUsage(err)

	cr := csv.NewCSVReader(r, comma)
	csv.SkipRecords(cr, *skipRecords)

	var headers []string
	if *header == "" {
		headers, err = cr.Read()
		d.PanicIfError(err)
	} else {
		headers = strings.Split(*header, string(comma))
	}

	kinds := []types.NomsKind{}
	if *detectColumnTypes {
		kinds = csv.GetSchema(cr, *numSamples, len(headers))
		fmt.Fprintf(os.Stdout, "%s\n", strings.Join(csv.KindsToStrings(kinds), ","))
	}

	if *detectPrimaryKeys {
		pks := csv.FindPrimaryKeys(cr, *numSamples, *numFieldsInPK, len(headers))
		for _, pk := range pks {
			fmt.Fprintf(os.Stdout, "%s\n", strings.Join(csv.GetFieldNamesFromIndices(headers, pk), ","))
		}
	}
}
