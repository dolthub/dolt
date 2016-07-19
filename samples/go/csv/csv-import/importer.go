// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/profile"
	"github.com/attic-labs/noms/go/util/progressreader"
	"github.com/attic-labs/noms/go/util/status"
	"github.com/attic-labs/noms/samples/go/csv"

	humanize "github.com/dustin/go-humanize"
)

const (
	destList = iota
	destMap  = iota
)

func main() {
	var (
		// Actually the delimiter uses runes, which can be multiple characters long.
		// https://blog.golang.org/strings
		delimiter       = flag.String("delimiter", ",", "field delimiter for csv file, must be exactly one character long.")
		header          = flag.String("header", "", "header row. If empty, we'll use the first row of the file")
		name            = flag.String("name", "Row", "struct name. The user-visible name to give to the struct type that will hold each row of data.")
		columnTypes     = flag.String("column-types", "", "a comma-separated list of types representing the desired type of each column. if absent all types default to be String")
		path            = flag.String("p", "", "noms path to blob to import")
		noProgress      = flag.Bool("no-progress", false, "prevents progress from being output if true")
		destType        = flag.String("dest-type", "list", "the destination type to import to. can be 'list' or 'map:<pk>', where <pk> is the index position (0-based) of the column that is a the unique identifier for the column")
		skipRecords     = flag.Uint("skip-records", 0, "number of records to skip at beginning of file")
		destTypePattern = regexp.MustCompile("^(list|map):(\\d+)$")
	)

	spec.RegisterDatabaseFlags(flag.CommandLine)
	profile.RegisterProfileFlags(flag.CommandLine)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: csv-import [options] <dataset> <csvfile>?\n\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	var err error
	switch {
	case flag.NArg() == 0:
		err = errors.New("Maybe you put options after the dataset?")
	case flag.NArg() == 1 && *path == "":
		err = errors.New("If <csvfile> isn't specified, you must specify a noms path with -p")
	case flag.NArg() == 2 && *path != "":
		err = errors.New("Cannot specify both <csvfile> and a noms path with -p")
	case flag.NArg() > 2:
		err = errors.New("Too many arguments")
	}
	d.CheckError(err)

	defer profile.MaybeStartProfile().Stop()

	var r io.Reader
	var size uint64

	if *path != "" {
		db, val, err := spec.GetPath(*path)
		d.CheckError(err)
		if val == nil {
			d.CheckError(fmt.Errorf("Path %s not found\n", *path))
		}
		blob, ok := val.(types.Blob)
		if !ok {
			d.CheckError(fmt.Errorf("Path %s not a Blob: %s\n", *path, types.EncodedValue(val.Type())))
		}
		defer db.Close()
		r = blob.Reader()
		size = blob.Len()
	} else {
		res, err := os.Open(flag.Arg(1))
		d.CheckError(err)
		defer res.Close()
		fi, err := res.Stat()
		d.CheckError(err)
		r = res
		size = uint64(fi.Size())
	}

	if !*noProgress {
		r = progressreader.New(r, getStatusPrinter(size))
	}

	comma, err := csv.StringToRune(*delimiter)
	d.CheckErrorNoUsage(err)

	var dest int
	var pk int
	if *destType == "list" {
		dest = destList
	} else if match := destTypePattern.FindStringSubmatch(*destType); match != nil {
		dest = destMap
		pk, err = strconv.Atoi(match[2])
		d.CheckErrorNoUsage(err)
	} else {
		fmt.Println("Invalid dest-type: ", *destType)
		return
	}

	cr := csv.NewCSVReader(r, comma)
	for i := uint(0); i < *skipRecords; i++ {
		cr.Read()
	}

	var headers []string
	if *header == "" {
		headers, err = cr.Read()
		d.PanicIfError(err)
	} else {
		headers = strings.Split(*header, string(comma))
	}

	ds, err := spec.GetDataset(flag.Arg(0))
	d.CheckError(err)
	defer ds.Database().Close()

	kinds := []types.NomsKind{}
	if *columnTypes != "" {
		kinds = csv.StringsToKinds(strings.Split(*columnTypes, ","))
	}

	var value types.Value
	if dest == destList {
		value, _ = csv.ReadToList(cr, *name, headers, kinds, ds.Database())
	} else {
		value = csv.ReadToMap(cr, headers, pk, kinds, ds.Database())
	}
	_, err = ds.CommitValue(value)
	if !*noProgress {
		status.Clear()
	}
	d.PanicIfError(err)
}

func getStatusPrinter(expected uint64) progressreader.Callback {
	startTime := time.Now()
	return func(seen uint64) {
		percent := float64(seen) / float64(expected) * 100
		elapsed := time.Since(startTime)
		rate := float64(seen) / elapsed.Seconds()

		status.Printf("%.2f%% of %s (%s/s)...",
			percent,
			humanize.Bytes(expected),
			humanize.Bytes(uint64(rate)))
	}
}
