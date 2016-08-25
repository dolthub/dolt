// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/profile"
	"github.com/attic-labs/noms/go/util/progressreader"
	"github.com/attic-labs/noms/go/util/status"
	"github.com/attic-labs/noms/samples/go/csv"
	humanize "github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"
)

const (
	dateFormat = "2006-01-02T15:04:05-0700"
	destList   = iota
	destMap    = iota
)

func main() {
	// Actually the delimiter uses runes, which can be multiple characters long.
	// https://blog.golang.org/strings
	delimiter := flag.String("delimiter", ",", "field delimiter for csv file, must be exactly one character long.")
	comment := flag.String("comment", "", "comment to add to commit's meta data")
	header := flag.String("header", "", "header row. If empty, we'll use the first row of the file")
	name := flag.String("name", "Row", "struct name. The user-visible name to give to the struct type that will hold each row of data.")
	columnTypes := flag.String("column-types", "", "a comma-separated list of types representing the desired type of each column. if absent all types default to be String")
	pathDescription := "noms path to blob to import"
	path := flag.String("path", "", pathDescription)
	flag.StringVar(path, "p", "", pathDescription)
	dateFlag := flag.String("date", "", fmt.Sprintf(`date of commit in ISO 8601 format ("%s"). By default, the current date is used.`, dateFormat))
	noProgress := flag.Bool("no-progress", false, "prevents progress from being output if true")
	destType := flag.String("dest-type", "list", "the destination type to import to. can be 'list' or 'map:<pk>', where <pk> is the index position (0-based) of the column that is a the unique identifier for the column")
	skipRecords := flag.Uint("skip-records", 0, "number of records to skip at beginning of file")

	spec.RegisterDatabaseFlags(flag.CommandLine)
	profile.RegisterProfileFlags(flag.CommandLine)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: csv-import [options] <csvfile> <dataset>\n\n")
		flag.PrintDefaults()
	}

	flag.Parse(true)

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

	var date = *dateFlag
	if date == "" {
		date = time.Now().UTC().Format(dateFormat)
	} else {
		_, err := time.Parse(dateFormat, date)
		d.CheckErrorNoUsage(err)
	}

	defer profile.MaybeStartProfile().Stop()

	var r io.Reader
	var size uint64
	var filePath string
	var dataSetArgN int

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
		dataSetArgN = 0
	} else {
		filePath = flag.Arg(0)
		res, err := os.Open(filePath)
		d.CheckError(err)
		defer res.Close()
		fi, err := res.Stat()
		d.CheckError(err)
		r = res
		size = uint64(fi.Size())
		dataSetArgN = 1
	}

	if !*noProgress {
		r = progressreader.New(r, getStatusPrinter(size))
	}

	comma, err := csv.StringToRune(*delimiter)
	d.CheckErrorNoUsage(err)

	var dest int
	var strPks []string
	if *destType == "list" {
		dest = destList
	} else if strings.HasPrefix(*destType, "map:") {
		dest = destMap
		strPks = strings.Split(strings.TrimPrefix(*destType, "map:"), ",")
		if len(strPks) == 0 {
			fmt.Println("Invalid dest-type map: ", *destType)
			return
		}
	} else {
		fmt.Println("Invalid dest-type: ", *destType)
		return
	}

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
	if *columnTypes != "" {
		kinds = csv.StringsToKinds(strings.Split(*columnTypes, ","))
	}

	ds, err := spec.GetDataset(flag.Arg(dataSetArgN))
	d.CheckError(err)
	defer ds.Database().Close()

	var value types.Value
	if dest == destList {
		value, _ = csv.ReadToList(cr, *name, headers, kinds, ds.Database())
	} else {
		value = csv.ReadToMap(cr, *name, headers, strPks, kinds, ds.Database())
	}
	mi := metaInfoForCommit(date, filePath, *path, *comment)
	_, err = ds.Commit(value, dataset.CommitOptions{Meta: mi})
	if !*noProgress {
		status.Clear()
	}
	d.PanicIfError(err)
}

func metaInfoForCommit(date, filePath, nomsPath, comment string) types.Struct {
	fileOrNomsPath := "inputPath"
	path := nomsPath
	if path == "" {
		path = filePath
		fileOrNomsPath = "inputFile"
	}
	metaValues := types.StructData{
		"date":         types.String(date),
		fileOrNomsPath: types.String(path),
	}
	if comment != "" {
		metaValues["comment"] = types.String(comment)
	}
	return types.NewStruct("Meta", metaValues)
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
