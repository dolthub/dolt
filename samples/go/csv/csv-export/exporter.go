// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/profile"
	"github.com/attic-labs/noms/go/util/verbose"
	"github.com/attic-labs/noms/samples/go/csv"
	flag "github.com/juju/gnuflag"
)

func main() {
	// Actually the delimiter uses runes, which can be multiple characters long.
	// https://blog.golang.org/strings
	delimiter := flag.String("delimiter", ",", "field delimiter for csv file, must be exactly one character long.")

	spec.RegisterDatabaseFlags(flag.CommandLine)
	verbose.RegisterVerboseFlags(flag.CommandLine)
	profile.RegisterProfileFlags(flag.CommandLine)

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: csv-export [options] dataset > filename")
		flag.PrintDefaults()
	}

	flag.Parse(true)

	if flag.NArg() != 1 {
		d.CheckError(errors.New("expected dataset arg"))
	}

	cfg := config.NewResolver()
	db, ds, err := cfg.GetDataset(flag.Arg(0))
	d.CheckError(err)

	defer db.Close()

	comma, err := csv.StringToRune(*delimiter)
	d.CheckError(err)

	err = d.Try(func() {
		defer profile.MaybeStartProfile().Stop()

		hv := ds.HeadValue()
		if l, ok := hv.(types.List); ok {
			structDesc := csv.GetListElemDesc(l, db)
			csv.WriteList(l, structDesc, comma, os.Stdout)
		} else if m, ok := hv.(types.Map); ok {
			structDesc := csv.GetMapElemDesc(m, db)
			csv.WriteMap(m, structDesc, comma, os.Stdout)
		} else {
			panic(fmt.Sprintf("Expected ListKind or MapKind, found %s", types.KindToString[hv.Type().Kind()]))
		}
	})
	if err != nil {
		fmt.Println("Failed to export dataset as CSV:")
		fmt.Println(err)
	}
}
