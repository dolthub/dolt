// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"os"
	"path"

	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/util/exit"
	"github.com/attic-labs/noms/go/util/verbose"
	"github.com/attic-labs/noms/samples/go/photo-dedup/job"

	flag "github.com/juju/gnuflag"
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s -db=<db-spec> -out-ds=<name> <input-paths...>\n\n", path.Base(os.Args[0]))
	fmt.Fprintf(os.Stderr, "Annotates each Photo in input-paths with a dhash field\n\n")
	fmt.Fprintf(os.Stderr, "  <input-paths...> : One or more input paths within <db-spec>\n")
	fmt.Fprintf(os.Stderr, "  <db>             : Database to work with\n")
	fmt.Fprintf(os.Stderr, "  <out-ds>         : Dataset to write photos groups to\n")
	fmt.Fprintf(os.Stderr, "  <input-paths...> : One or more input paths within <db-spec>\n\n")

	flag.PrintDefaults()
}

func main() {
	var dbStr = flag.String("db", "", "input database spec")
	var outDSStr = flag.String("out-ds", "", "output dataset to write to")
	verbose.RegisterVerboseFlags(flag.CommandLine)

	flag.Usage = usage
	flag.Parse(false)

	if flag.NArg() == 0 {
		flag.Usage()
		return
	}

	cfg := config.NewResolver()
	db, err := cfg.GetDatabase(*dbStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid input database '%s': %s\n", flag.Arg(0), err)
		return
	}
	defer db.Close()

	var outDS datas.Dataset
	if !datas.IsValidDatasetName(*outDSStr) {
		fmt.Fprintf(os.Stderr, "Invalid output dataset name: %s\n", *outDSStr)
		return
	} else {
		outDS = db.GetDataset(*outDSStr)
	}

	inputs, err := spec.ReadAbsolutePaths(db, flag.Args()...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return
	}

	if err = job.HashPhotosJob(db, inputs, outDS); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		exit.Fail()
	}
}
