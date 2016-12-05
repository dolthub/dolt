// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/progressreader"
	"github.com/attic-labs/noms/go/util/status"
	"github.com/attic-labs/noms/go/util/verbose"
	humanize "github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s <dataset> [<file>]\n", os.Args[0])
		flag.PrintDefaults()
	}

	spec.RegisterDatabaseFlags(flag.CommandLine)
	verbose.RegisterVerboseFlags(flag.CommandLine)
	flag.Parse(true)

	if flag.NArg() != 1 && flag.NArg() != 2 {
		d.CheckError(errors.New("expected dataset and optional file flag"))
	}

	cfg := config.NewResolver()
	var blob types.Blob
	path := flag.Arg(0)
	if db, val, err := cfg.GetPath(path); err != nil {
		d.CheckErrorNoUsage(err)
	} else if val == nil {
		d.CheckErrorNoUsage(fmt.Errorf("No value at %s", path))
	} else if b, ok := val.(types.Blob); !ok {
		d.CheckErrorNoUsage(fmt.Errorf("Value at %s is not a blob", path))
	} else {
		defer db.Close()
		blob = b
	}

	file := os.Stdout
	blobReader := blob.Reader().(io.Reader)
	showProgress := false

	filePath := flag.Arg(1)
	if filePath != "" {
		// Note: overwrites any existing file.
		var err error
		file, err = os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
		d.CheckErrorNoUsage(err)
		defer file.Close()

		showProgress = true
		start := time.Now()
		expected := humanize.Bytes(blob.Len())
		blobReader = progressreader.New(blob.Reader(), func(seen uint64) {
			elapsed := time.Since(start).Seconds()
			rate := uint64(float64(seen) / elapsed)
			status.Printf("%s of %s written in %ds (%s/s)...", humanize.Bytes(seen), expected, int(elapsed), humanize.Bytes(rate))
		})
	}

	io.Copy(file, blobReader)
	if showProgress {
		status.Done()
	}
}
