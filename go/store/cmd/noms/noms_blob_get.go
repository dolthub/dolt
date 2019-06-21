// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/liquidata-inc/ld/dolt/go/store/config"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/liquidata-inc/ld/dolt/go/store/util/profile"
	"github.com/liquidata-inc/ld/dolt/go/store/util/progressreader"
	"github.com/liquidata-inc/ld/dolt/go/store/util/status"
)

func nomsBlobGet(ctx context.Context, ds string, filePath string) int {
	cfg := config.NewResolver()
	var blob types.Blob
	if db, val, err := cfg.GetPath(ctx, ds); err != nil {
		d.CheckErrorNoUsage(err)
	} else if val == nil {
		d.CheckErrorNoUsage(fmt.Errorf("No value at %s", ds))
	} else if b, ok := val.(types.Blob); !ok {
		d.CheckErrorNoUsage(fmt.Errorf("Value at %s is not a blob", ds))
	} else {
		defer db.Close()
		blob = b
	}

	defer profile.MaybeStartProfile().Stop()

	if filePath == "" {
		blob.Copy(ctx, os.Stdout)
		return 0
	}

	// Note: overwrites any existing file.
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
	d.CheckErrorNoUsage(err)
	defer file.Close()

	start := time.Now()
	expected := humanize.Bytes(blob.Len())

	// Create a pipe so that we can connect a progress reader
	preader, pwriter := io.Pipe()

	go func() {
		blob.Copy(ctx, pwriter)
		pwriter.Close()
	}()

	blobReader := progressreader.New(preader, func(seen uint64) {
		elapsed := time.Since(start).Seconds()
		rate := uint64(float64(seen) / elapsed)
		status.Printf("%s of %s written in %ds (%s/s)...", humanize.Bytes(seen), expected, int(elapsed), humanize.Bytes(rate))
	})

	io.Copy(file, blobReader)
	status.Done()
	return 0
}
