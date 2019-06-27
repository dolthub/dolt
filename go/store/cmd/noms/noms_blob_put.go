// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/store/cmd/noms/util"
	"io"
	"os"

	"github.com/liquidata-inc/ld/dolt/go/store/config"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/liquidata-inc/ld/dolt/go/store/util/profile"
)

func nomsBlobPut(ctx context.Context, filePath string, dsPath string, concurrency int) int {
	info, err := os.Stat(filePath)
	if err != nil {
		util.CheckError(errors.New("couldn't stat file"))
	}

	defer profile.MaybeStartProfile().Stop()

	fileSize := info.Size()
	chunkSize := fileSize / int64(concurrency)
	if chunkSize < (1 << 20) {
		chunkSize = 1 << 20
	}

	readers := make([]io.Reader, fileSize/chunkSize)
	for i := 0; i < len(readers); i++ {
		r, err := os.Open(filePath)
		util.CheckErrorNoUsage(err)
		defer r.Close()
		r.Seek(int64(i)*chunkSize, 0)
		limit := chunkSize
		if i == len(readers)-1 {
			limit += fileSize % chunkSize // adjust size of last slice to include the final bytes.
		}
		lr := io.LimitReader(r, limit)
		readers[i] = lr
	}

	cfg := config.NewResolver()
	db, ds, err := cfg.GetDataset(ctx, dsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not create dataset: %s\n", err)
		return 1
	}
	defer db.Close()

	// TODO(binformat)
	blob := types.NewBlob(ctx, types.Format_7_18, db, readers...)

	_, err = db.CommitValue(ctx, ds, blob)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error committing: %s\n", err)
		return 1
	}
	return 0
}
