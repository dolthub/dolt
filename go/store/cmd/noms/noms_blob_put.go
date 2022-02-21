// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/dolthub/dolt/go/store/cmd/noms/util"
	"github.com/dolthub/dolt/go/store/config"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/profile"
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
		_, err = r.Seek(int64(i)*chunkSize, 0)

		// TODO: fix panics
		d.PanicIfError(err)

		limit := chunkSize
		if i == len(readers)-1 {
			limit += fileSize % chunkSize // adjust size of last slice to include the final bytes.
		}
		lr := io.LimitReader(r, limit)
		readers[i] = lr
	}

	cfg := config.NewResolver()
	db, vrw, ds, err := cfg.GetDataset(ctx, dsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not create dataset: %s\n", err)
		return 1
	}
	defer db.Close()

	blob, err := types.NewBlob(ctx, vrw, readers...)

	// TODO: fix panics
	d.PanicIfError(err)

	_, err = datas.CommitValue(ctx, db, ds, blob)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error committing: %s\n", err)
		return 1
	}
	return 0
}
