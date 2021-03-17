// Copyright 2021 Dolthub, Inc.
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

package diff_tests

import (
	"archive/zip"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/valuefile"
)

func unzipNVF(t *testing.T) {
	const (
		dir     = "testdata"
		nvfFile = "diff_maps.nvf"
		zipFile = "diff_maps.nvf.zip"
	)

	rd, err := zip.OpenReader(filepath.Join(dir, zipFile))
	require.NoError(t, err)

	defer rd.Close()

	for _, f := range rd.File {
		if f.Name == nvfFile {
			func() {
				inFile, err := f.Open()
				require.NoError(t, err)
				defer func() {
					require.NoError(t, inFile.Close())
				}()

				outFile, err := os.Create(filepath.Join(dir, f.Name))
				require.NoError(t, err)
				defer func() {
					require.NoError(t, outFile.Close())
				}()

				_, err = io.Copy(outFile, inFile)
				require.NoError(t, err)
			}()

			break
		}
	}
}

func TestTopDownBad(t *testing.T) {
	const (
		expectedDiffCount = 8950
	)

	// file to big for github when uncompressed
	unzipNVF(t)

	ctx := context.Background()
	vals, err := valuefile.ReadValueFile(ctx, "testdata/diff_maps.nvf")
	require.NoError(t, err)

	fromRows := vals[0].(types.Map)
	toRows := vals[1].(types.Map)

	tdChanges := make(chan types.ValueChanged, 1)
	lrChanges := make(chan types.ValueChanged, 1)

	go func() {
		defer close(tdChanges)
		err = toRows.Diff(ctx, fromRows, tdChanges)
		require.NoError(t, err)
	}()

	go func() {
		defer close(lrChanges)
		err = toRows.DiffLeftRight(ctx, fromRows, lrChanges)
		require.NoError(t, err)
	}()

	var diffCount uint32
	for {
		lrDiff, lrOk := <-lrChanges
		tdDiff, tdOk := <-tdChanges

		require.Equal(t, lrOk, tdOk)

		if !lrOk {
			break
		}

		diffCount++

		requireEqualVals(t, ctx, lrDiff.Key, tdDiff.Key, "keys differ")
		requireEqualVals(t, ctx, lrDiff.NewValue, tdDiff.NewValue, "new vals differ")
		requireEqualVals(t, ctx, lrDiff.OldValue, tdDiff.OldValue, "old vals differ")
	}

	require.Equal(t, expectedDiffCount, diffCount)
}

func requireEqualVals(t *testing.T, ctx context.Context, v1, v2 types.Value, msg string) {
	if types.IsNull(v1) {
		if types.IsNull(v2) {
			return
		}

		val2Str, err := types.EncodedValue(ctx, v2)
		require.NoError(t, err)
		require.Fail(t, msg, "NULL != %s", val2Str)
	}

	if !v1.Equals(v2) {
		val1Str, err := types.EncodedValue(ctx, v1)
		require.NoError(t, err)

		val2Str, err := types.EncodedValue(ctx, v2)
		require.NoError(t, err)

		require.Fail(t, msg, "%s != %s", val1Str, val2Str)
	}
}
