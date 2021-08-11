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

package edits

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestBinarySearch(t *testing.T) {
	entryCounts := []int{11, 15, 16, 19, 31, 1024, 32151}
	for _, count := range entryCounts {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			vals := make([]entry, count)
			for i := 0; i < count; i++ {
				vals[i] = entry{kvp: &types.KVP{Key: types.Float(float64(i + 1))}}
			}

			for i := 0; i < count+1; i++ {
				idx, err := search(types.Format_Default, types.Float(float64(i)), vals)
				require.NoError(t, err)
				require.Equal(t, i, idx)

				idx, err = search(types.Format_Default, types.Float(float64(i)+0.5), vals)
				require.NoError(t, err)
				require.Equal(t, i, idx)
			}
		})
	}
}

func genReader(t *testing.T, r *rand.Rand, nbf *types.NomsBinFormat, vrw types.ValueReadWriter) (int64, types.TupleReadCloser) {
	// generate a random number of key value tuples
	numItems := r.Int63() % (32 * 1024)

	buf := bytes.NewBuffer(nil)
	wr := types.NewTupleWriter(buf)

	valTpl, err := types.NewTuple(nbf)
	require.NoError(t, err)

	// intKey increases between 0 and 999 every iteration to keep the data in sorted order
	var intKey uint64
	for i := int64(0); i < numItems; i++ {
		intKey += r.Uint64() % 1000
		keyTpl, err := types.NewTuple(nbf, types.Uint(intKey))
		require.NoError(t, err)

		err = wr.WriteTuples(keyTpl, valTpl)
		require.NoError(t, err)
	}

	return numItems, types.NewTupleReader(nbf, vrw, io.NopCloser(bytes.NewBuffer(buf.Bytes())))
}

func TestTupleStreamMerger(t *testing.T) {
	const (
		numTests   = 8
		minReaders = 2
		maxReaders = 8
	)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < numTests; i++ {
		seed := r.Int63()
		t.Run(strconv.FormatInt(seed, 10), func(t *testing.T) {
			ctx := context.Background()
			r := rand.New(rand.NewSource(seed))
			nbf := types.Format_Default
			vrw := types.NewMemoryValueStore()

			// generate a bunch of readers.  The random data within a single reader is sorted by key
			var numItems int64
			numReaders := minReaders + r.Intn(maxReaders-minReaders)
			readers := make([]types.TupleReadCloser, numReaders)

			for i := 0; i < numReaders; i++ {
				n, rd := genReader(t, r, nbf, vrw)
				numItems += n
				readers[i] = rd
			}

			// Throw in a reader with no data to check that case
			readers = append(readers, types.NewTupleReader(nbf, vrw, io.NopCloser(bytes.NewBuffer([]byte{}))))
			numReaders++

			// create a merger and iterate through all values validating that every value is less than
			// the next value read, and that we retrieved all of the data.
			merger, err := NewTupleStreamMerger(ctx, nbf, readers, numItems, nil)
			require.NoError(t, err)

			curr, err := merger.Next()
			require.NoError(t, err)
			itemsRead := int64(1)

			prevKeyVal, err := curr.Key.Value(ctx)
			require.NoError(t, err)
			for {
				curr, err = merger.Next()

				if err == io.EOF {
					break
				}
				itemsRead++

				currKeyVal, err := curr.Key.Value(ctx)
				require.NoError(t, err)

				isLess, err := prevKeyVal.Less(nbf, currKeyVal)
				require.NoError(t, err)

				require.True(t, isLess || prevKeyVal.Equals(currKeyVal))
				prevKeyVal = currKeyVal
			}

			require.Equal(t, numItems, itemsRead)
		})
	}
}
