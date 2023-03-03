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
			ctx := context.Background()
			vrw := types.NewMemoryValueStore()
			vals := make([]entry, count)
			for i := 0; i < count; i++ {
				vals[i] = entry{key: types.Float(float64(i + 1))}
			}

			for i := 0; i < count+1; i++ {
				idx, err := search(ctx, vrw, 0, types.Float(float64(i)), vals)
				require.NoError(t, err)
				require.Equal(t, i, idx)

				idx, err = search(ctx, vrw, 0, types.Float(float64(i)+0.5), vals)
				require.NoError(t, err)
				require.Equal(t, i, idx)
			}

			// test that in the case of equality that an earlier reader index returns as being less
			for i := 1; i < count+1; i++ {
				idx, err := search(ctx, vrw, -1, types.Float(float64(i)), vals)
				require.NoError(t, err)
				require.Equal(t, i-1, idx)

				idx, err = search(ctx, vrw, -1, types.Float(float64(i)+0.5), vals)
				require.NoError(t, err)
				require.Equal(t, i, idx)
			}
		})
	}
}

func readerForTuples(t *testing.T, ctx context.Context, vrw types.ValueReadWriter, tuples ...types.Tuple) types.TupleReadCloser {
	require.True(t, len(tuples)%2 == 0)
	prev := tuples[0]
	for i := 2; i < len(tuples); i += 2 {
		isLess, err := prev.Less(ctx, vrw.Format(), tuples[i])
		require.NoError(t, err)
		require.True(t, isLess)
		prev = tuples[i]
	}

	buf := bytes.NewBuffer(nil)
	wr := types.NewTupleWriter(buf)

	err := wr.WriteTuples(tuples...)
	require.NoError(t, err)

	return types.NewTupleReader(vrw.Format(), vrw, io.NopCloser(bytes.NewBuffer(buf.Bytes())))
}

func newTuple(t *testing.T, nbf *types.NomsBinFormat, vals ...types.Value) types.Tuple {
	tpl, err := types.NewTuple(nbf, vals...)
	require.NoError(t, err)
	return tpl
}

func TestComparableBinarySearch(t *testing.T) {
	ctx := context.Background()
	nbf := types.Format_Default
	vrw := types.NewMemoryValueStore()

	readers := []types.EditProvider{
		types.TupleReaderAsEditProvider(readerForTuples(t, ctx, vrw, []types.Tuple{
			newTuple(t, nbf), newTuple(t, nbf, types.Int(0)),
			newTuple(t, nbf, types.Bool(false)), newTuple(t, nbf, types.Int(2)),
			newTuple(t, nbf, types.Float(1.0)), newTuple(t, nbf, types.Int(5)),
			newTuple(t, nbf, types.String("zz")), newTuple(t, nbf, types.Int(9)),
			newTuple(t, nbf, types.UUID{}), newTuple(t, nbf, types.Int(11)),
		}...)),
		types.TupleReaderAsEditProvider(readerForTuples(t, ctx, vrw, []types.Tuple{
			newTuple(t, nbf), newTuple(t, nbf, types.Int(1)),
			newTuple(t, nbf, types.Bool(true)), newTuple(t, nbf, types.Int(4)),
			newTuple(t, nbf, types.Float(2.0)), newTuple(t, nbf, types.Int(6)),
			newTuple(t, nbf, types.String("zz")), newTuple(t, nbf, types.Int(10)),
			newTuple(t, nbf, types.UUID{}), newTuple(t, nbf, types.Int(12)),
		}...)),
		types.TupleReaderAsEditProvider(readerForTuples(t, ctx, vrw, []types.Tuple{
			newTuple(t, nbf, types.Bool(false)), newTuple(t, nbf, types.Int(3)),
			newTuple(t, nbf, types.Float(2.0)), newTuple(t, nbf, types.Int(7)),
			newTuple(t, nbf, types.String("aaa")), newTuple(t, nbf, types.Int(8)),
			newTuple(t, nbf, types.UUID{}), newTuple(t, nbf, types.Int(13)),
		}...)),
	}

	const numItems = 14

	// create a merger and iterate through all values validating that every value is less than
	// the next value read, and that we retrieved all of the data.
	merger, err := NewEPMerger(ctx, vrw, readers)
	require.NoError(t, err)

	items := testMergeOrder(t, ctx, vrw, merger)
	require.Equal(t, numItems, len(items))

	for i := 0; i < len(items); i++ {
		v, err := items[i].Val.Value(ctx)
		require.NoError(t, err)

		itr, err := v.(types.Tuple).Iterator()
		require.NoError(t, err)

		_, idxVal, err := itr.Next()
		require.NoError(t, err)
		require.Equal(t, int64(i), int64(idxVal.(types.Int)))
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
			readers := make([]types.EditProvider, numReaders)

			for i := 0; i < numReaders; i++ {
				n, rd := genReader(t, r, nbf, vrw)
				numItems += n
				readers[i] = types.TupleReaderAsEditProvider(rd)
			}

			// Throw in a reader with no data to check that case
			readers = append(readers, types.TupleReaderAsEditProvider(types.NewTupleReader(nbf, vrw, bytes.NewBuffer([]byte{}))))
			numReaders++

			// create a merger and iterate through all values validating that every value is less than
			// the next value read, and that we retrieved all of the data.
			merger, err := NewEPMerger(ctx, vrw, readers)
			require.NoError(t, err)

			items := testMergeOrder(t, ctx, vrw, merger)
			require.Equal(t, numItems, int64(len(items)))
		})
	}
}

func testMergeOrder(t *testing.T, ctx context.Context, vr types.ValueReader, merger types.EditProvider) []*types.KVP {
	curr, err := merger.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, curr)

	prevKeyVal, err := curr.Key.Value(ctx)
	require.NoError(t, err)

	var items []*types.KVP
	items = append(items, curr)
	for {
		curr, err = merger.Next(ctx)
		if err == io.EOF {
			break
		}
		items = append(items, curr)

		currKeyVal, err := curr.Key.Value(ctx)
		require.NoError(t, err)

		isLess, err := prevKeyVal.Less(ctx, vr.Format(), currKeyVal)
		require.NoError(t, err)

		require.True(t, isLess || prevKeyVal.Equals(currKeyVal))
		prevKeyVal = currKeyVal
	}

	return items
}
