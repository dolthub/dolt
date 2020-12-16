// Copyright 2020 Dolthub, Inc.
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

package store

/*import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	simIdxBenchDataset = "simulated_index_benchmark"
	numRows            = 100000
	rangeSize          = 10
)

func poe(err error) {
	if err != nil {
		panic(err)
	}
}

var benchmarkTmpDir = os.TempDir()

func getBenchmarkDB(ctx context.Context) datas.Database {
	cs, err := nbs.NewLocalStore(ctx, types.Format_Default.VersionString(), benchmarkTmpDir, 1<<28)
	poe(err)

	return datas.NewDatabase(nbs.NewNBSMetricWrapper(cs))
}

func writeTupleToDB(ctx context.Context, db datas.Database, dsID string, vals ...types.Value) {
	root, err := types.NewTuple(db.Format(), vals...)
	poe(err)

	ds, err := db.GetDataset(ctx, dsID)
	poe(err)

	_, err = db.CommitValue(ctx, ds, root)
	poe(err)
}

func readTupleFromDB(ctx context.Context, t require.TestingT, dsID string) (*types.NomsBinFormat, []types.Value) {
	db := getBenchmarkDB(ctx)
	ds, err := db.GetDataset(ctx, dsID)
	require.NoError(t, err)

	ref, ok, err := ds.MaybeHeadRef()
	require.NoError(t, err)
	require.True(t, ok)

	val, err := ref.TargetValue(ctx, db)
	require.NoError(t, err)

	st := val.(types.Struct)
	val, ok, err = st.MaybeGet("value")
	require.NoError(t, err)
	require.True(t, ok)
	tup := val.(types.Tuple)
	valSlice, err := tup.AsSlice()
	require.NoError(t, err)
	return db.Format(), valSlice
}

func init() {
	ctx := context.Background()
	db := getBenchmarkDB(ctx)
	nbf := db.Format()

	m, err := types.NewMap(ctx, db)
	poe(err)

	idx, err := types.NewMap(ctx, db)
	poe(err)

	me := m.Edit()
	idxMe := idx.Edit()
	rng := rand.New(rand.NewSource(0))
	for i := 0; i <= numRows; i++ {
		k, err := types.NewTuple(nbf, types.Uint(0), types.Int(int64(i)))
		poe(err)
		randf := rng.Float64()
		v, err := types.NewTuple(nbf, types.Uint(1), types.Float(randf), types.Uint(2), types.Bool(i%2 == 0), types.Uint(3), types.String(uuid.New().String()), types.Uint(4), types.Timestamp(time.Now()))
		poe(err)
		idxKey, err := types.NewTuple(nbf, types.Uint(5), types.Float(randf), types.Uint(0), types.Int(int64(i)))
		poe(err)

		me = me.Set(k, v)
		idxMe = idxMe.Set(idxKey, types.NullValue)
	}

	m, err = me.Map(ctx)
	poe(err)

	idx, err = idxMe.Map(ctx)
	poe(err)

	writeTupleToDB(ctx, db, simIdxBenchDataset, m, idx)
}

func BenchmarkSimulatedIndex(b *testing.B) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(0))
	nbf, vals := readTupleFromDB(ctx, b, simIdxBenchDataset)

	m := vals[0].(types.Map)
	idx := vals[1].(types.Map)

	b.ResetTimer()

	var idxItr types.MapIterator
	for i := 0; i < b.N; i++ {
		randf := rng.Float64()
		rangeStartKey, err := types.NewTuple(nbf, types.Uint(5), types.Float(randf))
		require.NoError(b, err)
		idxItr, err = idx.IteratorFrom(ctx, rangeStartKey)
		require.NoError(b, err)

		for j := 0; j < rangeSize; j++ {
			idxKey, _, err := idxItr.Next(ctx)
			require.NoError(b, err)

			if idxKey == nil {
				break
			}

			vals, err := idxKey.(types.Tuple).AsSlice()
			require.NoError(b, err)
			keyTup, err := types.NewTuple(nbf, vals[2:]...)

			k, _, err := m.MaybeGet(ctx, keyTup)
			require.NoError(b, err)
			require.NotNil(b, k)
		}
	}
}

func BenchmarkSimulatedCoveringIndex(b *testing.B) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(0))
	nbf, vals := readTupleFromDB(ctx, b, simIdxBenchDataset)

	idx := vals[1].(types.Map)

	b.ResetTimer()

	var idxItr types.MapIterator
	for i := 0; i < b.N; i++ {
		randf := rng.Float64()
		rangeStartKey, err := types.NewTuple(nbf, types.Uint(5), types.Float(randf))
		require.NoError(b, err)
		idxItr, err = idx.IteratorFrom(ctx, rangeStartKey)
		require.NoError(b, err)

		for j := 0; j < rangeSize; j++ {
			idxKey, _, err := idxItr.Next(ctx)
			require.NoError(b, err)

			if idxKey == nil {
				break
			}
		}
	}
}*/
