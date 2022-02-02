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

import (
	"context"
	"io"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

func poe(err error) {
	if err != nil {
		panic(err)
	}
}

func getDBAtDir(ctx context.Context, dir string) (datas.Database, types.ValueReadWriter) {
	cs, err := nbs.NewLocalStore(ctx, types.Format_Default.VersionString(), dir, 1<<28)
	poe(err)

	vrw := types.NewValueStore(nbs.NewNBSMetricWrapper(cs))
	return datas.NewTypesDatabase(vrw), vrw
}

const (
	simIdxBenchDataset = "simulated_index_benchmark"
	numRows            = 100000
	rangeSize          = 10
)

var benchmarkTmpDir = os.TempDir()
var genOnce = &sync.Once{}

func getBenchmarkDB(ctx context.Context) (datas.Database, types.ValueReadWriter) {
	return getDBAtDir(ctx, benchmarkTmpDir)
}

func writeTupleToDB(ctx context.Context, db datas.Database, vrw types.ValueReadWriter, dsID string, vals ...types.Value) {
	root, err := types.NewTuple(vrw.Format(), vals...)
	poe(err)

	ds, err := db.GetDataset(ctx, dsID)
	poe(err)

	_, err = datas.CommitValue(ctx, db, ds, root)
	poe(err)
}

func readTupleFromDB(ctx context.Context, t require.TestingT, dsID string) (*types.NomsBinFormat, []types.Value) {
	db, vrw := getBenchmarkDB(ctx)
	ds, err := db.GetDataset(ctx, dsID)
	require.NoError(t, err)

	ref, ok, err := ds.MaybeHeadRef()
	require.NoError(t, err)
	require.True(t, ok)

	val, err := ref.TargetValue(ctx, vrw)
	require.NoError(t, err)

	st := val.(types.Struct)
	val, ok, err = st.MaybeGet("value")
	require.NoError(t, err)
	require.True(t, ok)
	tup := val.(types.Tuple)
	valSlice, err := tup.AsSlice()
	require.NoError(t, err)
	return vrw.Format(), valSlice
}

var testDataCols = []schema.Column{
	schema.NewColumn("id", 0, types.IntKind, true),
	schema.NewColumn("fColh", 1, types.FloatKind, false),
	schema.NewColumn("bCol", 2, types.BoolKind, false),
	schema.NewColumn("uuidStrCol", 3, types.StringKind, false),
	schema.NewColumn("timeCol", 4, types.TimestampKind, false),
	schema.NewColumn("colInt1", 6, types.IntKind, false),
	schema.NewColumn("colInt2", 7, types.IntKind, false),
	schema.NewColumn("colInt3", 8, types.IntKind, false),
	schema.NewColumn("colInt4", 9, types.IntKind, false),
}

func generateTestData(ctx context.Context) {
	genOnce.Do(func() {
		db, vrw := getBenchmarkDB(ctx)
		nbf := vrw.Format()

		m, err := types.NewMap(ctx, vrw)
		poe(err)

		idx, err := types.NewMap(ctx, vrw)
		poe(err)

		me := m.Edit()
		idxMe := idx.Edit()
		rng := rand.New(rand.NewSource(0))
		for i := 0; i <= numRows; i++ {
			k, err := types.NewTuple(nbf, types.Uint(0), types.Int(int64(i)))
			poe(err)
			randf := rng.Float64()
			v, err := types.NewTuple(nbf, types.Uint(1), types.Float(randf), types.Uint(2), types.Bool(i%2 == 0), types.Uint(3), types.String(uuid.New().String()), types.Uint(4), types.Timestamp(time.Now()), types.Uint(6), types.Int(-100), types.Uint(7), types.Int(-1000), types.Uint(8), types.Int(-10000), types.Uint(9), types.Int(-1000000))
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

		writeTupleToDB(ctx, db, vrw, simIdxBenchDataset, m, idx)
	})
}

func BenchmarkSimulatedIndex(b *testing.B) {
	ctx := context.Background()
	generateTestData(ctx)

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
	generateTestData(ctx)

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
}

func BenchmarkMapItr(b *testing.B) {
	ctx := context.Background()
	generateTestData(ctx)

	require.True(b, b.N < numRows, "b.N:%d >= numRows:%d", b.N, numRows)

	_, vals := readTupleFromDB(ctx, b, simIdxBenchDataset)
	m := vals[0].(types.Map)

	itr, err := m.RangeIterator(ctx, 0, uint64(b.N))
	require.NoError(b, err)

	var closeFunc func() error
	if cl, ok := itr.(io.Closer); ok {
		closeFunc = cl.Close
	}

	sch, err := schema.SchemaFromCols(schema.NewColCollection(testDataCols...))
	require.NoError(b, err)

	dmItr := index.NewDoltMapIter(itr.NextTuple, closeFunc, index.NewKVToSqlRowConverterForCols(m.Format(), sch))
	sqlCtx := sql.NewContext(ctx)

	b.ResetTimer()
	for {
		var r sql.Row
		r, err = dmItr.Next(sqlCtx)

		if r == nil || err != nil {
			break
		}
	}
	b.StopTimer()

	if err != io.EOF {
		require.NoError(b, err)
	}
	dmItr.Close(sqlCtx)
}

/*func BenchmarkFullScan(b *testing.B) {
	const dir = "dolt directory containing db with table to scan"
	const branch = "master"
	const tableName = "bigram_counts"

	ctx := context.Background()
	ddb, err := doltdb.LoadDoltDB(ctx, types.Format_Default, dir)
	require.NoError(b, err)

	cs, err := doltdb.NewCommitSpec("HEAD")
	require.NoError(b, err)

	cm, err := ddb.Resolve(ctx, cs, ref.NewBranchRef(branch))
	require.NoError(b, err)

	root, err := cm.GetRootValue()
	require.NoError(b, err)

	tbl, ok, err := root.GetTable(ctx, tableName)
	require.NoError(b, err)
	require.True(b, ok)

	m, err := tbl.GetNomsRowData(ctx)
	require.NoError(b, err)
	require.True(b, uint64(b.N) < m.Len(), "b.N:%d >= numRows:%d", b.N, m.Len())

	itr, err := m.RangeIterator(ctx, 0, uint64(b.N))
	require.NoError(b, err)

	dmItr := sqle.NewDoltMapIter(ctx, itr.NextTuple, closeFunc, sqle.NewKVToSqlRowConverterForCols(m.Format(), testDataCols))

	b.ResetTimer()
	for {
		var r sql.Row
		r, err = dmItr.Next()

		if r == nil || err != nil {
			break
		}
	}
	b.StopTimer()

	if err != io.EOF {
		require.NoError(b, err)
	}
}*/
