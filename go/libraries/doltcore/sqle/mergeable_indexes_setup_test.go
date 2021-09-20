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

package sqle

import (
	"context"
	"fmt"
	"strings"
	"testing"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/lookup"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

func setupMergeableIndexes(t *testing.T, tableName, insertQuery string) (*sqle.Engine, *env.DoltEnv, *testMergeableIndexDb, []*indexTuple, *doltdb.RootValue) {
	dEnv := dtestutils.CreateTestEnv()
	root, err := dEnv.WorkingRoot(context.Background())
	require.NoError(t, err)
	opts := editor.Options{Deaf: dEnv.DbEaFactory()}
	db := NewDatabase("dolt", dEnv.DbData(), opts)
	engine, sqlCtx, err := NewTestEngine(t, dEnv, context.Background(), db, root)
	require.NoError(t, err)

	_, iter, err := engine.Query(sqlCtx, fmt.Sprintf(`CREATE TABLE %s (
		pk bigint PRIMARY KEY,
		v1 bigint,
		v2 bigint,
		INDEX idxv1 (v1),
		INDEX idxv2v1 (v2,v1)
	)`, tableName))
	require.NoError(t, err)
	require.NoError(t, drainIter(sqlCtx, iter))

	_, iter, err = engine.Query(sqlCtx, insertQuery)
	require.NoError(t, err)
	require.NoError(t, drainIter(sqlCtx, iter))

	sqlTbl, ok, err := db.GetTableInsensitive(sqlCtx, tableName)
	require.NoError(t, err)
	require.True(t, ok)
	tbl, ok := sqlTbl.(*AlterableDoltTable)
	require.True(t, ok)

	idxv1, ok := tbl.sch.Indexes().GetByNameCaseInsensitive("idxv1")
	require.True(t, ok)

	table, err := tbl.doltTable(sqlCtx)
	require.NoError(t, err)

	idxv1RowData, err := table.GetIndexRowData(context.Background(), idxv1.Name())
	require.NoError(t, err)
	idxv1Cols := make([]schema.Column, idxv1.Count())
	for i, tag := range idxv1.IndexedColumnTags() {
		idxv1Cols[i], _ = idxv1.GetColumn(tag)
	}
	idxv1ToTuple := &indexTuple{
		nbf:  idxv1RowData.Format(),
		cols: idxv1Cols,
	}

	idxv2v1, ok := tbl.sch.Indexes().GetByNameCaseInsensitive("idxv2v1")
	require.True(t, ok)
	idxv2v1RowData, err := table.GetIndexRowData(context.Background(), idxv2v1.Name())
	require.NoError(t, err)
	idxv2v1Cols := make([]schema.Column, idxv2v1.Count())
	for i, tag := range idxv2v1.IndexedColumnTags() {
		idxv2v1Cols[i], _ = idxv2v1.GetColumn(tag)
	}
	idxv2v1ToTuple := &indexTuple{
		nbf:  idxv2v1RowData.Format(),
		cols: idxv2v1Cols,
	}

	mergeableDb := &testMergeableIndexDb{
		t:        t,
		tbl:      tbl,
		editOpts: opts,
	}
	pro := NewDoltDatabaseProvider(dEnv.Config, mergeableDb)
	engine = sqle.NewDefault(pro)

	// Get an updated root to use for the rest of the test
	ctx := sql.NewEmptyContext()
	roots, ok := dsess.DSessFromSess(sqlCtx.Session).GetRoots(ctx, mergeableDb.Name())
	require.True(t, ok)

	return engine, dEnv, mergeableDb, []*indexTuple{
		idxv1ToTuple,
		idxv2v1ToTuple,
		{
			nbf:  idxv2v1RowData.Format(),
			cols: idxv2v1Cols[:len(idxv2v1Cols)-1],
		},
	}, roots.Working
}

// Database made to test mergeable indexes while using the full SQL engine.
type testMergeableIndexDb struct {
	t           *testing.T
	tbl         *AlterableDoltTable
	finalRanges func([]lookup.Range) // We return the final range set to compare to the expected ranges
	editOpts    editor.Options
}

func (db *testMergeableIndexDb) EditOptions() editor.Options {
	return db.editOpts
}

func (db *testMergeableIndexDb) Name() string {
	return "dolt"
}

func (db *testMergeableIndexDb) GetTableInsensitive(_ *sql.Context, tblName string) (sql.Table, bool, error) {
	if strings.ToLower(tblName) == strings.ToLower(db.tbl.tableName) {
		return &testMergeableIndexTable{
			AlterableDoltTable: db.tbl,
			t:                  db.t,
			finalRanges:        db.finalRanges,
		}, true, nil
	}
	return nil, false, nil
}
func (db *testMergeableIndexDb) GetTableNames(_ *sql.Context) ([]string, error) {
	return []string{db.tbl.tableName}, nil
}

// Table made to test mergeable indexes by intercepting specific index-related functions.
type testMergeableIndexTable struct {
	*AlterableDoltTable
	t           *testing.T
	il          *testMergeableIndexLookup
	finalRanges func([]lookup.Range) // We return the final range set to compare to the expected ranges
}

var _ sql.IndexedTable = (*testMergeableIndexTable)(nil)

func (tbl *testMergeableIndexTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	indexes, err := tbl.AlterableDoltTable.GetIndexes(ctx)
	if err != nil {
		return nil, err
	}
	for i, index := range indexes {
		indexes[i] = &testMergeableDoltIndex{
			doltIndex:   index.(*doltIndex),
			t:           tbl.t,
			finalRanges: tbl.finalRanges,
		}
	}
	return indexes, nil
}

func (tbl *testMergeableIndexTable) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	il, ok := lookup.(*testMergeableIndexLookup)
	require.True(tbl.t, ok)
	return &testMergeableIndexTable{
		AlterableDoltTable: tbl.AlterableDoltTable,
		t:                  tbl.t,
		il:                 il,
		finalRanges:        tbl.finalRanges,
	}
}

type testProjectedMergableIndexTable struct {
	*testMergeableIndexTable
	cols []string
}

func (tbl *testMergeableIndexTable) WithProjection(colNames []string) sql.Table {
	return &testProjectedMergableIndexTable{tbl, colNames}
}

func (tbl *testMergeableIndexTable) Partitions(_ *sql.Context) (sql.PartitionIter, error) {
	rowData := tbl.il.IndexRowData()
	return sqlutil.NewSinglePartitionIter(rowData), nil
}

func (tbl *testMergeableIndexTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return tbl.il.RowIter(ctx, part.(sqlutil.SinglePartition).RowData)
}

// Index made to test mergeable indexes by intercepting all calls that return lookups and returning modified lookups.
type testMergeableDoltIndex struct {
	*doltIndex
	t           *testing.T
	finalRanges func([]lookup.Range) // We return the final range set to compare to the expected ranges
}

func (di *testMergeableDoltIndex) Get(keys ...interface{}) (sql.IndexLookup, error) {
	indexLookup, err := di.doltIndex.Get(keys...)
	return &testMergeableIndexLookup{
		doltIndexLookup: indexLookup.(*doltIndexLookup),
		t:               di.t,
		finalRanges:     di.finalRanges,
	}, err
}
func (di *testMergeableDoltIndex) Not(keys ...interface{}) (sql.IndexLookup, error) {
	indexLookup, err := di.doltIndex.Not(keys...)
	return &testMergeableIndexLookup{
		doltIndexLookup: indexLookup.(*doltIndexLookup),
		t:               di.t,
		finalRanges:     di.finalRanges,
	}, err
}
func (di *testMergeableDoltIndex) AscendGreaterOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	indexLookup, err := di.doltIndex.AscendGreaterOrEqual(keys...)
	return &testMergeableIndexLookup{
		doltIndexLookup: indexLookup.(*doltIndexLookup),
		t:               di.t,
		finalRanges:     di.finalRanges,
	}, err
}
func (di *testMergeableDoltIndex) AscendLessThan(keys ...interface{}) (sql.IndexLookup, error) {
	indexLookup, err := di.doltIndex.AscendLessThan(keys...)
	return &testMergeableIndexLookup{
		doltIndexLookup: indexLookup.(*doltIndexLookup),
		t:               di.t,
		finalRanges:     di.finalRanges,
	}, err
}
func (di *testMergeableDoltIndex) AscendRange(greaterOrEqual, lessThanOrEqual []interface{}) (sql.IndexLookup, error) {
	indexLookup, err := di.doltIndex.AscendRange(greaterOrEqual, lessThanOrEqual)
	return &testMergeableIndexLookup{
		doltIndexLookup: indexLookup.(*doltIndexLookup),
		t:               di.t,
		finalRanges:     di.finalRanges,
	}, err
}
func (di *testMergeableDoltIndex) DescendGreater(keys ...interface{}) (sql.IndexLookup, error) {
	indexLookup, err := di.doltIndex.DescendGreater(keys...)
	return &testMergeableIndexLookup{
		doltIndexLookup: indexLookup.(*doltIndexLookup),
		t:               di.t,
		finalRanges:     di.finalRanges,
	}, err
}
func (di *testMergeableDoltIndex) DescendLessOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	indexLookup, err := di.doltIndex.DescendLessOrEqual(keys...)
	return &testMergeableIndexLookup{
		doltIndexLookup: indexLookup.(*doltIndexLookup),
		t:               di.t,
		finalRanges:     di.finalRanges,
	}, err
}
func (di *testMergeableDoltIndex) DescendRange(lessOrEqual, greaterOrEqual []interface{}) (sql.IndexLookup, error) {
	indexLookup, err := di.doltIndex.DescendRange(lessOrEqual, greaterOrEqual)
	return &testMergeableIndexLookup{
		doltIndexLookup: indexLookup.(*doltIndexLookup),
		t:               di.t,
		finalRanges:     di.finalRanges,
	}, err
}

// Lookup made to test mergeable indexes by intercepting the lookup functions and adding tracking for testing.
type testMergeableIndexLookup struct {
	*doltIndexLookup
	t           *testing.T
	finalRanges func([]lookup.Range) // We return the final range set to compare to the expected ranges
}

func (il *testMergeableIndexLookup) IsMergeable(indexLookup sql.IndexLookup) bool {
	return il.doltIndexLookup.IsMergeable(indexLookup.(*testMergeableIndexLookup).doltIndexLookup)
}
func (il *testMergeableIndexLookup) Intersection(indexLookups ...sql.IndexLookup) (sql.IndexLookup, error) {
	newLookups := make([]sql.IndexLookup, len(indexLookups))
	for i, otherIl := range indexLookups {
		newLookups[i] = otherIl.(*testMergeableIndexLookup).doltIndexLookup
	}
	intersectedIl, err := il.doltIndexLookup.Intersection(newLookups...)
	if err != nil {
		return nil, err
	}
	return &testMergeableIndexLookup{
		doltIndexLookup: intersectedIl.(*doltIndexLookup),
		t:               il.t,
		finalRanges:     il.finalRanges,
	}, nil
}
func (il *testMergeableIndexLookup) Union(indexLookups ...sql.IndexLookup) (sql.IndexLookup, error) {
	newLookups := make([]sql.IndexLookup, len(indexLookups))
	for i, otherIl := range indexLookups {
		newLookups[i] = otherIl.(*testMergeableIndexLookup).doltIndexLookup
	}
	unionedIl, err := il.doltIndexLookup.Union(newLookups...)
	if err != nil {
		return nil, err
	}
	return &testMergeableIndexLookup{
		doltIndexLookup: unionedIl.(*doltIndexLookup),
		t:               il.t,
		finalRanges:     il.finalRanges,
	}, nil
}
func (il *testMergeableIndexLookup) RowIter(ctx *sql.Context, rowData types.Map) (sql.RowIter, error) {
	il.finalRanges(il.ranges) // this is where the ranges turn into noms.ReadRanges, so we return the final slice here
	return il.doltIndexLookup.RowIter(ctx, rowData, nil)
}

// indexTuple converts integers into the appropriate tuple for comparison against ranges
type indexTuple struct {
	nbf  *types.NomsBinFormat
	cols []schema.Column
}

func (it *indexTuple) tuple(vals ...int) types.Tuple {
	if len(it.cols) != len(vals) {
		panic("len of columns in index does not match the given number of values")
	}
	valsWithTags := make([]types.Value, len(vals)*2)
	for i, val := range vals {
		valsWithTags[2*i] = types.Uint(it.cols[i].Tag)
		valsWithTags[2*i+1] = types.Int(val)
	}
	tpl, err := types.NewTuple(it.nbf, valsWithTags...)
	if err != nil {
		panic(err)
	}
	return tpl
}

func (it *indexTuple) nilTuple() types.Tuple {
	valsWithTags := make([]types.Value, len(it.cols)*2)
	for i := 0; i < len(it.cols); i++ {
		valsWithTags[2*i] = types.Uint(it.cols[i].Tag)
		valsWithTags[2*i+1] = types.NullValue
	}
	tpl, err := types.NewTuple(it.nbf, valsWithTags...)
	if err != nil {
		panic(err)
	}
	return tpl
}
