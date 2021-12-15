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
	"testing"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/store/types"
)

func setupIndexes(t *testing.T, tableName, insertQuery string) (*sqle.Engine, *env.DoltEnv, *doltdb.RootValue, Database, []*indexTuple) {
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

	mrEnv, err := env.DoltEnvAsMultiEnv(context.Background(), dEnv)
	require.NoError(t, err)
	pro := NewDoltDatabaseProvider(dEnv.Config, mrEnv.FileSystem(), db)
	pro = pro.WithDbFactoryUrl(doltdb.InMemDoltDB)

	engine = sqle.NewDefault(pro)

	// Get an updated root to use for the rest of the test
	ctx := sql.NewEmptyContext()
	sess, err := dsess.NewDoltSession(ctx, ctx.Session.(*sql.BaseSession), pro, config.NewEmptyMapConfig(), getDbState(t, db, dEnv))
	require.NoError(t, err)
	roots, ok := sess.GetRoots(ctx, db.Name())
	require.True(t, ok)
	err = sess.SetRoot(sqlCtx, db.Name(), roots.Working)

	it := []*indexTuple{
		idxv1ToTuple,
		idxv2v1ToTuple,
		{
			nbf:  idxv2v1RowData.Format(),
			cols: idxv2v1Cols[:len(idxv2v1Cols)-1],
		},
	}

	return engine, dEnv, roots.Working, db, it
}

func closedRange(tpl1, tpl2 types.Tuple) *noms.ReadRange {
	return customRange(tpl1, tpl2, sql.Closed, sql.Closed)
}

func openRange(tpl1, tpl2 types.Tuple) *noms.ReadRange {
	return customRange(tpl1, tpl2, sql.Open, sql.Open)
}

func customRange(tpl1, tpl2 types.Tuple, bt1, bt2 sql.RangeBoundType) *noms.ReadRange {
	var nrc nomsRangeCheck
	_ = tpl1.IterFields(func(tupleIndex uint64, tupleVal types.Value) (stop bool, err error) {
		if tupleIndex%2 == 0 {
			return false, nil
		}
		if bt1 == sql.Closed {
			nrc = append(nrc, columnBounds{
				boundsCase: boundsCase_greaterEquals_infinity,
				lowerbound: tupleVal,
			})
		} else {
			nrc = append(nrc, columnBounds{
				boundsCase: boundsCase_greater_infinity,
				lowerbound: tupleVal,
			})
		}
		return false, nil
	})
	_ = tpl2.IterFields(func(tupleIndex uint64, tupleVal types.Value) (stop bool, err error) {
		if tupleIndex%2 == 0 {
			return false, nil
		}
		idx := (tupleIndex - 1) / 2
		if bt2 == sql.Closed {
			// Bounds cases are enum aliases on bytes, and they're arranged such that we can increment the case
			// that was previously set when evaluating the lowerbound to get the proper overall case.
			nrc[idx].boundsCase += 1
			nrc[idx].upperbound = tupleVal
		} else {
			nrc[idx].boundsCase += 2
			nrc[idx].upperbound = tupleVal
		}
		return false, nil
	})
	return &noms.ReadRange{
		Start:     tpl1,
		Inclusive: true,
		Reverse:   false,
		Check:     nrc,
	}
}

func greaterThanRange(tpl types.Tuple) *noms.ReadRange {
	var nrc nomsRangeCheck
	_ = tpl.IterFields(func(tupleIndex uint64, tupleVal types.Value) (stop bool, err error) {
		if tupleIndex%2 == 0 {
			return false, nil
		}
		nrc = append(nrc, columnBounds{
			boundsCase: boundsCase_greater_infinity,
			lowerbound: tupleVal,
		})
		return false, nil
	})
	return &noms.ReadRange{
		Start:     tpl,
		Inclusive: true,
		Reverse:   false,
		Check:     nrc,
	}
}

func lessThanRange(tpl types.Tuple) *noms.ReadRange {
	var nrc nomsRangeCheck
	_ = tpl.IterFields(func(tupleIndex uint64, tupleVal types.Value) (stop bool, err error) {
		if tupleIndex%2 == 0 {
			return false, nil
		}
		nrc = append(nrc, columnBounds{
			boundsCase: boundsCase_infinity_less,
			upperbound: tupleVal,
		})
		return false, nil
	})
	return &noms.ReadRange{
		Start:     types.EmptyTuple(types.Format_Default),
		Inclusive: true,
		Reverse:   false,
		Check:     nrc,
	}
}

func greaterOrEqualRange(tpl types.Tuple) *noms.ReadRange {
	var nrc nomsRangeCheck
	_ = tpl.IterFields(func(tupleIndex uint64, tupleVal types.Value) (stop bool, err error) {
		if tupleIndex%2 == 0 {
			return false, nil
		}
		nrc = append(nrc, columnBounds{
			boundsCase: boundsCase_greaterEquals_infinity,
			lowerbound: tupleVal,
		})
		return false, nil
	})
	return &noms.ReadRange{
		Start:     tpl,
		Inclusive: true,
		Reverse:   false,
		Check:     nrc,
	}
}

func lessOrEqualRange(tpl types.Tuple) *noms.ReadRange {
	var nrc nomsRangeCheck
	_ = tpl.IterFields(func(tupleIndex uint64, tupleVal types.Value) (stop bool, err error) {
		if tupleIndex%2 == 0 {
			return false, nil
		}
		nrc = append(nrc, columnBounds{
			boundsCase: boundsCase_infinity_lessEquals,
			upperbound: tupleVal,
		})
		return false, nil
	})
	return &noms.ReadRange{
		Start:     types.EmptyTuple(types.Format_Default),
		Inclusive: true,
		Reverse:   false,
		Check:     nrc,
	}
}

func allRange() *noms.ReadRange {
	return &noms.ReadRange{
		Start:     types.EmptyTuple(types.Format_Default),
		Inclusive: true,
		Reverse:   false,
		Check:     nomsRangeCheck{},
	}
}

func readRangesEqual(nr1, nr2 *noms.ReadRange) bool {
	if nr1 == nil || nr2 == nil {
		if nr1 == nil && nr2 == nil {
			return true
		}
		return false
	}
	if nr1.Inclusive != nr2.Inclusive || nr1.Reverse != nr2.Reverse || !nr1.Start.Equals(nr2.Start) ||
		!nr1.Check.(nomsRangeCheck).Equals(nr2.Check.(nomsRangeCheck)) {
		return false
	}
	return true
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
