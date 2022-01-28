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

package merge

import (
	"context"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	filesys2 "github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

func mustTuple(tpl types.Tuple, err error) types.Tuple {
	if err != nil {
		panic(err)
	}

	return tpl
}

func mustString(str string, err error) string {
	if err != nil {
		panic(err)
	}

	return str
}

func mustGetValue(val types.Value, _ bool, err error) types.Value {
	if err != nil {
		panic(err)
	}

	return val
}

type RowMergeTest struct {
	name                  string
	row, mergeRow, ancRow types.Value
	sch                   schema.Schema
	expectedResult        types.Value
	expectConflict        bool
}

func valsToTestTupleWithoutPks(vals []types.Value) types.Value {
	return valsToTestTuple(vals, false)
}

func valsToTestTupleWithPks(vals []types.Value) types.Value {
	return valsToTestTuple(vals, true)
}

func valsToTestTuple(vals []types.Value, includePrimaryKeys bool) types.Value {
	if vals == nil {
		return nil
	}

	tplVals := make([]types.Value, 0, 2*len(vals))
	for i, val := range vals {
		if !types.IsNull(val) {
			tag := i
			// Assume one primary key tag, add 1 to all other tags
			if includePrimaryKeys {
				tag++
			}
			tplVals = append(tplVals, types.Uint(tag))
			tplVals = append(tplVals, val)
		}
	}

	return mustTuple(types.NewTuple(types.Format_Default, tplVals...))
}

func createRowMergeStruct(name string, vals, mergeVals, ancVals, expected []types.Value, expectCnf bool) RowMergeTest {
	longest := vals

	if len(mergeVals) > len(longest) {
		longest = mergeVals
	}

	if len(ancVals) > len(longest) {
		longest = ancVals
	}

	cols := make([]schema.Column, len(longest)+1)
	// Schema needs a primary key to be valid, but all the logic being tested works only on the non-key columns.
	cols[0] = schema.NewColumn("primaryKey", 0, types.IntKind, true)
	for i, val := range longest {
		tag := i + 1
		cols[tag] = schema.NewColumn(strconv.FormatInt(int64(tag), 10), uint64(tag), val.Kind(), false)
	}

	colColl := schema.NewColCollection(cols...)
	sch := schema.MustSchemaFromCols(colColl)

	tpl := valsToTestTupleWithPks(vals)
	mergeTpl := valsToTestTupleWithPks(mergeVals)
	ancTpl := valsToTestTupleWithPks(ancVals)
	expectedTpl := valsToTestTupleWithPks(expected)
	return RowMergeTest{name, tpl, mergeTpl, ancTpl, sch, expectedTpl, expectCnf}
}

func TestRowMerge(t *testing.T) {
	tests := []RowMergeTest{
		createRowMergeStruct(
			"add same row",
			[]types.Value{types.String("one"), types.Int(2)},
			[]types.Value{types.String("one"), types.Int(2)},
			nil,
			[]types.Value{types.String("one"), types.Int(2)},
			false,
		),
		createRowMergeStruct(
			"add diff row",
			[]types.Value{types.String("one"), types.String("two")},
			[]types.Value{types.String("one"), types.String("three")},
			nil,
			nil,
			true,
		),
		createRowMergeStruct(
			"both delete row",
			nil,
			nil,
			[]types.Value{types.String("one"), types.Uint(2)},
			nil,
			false,
		),
		createRowMergeStruct(
			"one delete one modify",
			nil,
			[]types.Value{types.String("two"), types.Uint(2)},
			[]types.Value{types.String("one"), types.Uint(2)},
			nil,
			true,
		),
		createRowMergeStruct(
			"modify rows without overlap",
			[]types.Value{types.String("two"), types.Uint(2)},
			[]types.Value{types.String("one"), types.Uint(3)},
			[]types.Value{types.String("one"), types.Uint(2)},
			[]types.Value{types.String("two"), types.Uint(3)},
			false,
		),
		createRowMergeStruct(
			"modify rows with equal overlapping changes",
			[]types.Value{types.String("two"), types.Uint(2), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))},
			[]types.Value{types.String("one"), types.Uint(3), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))},
			[]types.Value{types.String("one"), types.Uint(2), types.UUID(uuid.MustParse("00000000-0000-0000-0000-000000000000"))},
			[]types.Value{types.String("two"), types.Uint(3), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))},
			false,
		),
		createRowMergeStruct(
			"modify rows with differing overlapping changes",
			[]types.Value{types.String("two"), types.Uint(2), types.UUID(uuid.MustParse("99999999-9999-9999-9999-999999999999"))},
			[]types.Value{types.String("one"), types.Uint(3), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))},
			[]types.Value{types.String("one"), types.Uint(2), types.UUID(uuid.MustParse("00000000-0000-0000-0000-000000000000"))},
			nil,
			true,
		),
		createRowMergeStruct(
			"modify rows where one adds a column",
			[]types.Value{types.String("two"), types.Uint(2)},
			[]types.Value{types.String("one"), types.Uint(3), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))},
			[]types.Value{types.String("one"), types.Uint(2)},
			[]types.Value{types.String("two"), types.Uint(3), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))},
			false,
		),
		createRowMergeStruct(
			"modify row where values added in different columns",
			[]types.Value{types.String("one"), types.Uint(2), types.String(""), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))},
			[]types.Value{types.String("one"), types.Uint(2), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")), types.String("")},
			[]types.Value{types.String("one"), types.Uint(2), types.NullValue, types.NullValue},
			nil,
			true,
		),
		createRowMergeStruct(
			"modify row where initial value wasn't given",
			[]types.Value{mustTuple(types.NewTuple(types.Format_Default, types.String("one"), types.Uint(2), types.String("a")))},
			[]types.Value{mustTuple(types.NewTuple(types.Format_Default, types.String("one"), types.Uint(2), types.String("b")))},
			[]types.Value{mustTuple(types.NewTuple(types.Format_Default, types.String("one"), types.Uint(2), types.NullValue))},
			nil,
			true,
		),
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualResult, isConflict, err := pkRowMerge(context.Background(), types.Format_Default, test.sch, test.row, test.mergeRow, test.ancRow)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedResult, actualResult, "expected "+mustString(types.EncodedValue(context.Background(), test.expectedResult))+"got "+mustString(types.EncodedValue(context.Background(), actualResult)))
			assert.Equal(t, test.expectConflict, isConflict)
		})
	}
}

const (
	tableName = "test-table"
	name      = "billy bob"
	email     = "bigbillieb@fake.horse"

	idTag    = 100
	nameTag  = 0
	titleTag = 1
)

var colColl = schema.NewColCollection(
	schema.NewColumn("id", idTag, types.UUIDKind, true, schema.NotNullConstraint{}),
	schema.NewColumn("name", nameTag, types.StringKind, false, schema.NotNullConstraint{}),
	schema.NewColumn("title", titleTag, types.StringKind, false),
)
var sch = schema.MustSchemaFromCols(colColl)

var uuids = []types.UUID{
	types.UUID(uuid.MustParse("00000000-0000-0000-0000-000000000000")),
	types.UUID(uuid.MustParse("00000000-0000-0000-0000-000000000001")),
	types.UUID(uuid.MustParse("00000000-0000-0000-0000-000000000002")),
	types.UUID(uuid.MustParse("00000000-0000-0000-0000-000000000003")),
	types.UUID(uuid.MustParse("00000000-0000-0000-0000-000000000004")),
	types.UUID(uuid.MustParse("00000000-0000-0000-0000-000000000005")),
	types.UUID(uuid.MustParse("00000000-0000-0000-0000-000000000006")),
	types.UUID(uuid.MustParse("00000000-0000-0000-0000-000000000007")),
	types.UUID(uuid.MustParse("00000000-0000-0000-0000-000000000008")),
	types.UUID(uuid.MustParse("00000000-0000-0000-0000-000000000009")),
	types.UUID(uuid.MustParse("00000000-0000-0000-0000-00000000000a")),
	types.UUID(uuid.MustParse("00000000-0000-0000-0000-00000000000b")),
	types.UUID(uuid.MustParse("00000000-0000-0000-0000-00000000000c")),
}

var keyTuples = make([]types.Tuple, len(uuids))

var index schema.Index

func init() {
	keyTag := types.Uint(idTag)

	for i, id := range uuids {
		keyTuples[i] = mustTuple(types.NewTuple(types.Format_Default, keyTag, id))
	}

	index, _ = sch.Indexes().AddIndexByColTags("idx_name", []uint64{nameTag}, schema.IndexProperties{IsUnique: false, Comment: ""})
}

func setupMergeTest(t *testing.T) (types.ValueReadWriter, *doltdb.Commit, *doltdb.Commit, types.Map, types.Map) {
	ddb, _ := doltdb.LoadDoltDB(context.Background(), types.Format_Default, doltdb.InMemDoltDB, filesys2.LocalFS)
	vrw := ddb.ValueReadWriter()

	err := ddb.WriteEmptyRepo(context.Background(), env.DefaultInitBranch, name, email)
	require.NoError(t, err)

	mainHeadSpec, _ := doltdb.NewCommitSpec(env.DefaultInitBranch)
	mainHead, err := ddb.Resolve(context.Background(), mainHeadSpec, nil)
	require.NoError(t, err)

	initialRows, err := types.NewMap(context.Background(), vrw,
		keyTuples[0], valsToTestTupleWithoutPks([]types.Value{types.String("person 1"), types.String("dufus")}),
		keyTuples[1], valsToTestTupleWithoutPks([]types.Value{types.String("person 2"), types.NullValue}),
		keyTuples[2], valsToTestTupleWithoutPks([]types.Value{types.String("person 3"), types.NullValue}),
		keyTuples[3], valsToTestTupleWithoutPks([]types.Value{types.String("person 4"), types.String("senior dufus")}),
		keyTuples[4], valsToTestTupleWithoutPks([]types.Value{types.String("person 5"), types.NullValue}),
		keyTuples[5], valsToTestTupleWithoutPks([]types.Value{types.String("person 6"), types.NullValue}),
		keyTuples[6], valsToTestTupleWithoutPks([]types.Value{types.String("person 7"), types.String("madam")}),
		keyTuples[7], valsToTestTupleWithoutPks([]types.Value{types.String("person 8"), types.String("miss")}),
		keyTuples[8], valsToTestTupleWithoutPks([]types.Value{types.String("person 9"), types.NullValue}),
	)
	require.NoError(t, err)

	updateRowEditor := initialRows.Edit()                                                                                          // leave 0 as is
	updateRowEditor.Remove(keyTuples[1])                                                                                           // remove 1 from both
	updateRowEditor.Remove(keyTuples[2])                                                                                           // remove 2 from update
	updateRowEditor.Set(keyTuples[4], valsToTestTupleWithoutPks([]types.Value{types.String("person five"), types.NullValue}))      // modify 4 only in update
	updateRowEditor.Set(keyTuples[6], valsToTestTupleWithoutPks([]types.Value{types.String("person 7"), types.String("dr")}))      // modify 6 in both without overlap
	updateRowEditor.Set(keyTuples[7], valsToTestTupleWithoutPks([]types.Value{types.String("person eight"), types.NullValue}))     // modify 7 in both with equal overlap
	updateRowEditor.Set(keyTuples[8], valsToTestTupleWithoutPks([]types.Value{types.String("person nine"), types.NullValue}))      // modify 8 in both with conflicting overlap
	updateRowEditor.Set(keyTuples[9], valsToTestTupleWithoutPks([]types.Value{types.String("person ten"), types.NullValue}))       // add 9 in update
	updateRowEditor.Set(keyTuples[11], valsToTestTupleWithoutPks([]types.Value{types.String("person twelve"), types.NullValue}))   // add 11 in both without difference
	updateRowEditor.Set(keyTuples[12], valsToTestTupleWithoutPks([]types.Value{types.String("person thirteen"), types.NullValue})) // add 12 in both with differences

	updatedRows, err := updateRowEditor.Map(context.Background())
	require.NoError(t, err)

	mergeRowEditor := initialRows.Edit()                                                                                                 // leave 0 as is
	mergeRowEditor.Remove(keyTuples[1])                                                                                                  // remove 1 from both
	mergeRowEditor.Remove(keyTuples[3])                                                                                                  // remove 3 from merge
	mergeRowEditor.Set(keyTuples[5], valsToTestTupleWithoutPks([]types.Value{types.String("person six"), types.NullValue}))              // modify 5 only in merge
	mergeRowEditor.Set(keyTuples[6], valsToTestTupleWithoutPks([]types.Value{types.String("person seven"), types.String("madam")}))      // modify 6 in both without overlap
	mergeRowEditor.Set(keyTuples[7], valsToTestTupleWithoutPks([]types.Value{types.String("person eight"), types.NullValue}))            // modify 7 in both with equal overlap
	mergeRowEditor.Set(keyTuples[8], valsToTestTupleWithoutPks([]types.Value{types.String("person number nine"), types.NullValue}))      // modify 8 in both with conflicting overlap
	mergeRowEditor.Set(keyTuples[10], valsToTestTupleWithoutPks([]types.Value{types.String("person eleven"), types.NullValue}))          // add 10 in merge
	mergeRowEditor.Set(keyTuples[11], valsToTestTupleWithoutPks([]types.Value{types.String("person twelve"), types.NullValue}))          // add 11 in both without difference
	mergeRowEditor.Set(keyTuples[12], valsToTestTupleWithoutPks([]types.Value{types.String("person number thirteen"), types.NullValue})) // add 12 in both with differences

	mergeRows, err := mergeRowEditor.Map(context.Background())
	require.NoError(t, err)

	expectedRows, err := types.NewMap(context.Background(), vrw,
		keyTuples[0], mustGetValue(initialRows.MaybeGet(context.Background(), keyTuples[0])), // unaltered
		keyTuples[4], mustGetValue(updatedRows.MaybeGet(context.Background(), keyTuples[4])), // modified in updated
		keyTuples[5], mustGetValue(mergeRows.MaybeGet(context.Background(), keyTuples[5])), // modified in merged
		keyTuples[6], valsToTestTupleWithoutPks([]types.Value{types.String("person seven"), types.String("dr")}), // modified in both with no overlap
		keyTuples[7], mustGetValue(updatedRows.MaybeGet(context.Background(), keyTuples[7])), // modify both with the same value
		keyTuples[8], mustGetValue(updatedRows.MaybeGet(context.Background(), keyTuples[8])), // conflict
		keyTuples[9], mustGetValue(updatedRows.MaybeGet(context.Background(), keyTuples[9])), // added in update
		keyTuples[10], mustGetValue(mergeRows.MaybeGet(context.Background(), keyTuples[10])), // added in merge
		keyTuples[11], mustGetValue(updatedRows.MaybeGet(context.Background(), keyTuples[11])), // added same in both
		keyTuples[12], mustGetValue(updatedRows.MaybeGet(context.Background(), keyTuples[12])), // conflict
	)
	require.NoError(t, err)

	updateConflict := conflict.NewConflict(
		mustGetValue(initialRows.MaybeGet(context.Background(), keyTuples[8])),
		mustGetValue(updatedRows.MaybeGet(context.Background(), keyTuples[8])),
		mustGetValue(mergeRows.MaybeGet(context.Background(), keyTuples[8])))

	addConflict := conflict.NewConflict(
		nil,
		valsToTestTupleWithoutPks([]types.Value{types.String("person thirteen"), types.NullValue}),
		valsToTestTupleWithoutPks([]types.Value{types.String("person number thirteen"), types.NullValue}),
	)

	expectedConflicts, err := types.NewMap(context.Background(), vrw,
		keyTuples[8], mustTuple(updateConflict.ToNomsList(vrw)),
		keyTuples[12], mustTuple(addConflict.ToNomsList(vrw)),
	)
	require.NoError(t, err)

	tbl, err := doltdb.NewNomsTable(context.Background(), vrw, sch, initialRows, nil, nil)
	require.NoError(t, err)
	tbl, err = editor.RebuildAllIndexes(context.Background(), tbl, editor.TestEditorOptions(vrw))
	require.NoError(t, err)

	updatedTbl, err := doltdb.NewNomsTable(context.Background(), vrw, sch, updatedRows, nil, nil)
	require.NoError(t, err)
	updatedTbl, err = editor.RebuildAllIndexes(context.Background(), updatedTbl, editor.TestEditorOptions(vrw))
	require.NoError(t, err)

	mergeTbl, err := doltdb.NewNomsTable(context.Background(), vrw, sch, mergeRows, nil, nil)
	require.NoError(t, err)
	mergeTbl, err = editor.RebuildAllIndexes(context.Background(), mergeTbl, editor.TestEditorOptions(vrw))
	require.NoError(t, err)

	mRoot, err := mainHead.GetRootValue()
	require.NoError(t, err)

	mRoot, err = mRoot.PutTable(context.Background(), tableName, tbl)
	require.NoError(t, err)

	updatedRoot, err := mRoot.PutTable(context.Background(), tableName, updatedTbl)
	require.NoError(t, err)

	mergeRoot, err := mRoot.PutTable(context.Background(), tableName, mergeTbl)
	require.NoError(t, err)

	mainHash, err := ddb.WriteRootValue(context.Background(), mRoot)
	require.NoError(t, err)
	hash, err := ddb.WriteRootValue(context.Background(), updatedRoot)
	require.NoError(t, err)
	mergeHash, err := ddb.WriteRootValue(context.Background(), mergeRoot)
	require.NoError(t, err)

	meta, err := doltdb.NewCommitMeta(name, email, "fake")
	require.NoError(t, err)
	initialCommit, err := ddb.Commit(context.Background(), mainHash, ref.NewBranchRef(env.DefaultInitBranch), meta)
	require.NoError(t, err)
	commit, err := ddb.Commit(context.Background(), hash, ref.NewBranchRef(env.DefaultInitBranch), meta)
	require.NoError(t, err)

	err = ddb.NewBranchAtCommit(context.Background(), ref.NewBranchRef("to-merge"), initialCommit)
	require.NoError(t, err)
	mergeCommit, err := ddb.Commit(context.Background(), mergeHash, ref.NewBranchRef("to-merge"), meta)
	require.NoError(t, err)

	return vrw, commit, mergeCommit, expectedRows, expectedConflicts
}

func TestMergeCommits(t *testing.T) {
	vrw, commit, mergeCommit, expectedRows, expectedConflicts := setupMergeTest(t)

	root, err := commit.GetRootValue()
	require.NoError(t, err)

	mergeRoot, err := mergeCommit.GetRootValue()
	require.NoError(t, err)

	ancCm, err := doltdb.GetCommitAncestor(context.Background(), commit, mergeCommit)
	require.NoError(t, err)

	ancRoot, err := ancCm.GetRootValue()
	require.NoError(t, err)

	ff, err := commit.CanFastForwardTo(context.Background(), mergeCommit)
	require.NoError(t, err)
	require.False(t, ff)

	merger := NewMerger(context.Background(), root, mergeRoot, ancRoot, vrw)
	opts := editor.TestEditorOptions(vrw)
	merged, stats, err := merger.MergeTable(context.Background(), tableName, opts)

	if err != nil {
		t.Fatal(err)
	}

	if stats.Adds != 2 || stats.Deletes != 2 || stats.Modifications != 3 || stats.Conflicts != 2 {
		t.Error("Actual stats differ from expected")
	}

	tbl, _, err := root.GetTable(context.Background(), tableName)
	assert.NoError(t, err)
	sch, err := tbl.GetSchema(context.Background())
	assert.NoError(t, err)
	expected, err := doltdb.NewNomsTable(context.Background(), vrw, sch, expectedRows, nil, nil)
	assert.NoError(t, err)
	expected, err = editor.RebuildAllIndexes(context.Background(), expected, editor.TestEditorOptions(vrw))
	assert.NoError(t, err)
	conflictSchema := conflict.NewConflictSchema(sch, sch, sch)
	assert.NoError(t, err)
	expected, err = expected.SetConflicts(context.Background(), conflictSchema, expectedConflicts)
	assert.NoError(t, err)

	h, err := merged.HashOf()
	assert.NoError(t, err)
	eh, err := expected.HashOf()
	assert.NoError(t, err)
	if h == eh {
		mergedRows, err := merged.GetNomsRowData(context.Background())
		assert.NoError(t, err)
		if !mergedRows.Equals(expectedRows) {
			t.Error(mustString(types.EncodedValue(context.Background(), mergedRows)), "\n!=\n", mustString(types.EncodedValue(context.Background(), expectedRows)))
		}
		mergedIndexRows, err := merged.GetNomsIndexRowData(context.Background(), index.Name())
		assert.NoError(t, err)
		expectedIndexRows, err := expected.GetNomsIndexRowData(context.Background(), index.Name())
		assert.NoError(t, err)
		if expectedRows.Len() != mergedIndexRows.Len() || !mergedIndexRows.Equals(expectedIndexRows) {
			t.Error("index contents are incorrect")
		}
	} else {
		assert.Fail(t, "%s and %s do not equal", h.String(), eh.String())
	}
}
