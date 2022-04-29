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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	filesys2 "github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/valutil"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	tableName = "test-table"
	name      = "billy bob"
	email     = "bigbillieb@fake.horse"

	idTag   = 100
	col1Tag = 0
	col2Tag = 1
)

var colColl = schema.NewColCollection(
	schema.NewColumn("id", idTag, types.IntKind, true, schema.NotNullConstraint{}),
	schema.NewColumn("col1", col1Tag, types.IntKind, false, schema.NotNullConstraint{}),
	schema.NewColumn("col2", col2Tag, types.IntKind, false, schema.NotNullConstraint{}),
)
var sch = schema.MustSchemaFromCols(colColl)

var indexSchema schema.Index
var compositeIndexSchema schema.Index

func init() {
	indexSchema, _ = sch.Indexes().AddIndexByColTags("idx_col1", []uint64{col1Tag}, schema.IndexProperties{IsUnique: false, Comment: ""})
	compositeIndexSchema, _ = sch.Indexes().AddIndexByColTags("idx_col1_col2", []uint64{col1Tag, col2Tag}, schema.IndexProperties{IsUnique: false, Comment: ""})
}

type rowV struct {
	col1, col2 int
}

func (v rowV) nomsValue() types.Value {
	return valsToTestTupleWithoutPks([]types.Value{types.Int(v.col1), types.Int(v.col2)})
}

const (
	NoopAction ActionType = iota
	InsertAction
	UpdateAction
	DeleteAction
)

type ActionType int

type testRow struct {
	key                     int
	initialValue            *rowV
	leftAction, rightAction ActionType
	leftValue, rightValue   *rowV
	conflict                bool
	expectedValue           *rowV
}

// There are 16 cases for merges if the left and right branches don't modify primary keys.
//
// If a row exists in the ancestor commit, then left and right can perform a no-op, update,
// or delete => 3*3 = +9.
//
// If a row does not exist in the ancestor commit, then left and right can perform a no-op or
// insert => 2*2 = +4.
//
// For (update, update) there are identical updates, conflicting updates, and
// non-conflicting updates. => +2
//
// For (insert, insert) there are identical inserts and conflicting inserts => +1

var testRows = []testRow{
	// Ancestor exists
	{
		0,
		&rowV{0, 0},
		NoopAction,
		NoopAction,
		nil,
		nil,
		false,
		&rowV{0, 0},
	},
	{
		1,
		&rowV{1, 1},
		NoopAction,
		UpdateAction,
		nil,
		&rowV{-1, -1},
		false,
		&rowV{-1, -1},
	},
	{
		2,
		&rowV{2, 2},
		NoopAction,
		DeleteAction,
		nil,
		nil,
		false,
		nil,
	},
	{
		3,
		&rowV{3, 3},
		UpdateAction,
		NoopAction,
		&rowV{-3, -3},
		nil,
		false,
		&rowV{-3, -3},
	},
	// Identical Update
	{
		4,
		&rowV{4, 4},
		UpdateAction,
		UpdateAction,
		&rowV{-4, -4},
		&rowV{-4, -4},
		false,
		&rowV{-4, -4},
	},
	// Conflicting Update
	{
		5,
		&rowV{5, 5},
		UpdateAction,
		UpdateAction,
		&rowV{-5, 5},
		&rowV{0, 5},
		true,
		&rowV{-5, 5},
	},
	// Non-conflicting update
	{
		6,
		&rowV{6, 6},
		UpdateAction,
		UpdateAction,
		&rowV{-6, 6},
		&rowV{6, -6},
		false,
		&rowV{-6, -6},
	},
	{
		7,
		&rowV{7, 7},
		UpdateAction,
		DeleteAction,
		&rowV{-7, -7},
		nil,
		true,
		&rowV{-7, -7},
	},
	{
		8,
		&rowV{8, 8},
		DeleteAction,
		NoopAction,
		nil,
		nil,
		false,
		nil,
	},
	{
		9,
		&rowV{9, 9},
		DeleteAction,
		UpdateAction,
		nil,
		&rowV{-9, -9},
		true,
		nil,
	},
	{
		10,
		&rowV{10, 10},
		DeleteAction,
		DeleteAction,
		nil,
		nil,
		false,
		nil,
	},
	// Key does not exist in ancestor
	{
		11,
		nil,
		NoopAction,
		NoopAction,
		nil,
		nil,
		false,
		nil,
	},
	{
		12,
		nil,
		NoopAction,
		InsertAction,
		nil,
		&rowV{12, 12},
		false,
		&rowV{12, 12},
	},
	{
		13,
		nil,
		InsertAction,
		NoopAction,
		&rowV{13, 13},
		nil,
		false,
		&rowV{13, 13},
	},
	// Identical Insert
	{
		14,
		nil,
		InsertAction,
		InsertAction,
		&rowV{14, 14},
		&rowV{14, 14},
		false,
		&rowV{14, 14},
	},
	// Conflicting Insert
	{
		15,
		nil,
		InsertAction,
		InsertAction,
		&rowV{15, 15},
		&rowV{15, -15},
		true,
		&rowV{15, 15},
	},
}

func TestNomsMergeCommits(t *testing.T) {
	if types.IsFormat_DOLT_1(types.Format_Default) {
		t.Skip()
	}

	vrw, root, mergeRoot, ancRoot, expectedRows, expectedConflicts, expectedStats := setupNomsMergeTest(t)

	merger := NewMerger(context.Background(), root, mergeRoot, ancRoot, vrw)
	opts := editor.TestEditorOptions(vrw)
	merged, stats, err := merger.MergeTable(context.Background(), tableName, opts)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expectedStats, stats, "received stats is incorrect")

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

	mergedRows, err := merged.GetNomsRowData(context.Background())
	assert.NoError(t, err)
	_, conflicts, err := merged.GetConflicts(context.Background())
	assert.NoError(t, err)

	if !mergedRows.Equals(expectedRows) {
		t.Error(mustString(types.EncodedValue(context.Background(), mergedRows)), "\n!=\n", mustString(types.EncodedValue(context.Background(), expectedRows)))
	}
	if !conflicts.Equals(expectedConflicts) {
		t.Error(mustString(types.EncodedValue(context.Background(), conflicts)), "\n!=\n", mustString(types.EncodedValue(context.Background(), expectedConflicts)))
	}

	for _, index := range sch.Indexes().AllIndexes() {
		mergedIndexRows, err := merged.GetNomsIndexRowData(context.Background(), index.Name())
		assert.NoError(t, err)
		expectedIndexRows, err := expected.GetNomsIndexRowData(context.Background(), index.Name())
		assert.NoError(t, err)
		assert.Equal(t, expectedRows.Len(), mergedIndexRows.Len(), "index %s incorrect row count", index.Name())
		assert.Truef(t, expectedIndexRows.Equals(mergedIndexRows),
			"index %s contents incorrect.\nExpected: \n%s\nReceived: \n%s\n", index.Name(),
			mustString(types.EncodedValue(context.Background(), expectedIndexRows)),
			mustString(types.EncodedValue(context.Background(), mergedIndexRows)))
	}

	h, err := merged.HashOf()
	assert.NoError(t, err)
	eh, err := expected.HashOf()
	assert.NoError(t, err)
	assert.Equal(t, eh.String(), h.String(), "table hashes do not equal")
}

func setupNomsMergeTest(t *testing.T) (types.ValueReadWriter, *doltdb.RootValue, *doltdb.RootValue, *doltdb.RootValue, types.Map, types.Map, *MergeStats) {
	ddb := mustMakeEmptyRepo(t)
	vrw := ddb.ValueReadWriter()

	stats := &MergeStats{}
	var initalKVs []types.Value
	var expectedKVs []types.Value
	var expectedConflictsKVs []types.Value
	for _, testCase := range testRows {
		if testCase.initialValue != nil {
			initalKVs = append(initalKVs, nomsKey(testCase.key), testCase.initialValue.nomsValue())
		}
		if testCase.expectedValue != nil {
			expectedKVs = append(expectedKVs, nomsKey(testCase.key), testCase.expectedValue.nomsValue())
		}
		if testCase.conflict {
			stats.Conflicts++
			expectedConflictsKVs = append(
				expectedConflictsKVs,
				nomsKey(testCase.key),
				mustTuple(conflict.NewConflict(
					unwrapNoms(testCase.initialValue),
					unwrapNoms(testCase.leftValue),
					unwrapNoms(testCase.rightValue),
				).ToNomsList(vrw)),
			)
		}
	}
	initialRows, err := types.NewMap(context.Background(), vrw, initalKVs...)
	require.NoError(t, err)
	expectedRows, err := types.NewMap(context.Background(), vrw, expectedKVs...)
	require.NoError(t, err)
	expectedConflicts, err := types.NewMap(context.Background(), vrw, expectedConflictsKVs...)
	require.NoError(t, err)

	leftE := initialRows.Edit()
	rightE := initialRows.Edit()
	for _, testCase := range testRows {
		switch testCase.leftAction {
		case NoopAction:
			break
		case InsertAction, UpdateAction:
			leftE.Set(nomsKey(testCase.key), testCase.leftValue.nomsValue())
		case DeleteAction:
			leftE.Remove(nomsKey(testCase.key))
		}

		switch testCase.rightAction {
		case NoopAction:
			break
		case InsertAction, UpdateAction:
			rightE.Set(nomsKey(testCase.key), testCase.rightValue.nomsValue())
		case DeleteAction:
			rightE.Remove(nomsKey(testCase.key))
		}
	}

	updatedRows, err := leftE.Map(context.Background())
	require.NoError(t, err)
	mergeRows, err := rightE.Map(context.Background())
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

	root, mergeRoot, ancRoot := buildLeftRightAncCommitsAndBranches(t, ddb, tbl, updatedTbl, mergeTbl)

	return vrw, root, mergeRoot, ancRoot, expectedRows, expectedConflicts, calcExpectedStats(t)
}

func calcExpectedStats(t *testing.T) *MergeStats {
	s := &MergeStats{Operation: TableModified}
	for _, testCase := range testRows {
		if (testCase.leftAction == InsertAction) != (testCase.rightAction == InsertAction) {
			if testCase.leftAction == UpdateAction || testCase.rightAction == UpdateAction ||
				testCase.leftAction == DeleteAction || testCase.rightAction == DeleteAction {
				// Either the row exists in the ancestor commit and we are
				// deleting or updating it, or the row doesn't exist and we are
				// inserting it.
				t.Fatalf("it's impossible for an insert to be paired with an update or delete")
			}
		}

		if testCase.leftAction == NoopAction {
			switch testCase.rightAction {
			case NoopAction:
			case DeleteAction:
				s.Deletes++
			case InsertAction:
				s.Adds++
			case UpdateAction:
				s.Modifications++
			}
			continue
		}

		if testCase.rightAction == NoopAction {
			switch testCase.leftAction {
			case NoopAction:
			case DeleteAction:
				s.Deletes++
			case InsertAction:
				s.Adds++
			case UpdateAction:
				s.Modifications++
			}
			continue
		}

		if testCase.conflict {
			// (UpdateAction, DeleteAction),
			// (DeleteAction, UpdateAction),
			// (UpdateAction, UpdateAction) with conflict,
			// (InsertAction, InsertAction) with conflict
			s.Conflicts++
			continue
		}

		if testCase.leftAction == InsertAction && testCase.rightAction == InsertAction {
			// Equivalent inserts
			continue
		}

		if !valutil.NilSafeEqCheck(unwrapNoms(testCase.leftValue), unwrapNoms(testCase.rightValue)) {
			s.Modifications++
			continue
		}
	}

	return s
}

func mustMakeEmptyRepo(t *testing.T) *doltdb.DoltDB {
	ddb, _ := doltdb.LoadDoltDB(context.Background(), types.Format_Default, doltdb.InMemDoltDB, filesys2.LocalFS)
	err := ddb.WriteEmptyRepo(context.Background(), env.DefaultInitBranch, name, email)
	require.NoError(t, err)
	return ddb
}

func buildLeftRightAncCommitsAndBranches(t *testing.T, ddb *doltdb.DoltDB, tbl, updatedTbl, mergeTbl *doltdb.Table) (*doltdb.RootValue, *doltdb.RootValue, *doltdb.RootValue) {
	mainHeadSpec, _ := doltdb.NewCommitSpec(env.DefaultInitBranch)
	mainHead, err := ddb.Resolve(context.Background(), mainHeadSpec, nil)
	require.NoError(t, err)

	mRoot, err := mainHead.GetRootValue(context.Background())
	require.NoError(t, err)

	mRoot, err = mRoot.PutTable(context.Background(), tableName, tbl)
	require.NoError(t, err)

	updatedRoot, err := mRoot.PutTable(context.Background(), tableName, updatedTbl)
	require.NoError(t, err)

	mergeRoot, err := mRoot.PutTable(context.Background(), tableName, mergeTbl)
	require.NoError(t, err)

	r, mainHash, err := ddb.WriteRootValue(context.Background(), mRoot)
	require.NoError(t, err)
	mRoot = r
	r, hash, err := ddb.WriteRootValue(context.Background(), updatedRoot)
	require.NoError(t, err)
	updatedRoot = r
	r, mergeHash, err := ddb.WriteRootValue(context.Background(), mergeRoot)
	require.NoError(t, err)
	mergeRoot = r

	meta, err := datas.NewCommitMeta(name, email, "fake")
	require.NoError(t, err)
	initialCommit, err := ddb.Commit(context.Background(), mainHash, ref.NewBranchRef(env.DefaultInitBranch), meta)
	require.NoError(t, err)
	commit, err := ddb.Commit(context.Background(), hash, ref.NewBranchRef(env.DefaultInitBranch), meta)
	require.NoError(t, err)

	err = ddb.NewBranchAtCommit(context.Background(), ref.NewBranchRef("to-merge"), initialCommit)
	require.NoError(t, err)
	mergeCommit, err := ddb.Commit(context.Background(), mergeHash, ref.NewBranchRef("to-merge"), meta)
	require.NoError(t, err)

	root, err := commit.GetRootValue(context.Background())
	require.NoError(t, err)

	ancCm, err := doltdb.GetCommitAncestor(context.Background(), commit, mergeCommit)
	require.NoError(t, err)

	ancRoot, err := ancCm.GetRootValue(context.Background())
	require.NoError(t, err)

	ff, err := commit.CanFastForwardTo(context.Background(), mergeCommit)
	require.NoError(t, err)
	require.False(t, ff)

	return root, mergeRoot, ancRoot
}

func nomsKey(i int) types.Value {
	return mustTuple(types.NewTuple(types.Format_Default, types.Uint(idTag), types.Int(i)))
}

func unwrapNoms(v *rowV) types.Value {
	if v == nil {
		return nil
	}
	return v.nomsValue()
}

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
