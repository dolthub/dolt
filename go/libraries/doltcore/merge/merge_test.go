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
	"encoding/json"
	"sort"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	filesys2 "github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/valutil"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
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

type rowV struct {
	col1, col2 int
}

var vD = sch.GetValueDescriptor()
var vB = val.NewTupleBuilder(vD)
var syncPool = pool.NewBuffPool()

func (v rowV) value() val.Tuple {
	vB.PutInt64(0, int64(v.col1))
	vB.PutInt64(1, int64(v.col2))
	return vB.Build(syncPool)
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
//
// A modification of a primary key is the combination of the two base cases:
// First, a (delete, delete), then an (insert, insert). We omit tests for these
// and instead defer to the base cases.

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
	// Non-conflicting update 2
	{
		62,
		&rowV{62, 62},
		UpdateAction,
		UpdateAction,
		&rowV{-62, 62},
		&rowV{62, -62},
		false,
		&rowV{-62, -62},
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

func TestMergeCommits(t *testing.T) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip()
	}

	ddb, vrw, ns, rightCommitHash, ancCommitHash, root, mergeRoot, ancRoot, expectedRows, expectedArtifacts := setupMergeTest(t)
	defer ddb.Close()
	merger, err := NewMerger(root, mergeRoot, ancRoot, rightCommitHash, ancCommitHash, vrw, ns)
	if err != nil {
		t.Fatal(err)
	}
	opts := editor.TestEditorOptions(vrw)
	// TODO: stats
	merged, _, err := merger.MergeTable(sql.NewContext(context.Background()), doltdb.TableName{Name: tableName}, opts, MergeOpts{IsCherryPick: false})
	if err != nil {
		t.Fatal(err)
	}

	ctx := sql.NewEmptyContext()

	tbl, _, err := root.GetTable(ctx, doltdb.TableName{Name: tableName})
	assert.NoError(t, err)
	sch, err := tbl.GetSchema(ctx)
	assert.NoError(t, err)
	expected, err := doltdb.NewTable(ctx, vrw, ns, sch, expectedRows, nil, nil)
	assert.NoError(t, err)
	expected, err = rebuildAllProllyIndexes(ctx, expected)
	assert.NoError(t, err)
	expected, err = expected.SetArtifacts(ctx, durable.ArtifactIndexFromProllyMap(expectedArtifacts))
	require.NoError(t, err)

	mergedRows, err := merged.table.GetRowData(ctx)
	assert.NoError(t, err)

	artIdx, err := merged.table.GetArtifacts(ctx)
	require.NoError(t, err)
	artifacts := durable.ProllyMapFromArtifactIndex(artIdx)
	MustEqualArtifactMap(t, expectedArtifacts, artifacts)

	MustEqualProlly(t, tableName, durable.ProllyMapFromIndex(expectedRows), durable.ProllyMapFromIndex(mergedRows))

	for _, index := range sch.Indexes().AllIndexes() {
		mergedIndexRows, err := merged.table.GetIndexRowData(ctx, index.Name())
		require.NoError(t, err)
		expectedIndexRows, err := expected.GetIndexRowData(ctx, index.Name())
		require.NoError(t, err)
		MustEqualProlly(t, index.Name(), durable.ProllyMapFromIndex(expectedIndexRows), durable.ProllyMapFromIndex(mergedIndexRows))
	}

	h, err := merged.table.HashOf()
	require.NoError(t, err)
	eh, err := expected.HashOf()
	require.NoError(t, err)
	require.Equal(t, eh.String(), h.String(), "table hashes do not equal")
}

func TestNomsMergeCommits(t *testing.T) {
	if types.IsFormat_DOLT(types.Format_Default) {
		t.Skip()
	}

	vrw, ns, rightCommitHash, ancCommitHash, root, mergeRoot, ancRoot, expectedRows, expectedConflicts, expectedStats := setupNomsMergeTest(t)

	merger, err := NewMerger(root, mergeRoot, ancRoot, rightCommitHash, ancCommitHash, vrw, ns)
	if err != nil {
		t.Fatal(err)
	}
	opts := editor.TestEditorOptions(vrw)
	merged, stats, err := merger.MergeTable(sql.NewContext(context.Background()), doltdb.TableName{Name: tableName}, opts, MergeOpts{IsCherryPick: false})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expectedStats, stats, "received stats is incorrect")

	tbl, _, err := root.GetTable(context.Background(), doltdb.TableName{Name: tableName})
	assert.NoError(t, err)
	sch, err := tbl.GetSchema(context.Background())
	assert.NoError(t, err)
	expected, err := doltdb.NewNomsTable(context.Background(), vrw, ns, sch, expectedRows, nil, nil)
	assert.NoError(t, err)
	expected, err = editor.RebuildAllIndexes(context.Background(), expected, editor.TestEditorOptions(vrw))
	assert.NoError(t, err)
	conflictSchema := conflict.NewConflictSchema(sch, sch, sch)
	assert.NoError(t, err)
	expected, err = expected.SetConflicts(context.Background(), conflictSchema, durable.ConflictIndexFromNomsMap(expectedConflicts, vrw))
	assert.NoError(t, err)

	mergedRows, err := merged.table.GetNomsRowData(context.Background())
	assert.NoError(t, err)
	_, confIdx, err := merged.table.GetConflicts(context.Background())
	assert.NoError(t, err)
	conflicts := durable.NomsMapFromConflictIndex(confIdx)

	if !mergedRows.Equals(expectedRows) {
		t.Error(mustString(types.EncodedValue(context.Background(), expectedRows)), "\n!=\n", mustString(types.EncodedValue(context.Background(), mergedRows)))
	}
	if !conflicts.Equals(expectedConflicts) {
		t.Error(mustString(types.EncodedValue(context.Background(), expectedConflicts)), "\n!=\n", mustString(types.EncodedValue(context.Background(), conflicts)))
	}

	for _, index := range sch.Indexes().AllIndexes() {
		mergedIndexRows, err := merged.table.GetNomsIndexRowData(context.Background(), index.Name())
		assert.NoError(t, err)
		expectedIndexRows, err := expected.GetNomsIndexRowData(context.Background(), index.Name())
		assert.NoError(t, err)
		assert.Equal(t, expectedRows.Len(), mergedIndexRows.Len(), "index %s incorrect row count", index.Name())
		assert.Truef(t, expectedIndexRows.Equals(mergedIndexRows),
			"index %s contents incorrect.\nExpected: \n%s\nReceived: \n%s\n", index.Name(),
			mustString(types.EncodedValue(context.Background(), expectedIndexRows)),
			mustString(types.EncodedValue(context.Background(), mergedIndexRows)))
	}

	h, err := merged.table.HashOf()
	assert.NoError(t, err)
	eh, err := expected.HashOf()
	assert.NoError(t, err)
	assert.Equal(t, eh.String(), h.String(), "table hashes do not equal")
}

func sortTests(t []testRow) {
	sort.Slice(t, func(i, j int) bool {
		return t[i].key < t[j].key
	})
}

func setupMergeTest(t *testing.T) (*doltdb.DoltDB, types.ValueReadWriter, tree.NodeStore, doltdb.Rootish, doltdb.Rootish, doltdb.RootValue, doltdb.RootValue, doltdb.RootValue, durable.Index, prolly.ArtifactMap) {
	ddb := mustMakeEmptyRepo(t)
	vrw := ddb.ValueReadWriter()
	ns := ddb.NodeStore()
	sortTests(testRows)

	var initialKVs []val.Tuple
	var expectedKVs []val.Tuple

	for _, testCase := range testRows {
		if testCase.initialValue != nil {
			initialKVs = append(initialKVs, key(testCase.key), testCase.initialValue.value())
		}
		if testCase.expectedValue != nil {
			expectedKVs = append(expectedKVs, key(testCase.key), testCase.expectedValue.value())
		}
	}

	initialRows, err := prolly.NewMapFromTuples(context.Background(), ns, kD, vD, initialKVs...)
	require.NoError(t, err)
	expectedRows, err := prolly.NewMapFromTuples(context.Background(), ns, kD, vD, expectedKVs...)
	require.NoError(t, err)

	leftMut := initialRows.Mutate()
	rightMut := initialRows.Mutate()
	for _, testCase := range testRows {

		switch testCase.leftAction {
		case NoopAction:
			break
		case InsertAction, UpdateAction:
			err = leftMut.Put(context.Background(), key(testCase.key), testCase.leftValue.value())
			require.NoError(t, err)
		case DeleteAction:
			err = leftMut.Delete(context.Background(), key(testCase.key))
			require.NoError(t, err)
		}

		switch testCase.rightAction {
		case NoopAction:
			break
		case InsertAction, UpdateAction:
			err = rightMut.Put(context.Background(), key(testCase.key), testCase.rightValue.value())
			require.NoError(t, err)
		case DeleteAction:
			err = rightMut.Delete(context.Background(), key(testCase.key))
			require.NoError(t, err)
		}
	}

	ctx := sql.NewEmptyContext()

	updatedRows, err := leftMut.Map(ctx)
	require.NoError(t, err)
	mergeRows, err := rightMut.Map(ctx)
	require.NoError(t, err)

	rootTbl, err := doltdb.NewTable(ctx, vrw, ns, sch, durable.IndexFromProllyMap(updatedRows), nil, nil)
	require.NoError(t, err)
	rootTbl, err = rebuildAllProllyIndexes(ctx, rootTbl)
	require.NoError(t, err)

	mergeTbl, err := doltdb.NewTable(ctx, vrw, ns, sch, durable.IndexFromProllyMap(mergeRows), nil, nil)
	require.NoError(t, err)
	mergeTbl, err = rebuildAllProllyIndexes(ctx, mergeTbl)
	require.NoError(t, err)

	ancTbl, err := doltdb.NewTable(ctx, vrw, ns, sch, durable.IndexFromProllyMap(initialRows), nil, nil)
	require.NoError(t, err)
	ancTbl, err = rebuildAllProllyIndexes(ctx, ancTbl)
	require.NoError(t, err)

	rightCm, baseCm, root, mergeRoot, ancRoot := buildLeftRightAncCommitsAndBranches(t, ddb, rootTbl, mergeTbl, ancTbl)

	artifactMap, err := prolly.NewArtifactMapFromTuples(ctx, ns, kD)
	require.NoError(t, err)
	artEditor := artifactMap.Editor()

	baseCmHash, err := baseCm.HashOf()
	require.NoError(t, err)
	rightCmHash, err := rightCm.HashOf()
	require.NoError(t, err)

	m := prolly.ConflictMetadata{
		BaseRootIsh: baseCmHash,
	}
	d, err := json.Marshal(m)
	require.NoError(t, err)

	for _, testCase := range testRows {
		if testCase.conflict {
			err = artEditor.Add(ctx, key(testCase.key), rightCmHash, prolly.ArtifactTypeConflict, d)
			require.NoError(t, err)
		}
	}

	expectedArtifacts, err := artEditor.Flush(ctx)
	require.NoError(t, err)

	return ddb, vrw, ns, rightCm, baseCm, root, mergeRoot, ancRoot, durable.IndexFromProllyMap(expectedRows), expectedArtifacts
}

func setupNomsMergeTest(t *testing.T) (types.ValueReadWriter, tree.NodeStore, doltdb.Rootish, doltdb.Rootish, doltdb.RootValue, doltdb.RootValue, doltdb.RootValue, types.Map, types.Map, *MergeStats) {
	ddb := mustMakeEmptyRepo(t)
	vrw := ddb.ValueReadWriter()
	ns := ddb.NodeStore()
	sortTests(testRows)

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

	tbl, err := doltdb.NewNomsTable(context.Background(), vrw, ns, sch, initialRows, nil, nil)
	require.NoError(t, err)
	tbl, err = editor.RebuildAllIndexes(context.Background(), tbl, editor.TestEditorOptions(vrw))
	require.NoError(t, err)

	updatedTbl, err := doltdb.NewNomsTable(context.Background(), vrw, ns, sch, updatedRows, nil, nil)
	require.NoError(t, err)
	updatedTbl, err = editor.RebuildAllIndexes(context.Background(), updatedTbl, editor.TestEditorOptions(vrw))
	require.NoError(t, err)

	mergeTbl, err := doltdb.NewNomsTable(context.Background(), vrw, ns, sch, mergeRows, nil, nil)
	require.NoError(t, err)
	mergeTbl, err = editor.RebuildAllIndexes(context.Background(), mergeTbl, editor.TestEditorOptions(vrw))
	require.NoError(t, err)

	ancTable, err := doltdb.NewNomsTable(context.Background(), vrw, ns, sch, initialRows, nil, nil)
	require.NoError(t, err)
	ancTable, err = editor.RebuildAllIndexes(context.Background(), ancTable, editor.TestEditorOptions(vrw))
	require.NoError(t, err)

	rightCm, ancCommit, root, mergeRoot, ancRoot := buildLeftRightAncCommitsAndBranches(t, ddb, updatedTbl, mergeTbl, ancTable)

	return vrw, ns, rightCm, ancCommit, root, mergeRoot, ancRoot, expectedRows, expectedConflicts, calcExpectedStats(t)
}

// rebuildAllProllyIndexes builds the data for the secondary indexes in |tbl|'s
// schema.
func rebuildAllProllyIndexes(ctx *sql.Context, tbl *doltdb.Table) (*doltdb.Table, error) {
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if sch.Indexes().Count() == 0 {
		return tbl, nil
	}

	indexes, err := tbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	tableRowData, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	primary := durable.ProllyMapFromIndex(tableRowData)

	for _, index := range sch.Indexes().AllIndexes() {
		rebuiltIndexRowData, err := creation.BuildSecondaryProllyIndex(ctx, tbl.ValueReadWriter(), tbl.NodeStore(), sch, tableName, index, primary)
		if err != nil {
			return nil, err
		}

		indexes, err = indexes.PutIndex(ctx, index.Name(), rebuiltIndexRowData)
		if err != nil {
			return nil, err
		}
	}

	return tbl.SetIndexSet(ctx, indexes)
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
			s.DataConflicts++
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

func buildLeftRightAncCommitsAndBranches(t *testing.T, ddb *doltdb.DoltDB, rootTbl, mergeTbl, ancTbl *doltdb.Table) (doltdb.Rootish, doltdb.Rootish, doltdb.RootValue, doltdb.RootValue, doltdb.RootValue) {
	mainHeadSpec, _ := doltdb.NewCommitSpec(env.DefaultInitBranch)
	optCmt, err := ddb.Resolve(context.Background(), mainHeadSpec, nil)
	require.NoError(t, err)
	mainHead, ok := optCmt.ToCommit()
	require.True(t, ok)

	mRoot, err := mainHead.GetRootValue(context.Background())
	require.NoError(t, err)

	mRoot, err = mRoot.PutTable(context.Background(), doltdb.TableName{Name: tableName}, ancTbl)
	require.NoError(t, err)

	updatedRoot, err := mRoot.PutTable(context.Background(), doltdb.TableName{Name: tableName}, rootTbl)
	require.NoError(t, err)

	mergeRoot, err := mRoot.PutTable(context.Background(), doltdb.TableName{Name: tableName}, mergeTbl)
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

	err = ddb.NewBranchAtCommit(context.Background(), ref.NewBranchRef("to-merge"), initialCommit, nil)
	require.NoError(t, err)
	mergeCommit, err := ddb.Commit(context.Background(), mergeHash, ref.NewBranchRef("to-merge"), meta)
	require.NoError(t, err)

	root, err := commit.GetRootValue(context.Background())
	require.NoError(t, err)

	optCmt, err = doltdb.GetCommitAncestor(context.Background(), commit, mergeCommit)
	require.NoError(t, err)
	ancCm, ok := optCmt.ToCommit()
	require.True(t, ok)

	ancRoot, err := ancCm.GetRootValue(context.Background())
	require.NoError(t, err)

	ff, err := commit.CanFastForwardTo(context.Background(), mergeCommit)
	require.NoError(t, err)
	require.False(t, ff)

	return mergeCommit, ancCm, root, mergeRoot, ancRoot
}

var kD = sch.GetKeyDescriptor()
var kB = val.NewTupleBuilder(kD)

func key(i int) val.Tuple {
	kB.PutInt64(0, int64(i))
	return kB.Build(syncPool)
}

func nomsKey(i int) types.Value {
	return mustTuple(types.NewTuple(types.Format_Default, types.Uint(idTag), types.Int(i)))
}

func unwrap(v *rowV) val.Tuple {
	if v == nil {
		return nil
	}
	return v.value()
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

func MustDebugFormatProlly(t *testing.T, m prolly.Map) string {
	s, err := prolly.DebugFormat(context.Background(), m)
	require.NoError(t, err)
	return s
}

func MustEqualProlly(t *testing.T, name string, expected, actual prolly.Map) {
	require.Equal(t, expected.HashOf(), actual.HashOf(),
		"hashes differed for %s. expected: %s\nactual: %s", name, MustDebugFormatProlly(t, expected), MustDebugFormatProlly(t, actual))
}

func MustEqualArtifactMap(t *testing.T, expected prolly.ArtifactMap, actual prolly.ArtifactMap) {
	require.Equal(t, expected.HashOf(), actual.HashOf(),
		"artifact map hashes differed.")
}
