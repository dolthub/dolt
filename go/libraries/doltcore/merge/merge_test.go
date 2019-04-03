package merge

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
	"strconv"
	"testing"
)

type RowMergeTest struct {
	name                  string
	row, mergeRow, ancRow types.Value
	sch                   schema.Schema
	expectedResult        types.Value
	expectConflict        bool
}

func valsToTestTuple(vals []types.Value) types.Value {
	if vals == nil {
		return nil
	}

	tplVals := make([]types.Value, 0, 2*len(vals))
	for i, val := range vals {
		if !types.IsNull(val) {
			tplVals = append(tplVals, types.Uint(i))
			tplVals = append(tplVals, val)
		}
	}

	return types.NewTuple(tplVals...)
}

func createRowMergeStruct(name string, vals, mergeVals, ancVals, expected []types.Value, expectCnf bool) RowMergeTest {
	longest := vals

	if len(mergeVals) > len(longest) {
		longest = mergeVals
	}

	if len(ancVals) > len(longest) {
		longest = ancVals
	}

	cols := make([]schema.Column, len(longest))
	for i, val := range longest {
		cols[i] = schema.NewColumn(strconv.FormatInt(int64(i), 10), uint64(i), val.Kind(), false)
	}

	colColl, _ := schema.NewColCollection(cols...)
	sch := schema.SchemaFromCols(colColl)

	tpl := valsToTestTuple(vals)
	mergeTpl := valsToTestTuple(mergeVals)
	ancTpl := valsToTestTuple(ancVals)
	expectedTpl := valsToTestTuple(expected)
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
			"modify row where intial value wasn't given",
			[]types.Value{types.NewTuple(types.String("one"), types.Uint(2), types.String("a"))},
			[]types.Value{types.NewTuple(types.String("one"), types.Uint(2), types.String("b"))},
			[]types.Value{types.NewTuple(types.String("one"), types.Uint(2), types.NullValue)},
			nil,
			true,
		),
	}

	for _, test := range tests {
		actualResult, isConflict := rowMerge(test.sch, test.row, test.mergeRow, test.ancRow)

		if test.expectedResult == nil {
			if actualResult != nil {
				t.Error("Test:", test.name, "failed. expected nil result, and got non, nil")
			}
		} else if !test.expectedResult.Equals(actualResult) {
			t.Error(
				"Test:", "\""+test.name+"\"", "failed.",
				"Merged row did not match expected. expected:\n\t", types.EncodedValue(test.expectedResult),
				"\nactual:\n\t", types.EncodedValue(actualResult))
		}

		if test.expectConflict != isConflict {
			t.Error("Test:", test.name, "expected conflict:", test.expectConflict, "actual conflict:", isConflict)
		}
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

var colColl, _ = schema.NewColCollection(
	schema.NewColumn("id", idTag, types.UUIDKind, true, schema.NotNullConstraint{}),
	schema.NewColumn("name", nameTag, types.StringKind, false, schema.NotNullConstraint{}),
	schema.NewColumn("title", titleTag, types.StringKind, false),
)
var sch = schema.SchemaFromCols(colColl)

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

func init() {
	keyTag := types.Uint(idTag)

	for i, id := range uuids {
		keyTuples[i] = types.NewTuple(keyTag, id)
	}
}

func setupMergeTest() (types.ValueReadWriter, *doltdb.Commit, *doltdb.Commit, types.Map, types.Map) {
	ddb := doltdb.LoadDoltDB(doltdb.InMemDoltDB)
	vrw := ddb.ValueReadWriter()

	err := ddb.WriteEmptyRepo(name, email)

	if err != nil {
		panic(err)
	}

	masterHeadSpec, _ := doltdb.NewCommitSpec("head", "master")
	masterHead, err := ddb.Resolve(masterHeadSpec)

	if err != nil {
		panic(err)
	}

	initialRows := types.NewMap(vrw,
		keyTuples[0], valsToTestTuple([]types.Value{types.String("person 1"), types.String("dufus")}),
		keyTuples[1], valsToTestTuple([]types.Value{types.String("person 2"), types.NullValue}),
		keyTuples[2], valsToTestTuple([]types.Value{types.String("person 3"), types.NullValue}),
		keyTuples[3], valsToTestTuple([]types.Value{types.String("person 4"), types.String("senior dufus")}),
		keyTuples[4], valsToTestTuple([]types.Value{types.String("person 5"), types.NullValue}),
		keyTuples[5], valsToTestTuple([]types.Value{types.String("person 6"), types.NullValue}),
		keyTuples[6], valsToTestTuple([]types.Value{types.String("person 7"), types.String("madam")}),
		keyTuples[7], valsToTestTuple([]types.Value{types.String("person 8"), types.String("miss")}),
		keyTuples[8], valsToTestTuple([]types.Value{types.String("person 9"), types.NullValue}),
	)

	updateRowEditor := initialRows.Edit()                                                                                // leave 0 as is
	updateRowEditor.Remove(keyTuples[1])                                                                                 // remove 1 from both
	updateRowEditor.Remove(keyTuples[2])                                                                                 // remove 2 from update
	updateRowEditor.Set(keyTuples[4], valsToTestTuple([]types.Value{types.String("person five"), types.NullValue}))      // modify 4 only in update
	updateRowEditor.Set(keyTuples[6], valsToTestTuple([]types.Value{types.String("person 7"), types.String("dr")}))      // modify 6 in both without overlap
	updateRowEditor.Set(keyTuples[7], valsToTestTuple([]types.Value{types.String("person eight"), types.NullValue}))     // modify 7 in both with equal overlap
	updateRowEditor.Set(keyTuples[8], valsToTestTuple([]types.Value{types.String("person nine"), types.NullValue}))      // modify 8 in both with conflicting overlap
	updateRowEditor.Set(keyTuples[9], valsToTestTuple([]types.Value{types.String("person ten"), types.NullValue}))       // add 9 in update
	updateRowEditor.Set(keyTuples[11], valsToTestTuple([]types.Value{types.String("person twelve"), types.NullValue}))   // add 11 in both without difference
	updateRowEditor.Set(keyTuples[12], valsToTestTuple([]types.Value{types.String("person thirteen"), types.NullValue})) // add 12 in both with differences

	updatedRows := updateRowEditor.Map()

	mergeRowEditor := initialRows.Edit()                                                                                       // leave 0 as is
	mergeRowEditor.Remove(keyTuples[1])                                                                                        // remove 1 from both
	mergeRowEditor.Remove(keyTuples[3])                                                                                        // remove 3 from merge
	mergeRowEditor.Set(keyTuples[5], valsToTestTuple([]types.Value{types.String("person six"), types.NullValue}))              // modify 5 only in merge
	mergeRowEditor.Set(keyTuples[6], valsToTestTuple([]types.Value{types.String("person seven"), types.String("madam")}))      // modify 6 in both without overlap
	mergeRowEditor.Set(keyTuples[7], valsToTestTuple([]types.Value{types.String("person eight"), types.NullValue}))            // modify 7 in both with equal overlap
	mergeRowEditor.Set(keyTuples[8], valsToTestTuple([]types.Value{types.String("person number nine"), types.NullValue}))      // modify 8 in both with conflicting overlap
	mergeRowEditor.Set(keyTuples[10], valsToTestTuple([]types.Value{types.String("person eleven"), types.NullValue}))          // add 10 in merge
	mergeRowEditor.Set(keyTuples[11], valsToTestTuple([]types.Value{types.String("person twelve"), types.NullValue}))          // add 11 in both without difference
	mergeRowEditor.Set(keyTuples[12], valsToTestTuple([]types.Value{types.String("person number thirteen"), types.NullValue})) // add 12 in both with differences

	mergeRows := mergeRowEditor.Map()

	expectedRows := types.NewMap(vrw,
		keyTuples[0], initialRows.Get(keyTuples[0]), // unaltered
		keyTuples[4], updatedRows.Get(keyTuples[4]), // modified in updated
		keyTuples[5], mergeRows.Get(keyTuples[5]), // modified in merged
		keyTuples[6], valsToTestTuple([]types.Value{types.String("person seven"), types.String("dr")}), // modified in both with no overlap
		keyTuples[7], updatedRows.Get(keyTuples[7]), // modify both with the same value
		keyTuples[8], updatedRows.Get(keyTuples[8]), // conflict
		keyTuples[9], updatedRows.Get(keyTuples[9]), // added in update
		keyTuples[10], mergeRows.Get(keyTuples[10]), // added in merge
		keyTuples[11], updatedRows.Get(keyTuples[11]), // added same in both
		keyTuples[12], updatedRows.Get(keyTuples[12]), // conflict
	)

	updateConflict := doltdb.NewConflict(initialRows.Get(keyTuples[8]), updatedRows.Get(keyTuples[8]), mergeRows.Get(keyTuples[8]))

	addConflict := doltdb.NewConflict(
		nil,
		valsToTestTuple([]types.Value{types.String("person thirteen"), types.NullValue}),
		valsToTestTuple([]types.Value{types.String("person number thirteen"), types.NullValue}),
	)
	expectedConflicts := types.NewMap(vrw,
		keyTuples[8], updateConflict.ToNomsList(vrw),
		keyTuples[12], addConflict.ToNomsList(vrw),
	)

	schVal, _ := encoding.MarshalAsNomsValue(vrw, sch)
	tbl := doltdb.NewTable(vrw, schVal, initialRows)
	updatedTbl := doltdb.NewTable(vrw, schVal, updatedRows)
	mergeTbl := doltdb.NewTable(vrw, schVal, mergeRows)

	mRoot := masterHead.GetRootValue()
	mRoot = mRoot.PutTable(ddb, tableName, tbl)
	updatedRoot := mRoot.PutTable(ddb, tableName, updatedTbl)
	mergeRoot := mRoot.PutTable(ddb, tableName, mergeTbl)

	masterHash, _ := ddb.WriteRootValue(mRoot)
	hash, _ := ddb.WriteRootValue(updatedRoot)
	mergeHash, _ := ddb.WriteRootValue(mergeRoot)

	meta, _ := doltdb.NewCommitMeta(name, email, "fake")
	initialCommit, _ := ddb.Commit(masterHash, "master", meta)
	commit, _ := ddb.Commit(hash, "master", meta)

	ddb.NewBranchAtCommit("to-merge", initialCommit)
	mergeCommit, _ := ddb.Commit(mergeHash, "to-merge", meta)

	return vrw, commit, mergeCommit, expectedRows, expectedConflicts
}

func TestMergeCommits(t *testing.T) {
	vrw, commit, mergeCommit, expectedRows, expectedConflicts := setupMergeTest()
	merger, err := NewMerger(commit, mergeCommit, vrw)

	if err != nil {
		t.Fatal(err)
	}

	merged, stats, err := merger.MergeTable(tableName)

	if err != nil {
		t.Fatal(err)
	}

	tbl, _ := commit.GetRootValue().GetTable(tableName)
	schRef := tbl.GetSchemaRef()
	expected := doltdb.NewTable(vrw, schRef.TargetValue(vrw), expectedRows)
	expected = expected.SetConflicts(doltdb.NewConflict(schRef, schRef, schRef), expectedConflicts)

	if stats.Adds != 2 || stats.Deletes != 2 || stats.Modifications != 3 || stats.Conflicts != 2 {
		t.Error("Actual stats differ from expected")
	}

	if merged.HashOf() != expected.HashOf() {
		mergedRows := merged.GetRowData()
		if !mergedRows.Equals(expectedRows) {
			t.Error(types.EncodedValue(mergedRows), "\n!=\n", types.EncodedValue(expectedRows))
		}
	}
}
