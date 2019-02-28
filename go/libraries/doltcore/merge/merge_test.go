package merge

import (
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
)

func TestRowMerge(t *testing.T) {
	tests := []struct {
		name                  string
		row, mergeRow, ancRow types.Value
		expectedResult        types.Value
		expectConflict        bool
	}{
		{
			"add same row",
			types.NewTuple(types.String("one"), types.Int(2)),
			types.NewTuple(types.String("one"), types.Int(2)),
			nil,
			types.NewTuple(types.String("one"), types.Int(2)),
			false,
		},
		{
			"add diff row",
			types.NewTuple(types.String("one"), types.String("two")),
			types.NewTuple(types.String("one"), types.String("three")),
			nil,
			nil,
			true,
		},
		{
			"both delete row",
			nil,
			nil,
			types.NewTuple(types.String("one"), types.Uint(2)),
			nil,
			false,
		},
		{
			"one delete one modify",
			nil,
			types.NewTuple(types.String("two"), types.Uint(2)),
			types.NewTuple(types.String("one"), types.Uint(2)),
			nil,
			true,
		},
		{
			"modify rows without overlap",
			types.NewTuple(types.String("two"), types.Uint(2)),
			types.NewTuple(types.String("one"), types.Uint(3)),
			types.NewTuple(types.String("one"), types.Uint(2)),
			types.NewTuple(types.String("two"), types.Uint(3)),
			false,
		},
		{
			"modify rows with equal overlapping changes",
			types.NewTuple(types.String("two"), types.Uint(2), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))),
			types.NewTuple(types.String("one"), types.Uint(3), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))),
			types.NewTuple(types.String("one"), types.Uint(2), types.UUID(uuid.MustParse("00000000-0000-0000-0000-000000000000"))),
			types.NewTuple(types.String("two"), types.Uint(3), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))),
			false,
		},
		{
			"modify rows with differing overlapping changes",
			types.NewTuple(types.String("two"), types.Uint(2), types.UUID(uuid.MustParse("99999999-9999-9999-9999-999999999999"))),
			types.NewTuple(types.String("one"), types.Uint(3), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))),
			types.NewTuple(types.String("one"), types.Uint(2), types.UUID(uuid.MustParse("00000000-0000-0000-0000-000000000000"))),
			nil,
			true,
		},
		{
			"modify rows where one adds a column",
			types.NewTuple(types.String("two"), types.Uint(2)),
			types.NewTuple(types.String("one"), types.Uint(3), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))),
			types.NewTuple(types.String("one"), types.Uint(2)),
			types.NewTuple(types.String("two"), types.Uint(3), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))),
			false,
		},
		{
			"modify row where values added in different columns",
			types.NewTuple(types.String("one"), types.Uint(2), types.String(""), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))),
			types.NewTuple(types.String("one"), types.Uint(2), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")), types.String("")),
			types.NewTuple(types.String("one"), types.Uint(2), types.NullValue, types.NullValue),
			nil,

			true,
		},
		{
			"modify row where intial value wasn't given",
			types.NewTuple(types.String("one"), types.Uint(2), types.String("a")),
			types.NewTuple(types.String("one"), types.Uint(2), types.String("b")),
			types.NewTuple(types.String("one"), types.Uint(2), types.NullValue),
			nil,
			true,
		},
	}

	for _, test := range tests {
		actualResult, isConflict := rowMerge(test.row, test.mergeRow, test.ancRow)

		if test.expectedResult == nil {
			if actualResult != nil {
				t.Error("Test:", test.name, "failed. expected nil result, and got non, nil")
			}
		} else if !test.expectedResult.Equals(actualResult) {
			t.Error(
				"Test:", test.name, "failed.",
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
)

var schFlds = []*schema.Field{
	schema.NewField("id", types.UUIDKind, true),
	schema.NewField("name", types.StringKind, true),
	schema.NewField("title", types.StringKind, false),
}
var sch = schema.NewSchema(schFlds)

func init() {
	sch.AddConstraint(schema.NewConstraint(schema.PrimaryKey, []int{0}))
}

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
		uuids[0], types.NewTuple(types.String("person 1"), types.String("dufus")),
		uuids[1], types.NewTuple(types.String("person 2"), types.NullValue),
		uuids[2], types.NewTuple(types.String("person 3"), types.NullValue),
		uuids[3], types.NewTuple(types.String("person 4"), types.String("senior dufus")),
		uuids[4], types.NewTuple(types.String("person 5"), types.NullValue),
		uuids[5], types.NewTuple(types.String("person 6"), types.NullValue),
		uuids[6], types.NewTuple(types.String("person 7"), types.String("madam")),
		uuids[7], types.NewTuple(types.String("person 8"), types.String("miss")),
		uuids[8], types.NewTuple(types.String("person 9"), types.NullValue),
	)

	updateRowEditor := initialRows.Edit()                                                            // leave 0 as is
	updateRowEditor.Remove(uuids[1])                                                                 // remove 1 from both
	updateRowEditor.Remove(uuids[2])                                                                 // remove 2 from update
	updateRowEditor.Set(uuids[4], types.NewTuple(types.String("person five"), types.NullValue))      // modify 4 only in update
	updateRowEditor.Set(uuids[6], types.NewTuple(types.String("person 7"), types.String("dr")))      // modify 6 in both without overlap
	updateRowEditor.Set(uuids[7], types.NewTuple(types.String("person eight"), types.NullValue))     // modify 7 in both with equal overlap
	updateRowEditor.Set(uuids[8], types.NewTuple(types.String("person nine"), types.NullValue))      // modify 8 in both with conflicting overlap
	updateRowEditor.Set(uuids[9], types.NewTuple(types.String("person ten"), types.NullValue))       // add 9 in update
	updateRowEditor.Set(uuids[11], types.NewTuple(types.String("person twelve"), types.NullValue))   // add 11 in both without difference
	updateRowEditor.Set(uuids[12], types.NewTuple(types.String("person thirteen"), types.NullValue)) // add 12 in both with differences

	updatedRows := updateRowEditor.Map()

	mergeRowEditor := initialRows.Edit()                                                                   // leave 0 as is
	mergeRowEditor.Remove(uuids[1])                                                                        // remove 1 from both
	mergeRowEditor.Remove(uuids[3])                                                                        // remove 3 from merge
	mergeRowEditor.Set(uuids[5], types.NewTuple(types.String("person six"), types.NullValue))              // modify 5 only in merge
	mergeRowEditor.Set(uuids[6], types.NewTuple(types.String("person seven"), types.String("madam")))      // modify 6 in both without overlap
	mergeRowEditor.Set(uuids[7], types.NewTuple(types.String("person eight"), types.NullValue))            // modify 7 in both with equal overlap
	mergeRowEditor.Set(uuids[8], types.NewTuple(types.String("person number nine"), types.NullValue))      // modify 8 in both with conflicting overlap
	mergeRowEditor.Set(uuids[10], types.NewTuple(types.String("person eleven"), types.NullValue))          // add 10 in merge
	mergeRowEditor.Set(uuids[11], types.NewTuple(types.String("person twelve"), types.NullValue))          // add 11 in both without difference
	mergeRowEditor.Set(uuids[12], types.NewTuple(types.String("person number thirteen"), types.NullValue)) // add 12 in both with differences

	mergeRows := mergeRowEditor.Map()

	expectedRows := types.NewMap(vrw,
		uuids[0], initialRows.Get(uuids[0]), // unaltered
		uuids[4], updatedRows.Get(uuids[4]), // modified in updated
		uuids[5], mergeRows.Get(uuids[5]), // modified in merged
		uuids[6], types.NewTuple(types.String("person seven"), types.String("dr")), // modified in both with no overlap
		uuids[7], updatedRows.Get(uuids[7]), // modify both with the same value
		uuids[8], updatedRows.Get(uuids[8]), // conflict
		uuids[9], updatedRows.Get(uuids[9]), // added in update
		uuids[10], mergeRows.Get(uuids[10]), // added in merge
		uuids[11], updatedRows.Get(uuids[11]), // added same in both
		uuids[12], updatedRows.Get(uuids[12]), // conflict
	)

	updateConflict := doltdb.NewConflict(initialRows.Get(uuids[8]), updatedRows.Get(uuids[8]), mergeRows.Get(uuids[8]))
	addConflict := doltdb.NewConflict(
		nil,
		types.NewTuple(types.String("person thirteen"), types.NullValue),
		types.NewTuple(types.String("person number thirteen"), types.NullValue),
	)
	expectedConflicts := types.NewMap(vrw,
		uuids[8], updateConflict.ToNomsList(vrw),
		uuids[12], addConflict.ToNomsList(vrw),
	)

	schVal, _ := noms.MarshalAsNomsValue(vrw, sch)
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

	meta := doltdb.NewCommitMeta(name, email, "fake")
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
