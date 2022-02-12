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
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
	"github.com/dolthub/dolt/go/store/types"
)

// This file collects useful test table definitions and functions for SQL tests to use. It primarily defines a table
// name, schema, and some sample rows to use in tests, as well as functions for creating and seeding a test database,
// transforming row results, and so on.

const (
	IdTag = iota + 200
	FirstNameTag
	LastNameTag
	IsMarriedTag
	AgeTag
	emptyTag
	RatingTag
	UuidTag
	NumEpisodesTag
	firstUnusedTag // keep at end
)

const (
	EpisodeIdTag = iota + 300
	EpNameTag
	EpAirDateTag
	EpRatingTag
)

const (
	AppCharacterTag = iota + 400
	AppEpTag
	AppCommentsTag
)

const (
	HomerId = iota
	MargeId
	BartId
	LisaId
	MoeId
	BarneyId
)

var PeopleTestSchema = createPeopleTestSchema()
var untypedPeopleSch, _ = untyped.UntypeUnkeySchema(PeopleTestSchema)
var PeopleTableName = "people"

var EpisodesTestSchema = createEpisodesTestSchema()
var untypedEpisodesSch, _ = untyped.UntypeUnkeySchema(EpisodesTestSchema)
var EpisodesTableName = "episodes"

var AppearancesTestSchema = createAppearancesTestSchema()
var untypedAppearacesSch, _ = untyped.UntypeUnkeySchema(AppearancesTestSchema)
var AppearancesTableName = "appearances"

func createPeopleTestSchema() schema.Schema {
	colColl := schema.NewColCollection(
		schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("is_married", IsMarriedTag, types.BoolKind, false),
		schema.NewColumn("age", AgeTag, types.IntKind, false),
		schema.NewColumn("rating", RatingTag, types.FloatKind, false),
		schema.NewColumn("uuid", UuidTag, types.UUIDKind, false),
		schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
	)
	return schema.MustSchemaFromCols(colColl)
}

func createEpisodesTestSchema() schema.Schema {
	colColl := schema.NewColCollection(
		schema.NewColumn("id", EpisodeIdTag, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("name", EpNameTag, types.StringKind, false, schema.NotNullConstraint{}),
		newColumnWithTypeInfo("air_date", EpAirDateTag, typeinfo.DatetimeType, false),
		schema.NewColumn("rating", EpRatingTag, types.FloatKind, false),
	)
	return schema.MustSchemaFromCols(colColl)
}

func createAppearancesTestSchema() schema.Schema {
	colColl := schema.NewColCollection(
		schema.NewColumn("character_id", AppCharacterTag, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("episode_id", AppEpTag, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("comments", AppCommentsTag, types.StringKind, false),
	)
	return schema.MustSchemaFromCols(colColl)
}

func newColumnWithTypeInfo(name string, tag uint64, info typeinfo.TypeInfo, partOfPk bool, constraints ...schema.ColConstraint) schema.Column {
	col, err := schema.NewColumnWithTypeInfo(name, tag, info, partOfPk, "", false, "", constraints...)
	if err != nil {
		panic(fmt.Sprintf("unexpected error creating column: %s", err.Error()))
	}
	return col
}

func NewPeopleRow(id int, first, last string, isMarried bool, age int, rating float64) row.Row {
	vals := row.TaggedValues{
		IdTag:        types.Int(id),
		FirstNameTag: types.String(first),
		LastNameTag:  types.String(last),
		IsMarriedTag: types.Bool(isMarried),
		AgeTag:       types.Int(age),
		RatingTag:    types.Float(rating),
	}

	r, err := row.New(types.Format_Default, PeopleTestSchema, vals)

	if err != nil {
		panic(err)
	}

	return r
}

func newEpsRow2(id int, name string, airdate string, rating float64) row.Row {
	vals := row.TaggedValues{
		EpisodeIdTag: types.Int(id),
		EpNameTag:    types.String(name),
		EpAirDateTag: types.Timestamp(DatetimeStrToTimestamp(airdate)),
		EpRatingTag:  types.Float(rating),
	}

	r, err := row.New(types.Format_Default, EpisodesTestSchema, vals)

	if err != nil {
		panic(err)
	}

	return r
}

func DatetimeStrToTimestamp(datetime string) time.Time {
	time, err := time.Parse("2006-01-02 15:04:05", datetime)
	if err != nil {
		panic(fmt.Sprintf("unable to parse datetime %s", datetime))
	}
	return time
}

func newAppsRow2(charId, epId int, comment string) row.Row {
	vals := row.TaggedValues{
		AppCharacterTag: types.Int(charId),
		AppEpTag:        types.Int(epId),
		AppCommentsTag:  types.String(comment),
	}

	r, err := row.New(types.Format_Default, AppearancesTestSchema, vals)

	if err != nil {
		panic(err)
	}

	return r
}

// Most rows don't have these optional fields set, as they aren't needed for basic testing
func NewPeopleRowWithOptionalFields(id int, first, last string, isMarried bool, age int, rating float64, uid uuid.UUID, numEpisodes uint64) row.Row {
	vals := row.TaggedValues{
		IdTag:          types.Int(id),
		FirstNameTag:   types.String(first),
		LastNameTag:    types.String(last),
		IsMarriedTag:   types.Bool(isMarried),
		AgeTag:         types.Int(age),
		RatingTag:      types.Float(rating),
		UuidTag:        types.UUID(uid),
		NumEpisodesTag: types.Uint(numEpisodes),
	}

	r, err := row.New(types.Format_Default, PeopleTestSchema, vals)

	if err != nil {
		panic(err)
	}

	return r
}

// 6 characters
var Homer = NewPeopleRow(HomerId, "Homer", "Simpson", true, 40, 8.5)
var Marge = NewPeopleRowWithOptionalFields(MargeId, "Marge", "Simpson", true, 38, 8, uuid.MustParse("00000000-0000-0000-0000-000000000001"), 111)
var Bart = NewPeopleRowWithOptionalFields(BartId, "Bart", "Simpson", false, 10, 9, uuid.MustParse("00000000-0000-0000-0000-000000000002"), 222)
var Lisa = NewPeopleRowWithOptionalFields(LisaId, "Lisa", "Simpson", false, 8, 10, uuid.MustParse("00000000-0000-0000-0000-000000000003"), 333)
var Moe = NewPeopleRowWithOptionalFields(MoeId, "Moe", "Szyslak", false, 48, 6.5, uuid.MustParse("00000000-0000-0000-0000-000000000004"), 444)
var Barney = NewPeopleRowWithOptionalFields(BarneyId, "Barney", "Gumble", false, 40, 4, uuid.MustParse("00000000-0000-0000-0000-000000000005"), 555)
var AllPeopleRows = Rs(Homer, Marge, Bart, Lisa, Moe, Barney)

// Actually the first 4 episodes of the show
var Ep1 = newEpsRow2(1, "Simpsons Roasting On an Open Fire", "1989-12-18 03:00:00", 8.0)
var Ep2 = newEpsRow2(2, "Bart the Genius", "1990-01-15 03:00:00", 9.0)
var Ep3 = newEpsRow2(3, "Homer's Odyssey", "1990-01-22 03:00:00", 7.0)
var Ep4 = newEpsRow2(4, "There's No Disgrace Like Home", "1990-01-29 03:00:00", 8.5)
var AllEpsRows = Rs(Ep1, Ep2, Ep3, Ep4)

// These are made up, not the actual show data
var app1 = newAppsRow2(HomerId, 1, "Homer is great in this one")
var app2 = newAppsRow2(MargeId, 1, "Marge is here too")
var app3 = newAppsRow2(HomerId, 2, "Homer is great in this one too")
var app4 = newAppsRow2(BartId, 2, "This episode is named after Bart")
var app5 = newAppsRow2(LisaId, 2, "Lisa is here too")
var app6 = newAppsRow2(MoeId, 2, "I think there's a prank call scene")
var app7 = newAppsRow2(HomerId, 3, "Homer is in every episode")
var app8 = newAppsRow2(MargeId, 3, "Marge shows up a lot too")
var app9 = newAppsRow2(LisaId, 3, "Lisa is the best Simpson")
var app10 = newAppsRow2(BarneyId, 3, "I'm making this all up")

// nobody in episode 4, that one was terrible
// Unlike the other tables, you can't count on the order of these rows matching the insertion order.
var AllAppsRows = Rs(app1, app2, app3, app4, app5, app6, app7, app8, app9, app10)

// Convenience func to avoid the boilerplate of typing []row.Row{} all the time
func Rs(rows ...row.Row) []row.Row {
	if rows == nil {
		return make([]row.Row, 0)
	}
	return rows
}

// Returns the index of the first row in the list that has the same primary key as the one given, or -1 otherwise.
func FindRowIndex(find row.Row, rows []row.Row) int {
	idx := -1
	for i, updatedRow := range rows {
		rowId, _ := find.GetColVal(IdTag)
		updatedId, _ := updatedRow.GetColVal(IdTag)
		if rowId.Equals(updatedId) {
			idx = i
			break
		}
	}
	return idx
}

// Mutates the row given with pairs of {tag,value} given in the varargs param. Converts built-in types to noms types.
func MutateRow(sch schema.Schema, r row.Row, tagsAndVals ...interface{}) row.Row {
	if len(tagsAndVals)%2 != 0 {
		panic("expected pairs of tags and values")
	}

	var mutated row.Row = r
	var err error

	for i := 0; i < len(tagsAndVals); i += 2 {
		tag := tagsAndVals[i].(int)
		val := tagsAndVals[i+1]
		var nomsVal types.Value
		if val != nil {
			switch v := val.(type) {
			case uint64:
				nomsVal = types.Uint(v)
			case int:
				nomsVal = types.Int(v)
			case int32:
				nomsVal = types.Int(v)
			case int64:
				nomsVal = types.Int(v)
			case float32:
				nomsVal = types.Float(v)
			case float64:
				nomsVal = types.Float(v)
			case string:
				nomsVal = types.String(v)
			case uuid.UUID:
				nomsVal = types.UUID(v)
			case bool:
				nomsVal = types.Bool(v)
			case time.Time:
				nomsVal = types.Timestamp(v)
			default:
				panic("Unhandled type " + reflect.TypeOf(val).String())
			}
		} else {
			nomsVal = nil
		}

		mutated, err = mutated.SetColVal(uint64(tag), nomsVal, sch)
		if err != nil {
			panic(err.Error())
		}
	}

	return mutated
}

func GetAllRows(root *doltdb.RootValue, tableName string) ([]row.Row, error) {
	ctx := context.Background()
	table, _, err := root.GetTable(ctx, tableName)

	if err != nil {
		return nil, err
	}

	rowData, err := table.GetNomsRowData(ctx)

	if err != nil {
		return nil, err
	}

	sch, err := table.GetSchema(ctx)

	if err != nil {
		return nil, err
	}

	var rows []row.Row
	err = rowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
		r, err := row.FromNoms(sch, key.(types.Tuple), value.(types.Tuple))

		if err != nil {
			return false, err
		}

		rows = append(rows, r)
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return rows, nil
}

// Creates a test database with the test data set in it
func CreateTestDatabase(dEnv *env.DoltEnv, t *testing.T) {
	dtestutils.CreateTestTable(t, dEnv, PeopleTableName, PeopleTestSchema, AllPeopleRows...)
	dtestutils.CreateTestTable(t, dEnv, EpisodesTableName, EpisodesTestSchema, AllEpsRows...)
	dtestutils.CreateTestTable(t, dEnv, AppearancesTableName, AppearancesTestSchema, AllAppsRows...)
}

// Creates a test database without any data in it
func CreateEmptyTestDatabase(dEnv *env.DoltEnv, t *testing.T) {
	dtestutils.CreateTestTable(t, dEnv, PeopleTableName, PeopleTestSchema)
	dtestutils.CreateTestTable(t, dEnv, EpisodesTableName, EpisodesTestSchema)
	dtestutils.CreateTestTable(t, dEnv, AppearancesTableName, AppearancesTestSchema)
}

var idColTag0TypeUUID = schema.NewColumn("id", 0, types.IntKind, true)
var firstColTag1TypeStr = schema.NewColumn("first_name", 1, types.StringKind, false)
var lastColTag2TypeStr = schema.NewColumn("last_name", 2, types.StringKind, false)
var addrColTag3TypeStr = schema.NewColumn("addr", 3, types.StringKind, false)
var ageColTag4TypeInt = schema.NewColumn("age", 4, types.IntKind, false)
var ageColTag5TypeUint = schema.NewColumn("age", 5, types.UintKind, false)

var DiffSchema = dtestutils.MustSchema(
	schema.NewColumn("to_id", 0, types.IntKind, false),
	schema.NewColumn("to_first_name", 1, types.StringKind, false),
	schema.NewColumn("to_last_name", 2, types.StringKind, false),
	schema.NewColumn("to_addr", 3, types.StringKind, false),
	schema.NewColumn("from_id", 7, types.IntKind, false),
	schema.NewColumn("from_first_name", 8, types.StringKind, false),
	schema.NewColumn("from_last_name", 9, types.StringKind, false),
	schema.NewColumn("from_addr", 10, types.StringKind, false),
	schema.NewColumn("diff_type", 14, types.StringKind, false),
)

const TableWithHistoryName = "test_table"

var InitialHistSch = dtestutils.MustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr)
var AddAddrAt3HistSch = dtestutils.MustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr, addrColTag3TypeStr)
var AddAgeAt4HistSch = dtestutils.MustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr, ageColTag4TypeInt)
var ReaddAgeAt5HistSch = dtestutils.MustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr, addrColTag3TypeStr, ageColTag5TypeUint)

// TableUpdate defines a list of modifications that should be made to a table
type TableUpdate struct {
	// NewSch is an updated schema for this table. It overwrites the existing value.  If not provided the existing value
	// will not change
	NewSch schema.Schema

	// NewRowData if provided overwrites the entirety of the row data in the table.
	NewRowData *types.Map

	// RowUpdates are new values for rows that should be set in the map.  They can be updates or inserts.
	RowUpdates []row.Row
}

// HistoryNode represents a commit to be made
type HistoryNode struct {
	// Branch the branch that the commit should be on
	Branch string

	// CommitMessag is the commit message that should be applied
	CommitMsg string

	// Updates are the changes that should be made to the table's states before committing
	Updates map[string]TableUpdate

	// Children are the child commits of this commit
	Children []HistoryNode
}

func CreateHistory(ctx context.Context, dEnv *env.DoltEnv, t *testing.T) []HistoryNode {
	vrw := dEnv.DoltDB.ValueReadWriter()

	return []HistoryNode{
		{
			Branch:    "seed",
			CommitMsg: "Seeding with initial user data",
			Updates: map[string]TableUpdate{
				TableWithHistoryName: {
					NewSch: InitialHistSch,
					NewRowData: dtestutils.MustRowData(t, ctx, vrw, InitialHistSch, []row.TaggedValues{
						{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son")},
						{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks")},
						{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn")},
					}),
				},
			},
			Children: []HistoryNode{
				{
					Branch:    "add-age",
					CommitMsg: "Adding int age to users with tag 3",
					Updates: map[string]TableUpdate{
						TableWithHistoryName: {
							NewSch: AddAgeAt4HistSch,
							NewRowData: dtestutils.MustRowData(t, ctx, vrw, AddAgeAt4HistSch, []row.TaggedValues{
								{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 4: types.Int(35)},
								{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 4: types.Int(38)},
								{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 4: types.Int(37)},
								{0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave"), 4: types.Int(37)},
							}),
						},
					},
					Children: nil,
				},
				{
					Branch:    env.DefaultInitBranch,
					CommitMsg: "Adding string address to users with tag 3",
					Updates: map[string]TableUpdate{
						TableWithHistoryName: {
							NewSch: AddAddrAt3HistSch,
							NewRowData: dtestutils.MustRowData(t, ctx, vrw, AddAddrAt3HistSch, []row.TaggedValues{
								{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 3: types.String("123 Fake St")},
								{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 3: types.String("456 Bull Ln")},
								{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 3: types.String("789 Not Real Ct")},
								{0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave")},
								{0: types.Int(4), 1: types.String("Matt"), 2: types.String("Jesuele")},
							}),
						},
					},
					Children: []HistoryNode{
						{
							Branch:    env.DefaultInitBranch,
							CommitMsg: "Re-add age as a uint with tag 4",
							Updates: map[string]TableUpdate{
								TableWithHistoryName: {
									NewSch: ReaddAgeAt5HistSch,
									NewRowData: dtestutils.MustRowData(t, ctx, vrw, ReaddAgeAt5HistSch, []row.TaggedValues{
										{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 3: types.String("123 Fake St"), 5: types.Uint(35)},
										{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 3: types.String("456 Bull Ln"), 5: types.Uint(38)},
										{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 3: types.String("789 Not Real Ct"), 5: types.Uint(37)},
										{0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave"), 3: types.String("-1 Imaginary Wy"), 5: types.Uint(37)},
										{0: types.Int(4), 1: types.String("Matt"), 2: types.String("Jesuele")},
										{0: types.Int(5), 1: types.String("Daylon"), 2: types.String("Wilkins")},
									}),
								},
							},
							Children: nil,
						},
					},
				},
			},
		},
	}
}

func UpdateTables(t *testing.T, ctx context.Context, root *doltdb.RootValue, tblUpdates map[string]TableUpdate) *doltdb.RootValue {
	for tblName, updates := range tblUpdates {
		tbl, ok, err := root.GetTable(ctx, tblName)
		require.NoError(t, err)

		var sch schema.Schema
		if updates.NewSch != nil {
			sch = updates.NewSch
		} else {
			sch, err = tbl.GetSchema(ctx)
			require.NoError(t, err)
		}

		var rowData types.Map
		if updates.NewRowData == nil {
			if ok {
				rowData, err = tbl.GetNomsRowData(ctx)
				require.NoError(t, err)
			} else {
				rowData, err = types.NewMap(ctx, root.VRW())
				require.NoError(t, err)
			}
		} else {
			rowData = *updates.NewRowData
		}

		if updates.RowUpdates != nil {
			me := rowData.Edit()

			for _, r := range updates.RowUpdates {
				me = me.Set(r.NomsMapKey(sch), r.NomsMapValue(sch))
			}

			rowData, err = me.Map(ctx)
			require.NoError(t, err)
		}

		var indexData durable.IndexSet
		require.NoError(t, err)
		if tbl != nil {
			indexData, err = tbl.GetIndexSet(ctx)
			require.NoError(t, err)
		}

		tbl, err = doltdb.NewNomsTable(ctx, root.VRW(), sch, rowData, indexData, nil)
		require.NoError(t, err)

		root, err = root.PutTable(ctx, tblName, tbl)
		require.NoError(t, err)
	}

	return root
}

// InitializeWithHistory will go through the provided historyNodes and create the intended commit graph
func InitializeWithHistory(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, historyNodes ...HistoryNode) {
	for _, node := range historyNodes {
		cs, err := doltdb.NewCommitSpec(env.DefaultInitBranch)
		require.NoError(t, err)

		cm, err := dEnv.DoltDB.Resolve(ctx, cs, nil)
		require.NoError(t, err)

		processNode(t, ctx, dEnv, node, cm)
	}
}

func processNode(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, node HistoryNode, parent *doltdb.Commit) {
	branchRef := ref.NewBranchRef(node.Branch)
	ok, err := dEnv.DoltDB.HasRef(ctx, branchRef)
	require.NoError(t, err)

	if !ok {
		err = dEnv.DoltDB.NewBranchAtCommit(ctx, branchRef, parent)
		require.NoError(t, err)
	}

	cs, err := doltdb.NewCommitSpec(branchRef.String())
	require.NoError(t, err)

	cm, err := dEnv.DoltDB.Resolve(ctx, cs, nil)
	require.NoError(t, err)

	root, err := cm.GetRootValue()
	require.NoError(t, err)

	root = UpdateTables(t, ctx, root, node.Updates)
	h, err := dEnv.DoltDB.WriteRootValue(ctx, root)
	require.NoError(t, err)

	meta, err := doltdb.NewCommitMeta("Ash Ketchum", "ash@poke.mon", node.CommitMsg)
	require.NoError(t, err)

	cm, err = dEnv.DoltDB.Commit(ctx, h, branchRef, meta)
	require.NoError(t, err)

	for _, child := range node.Children {
		processNode(t, ctx, dEnv, child, cm)
	}
}
