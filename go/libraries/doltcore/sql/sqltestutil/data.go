// Copyright 2019 Liquidata, Inc.
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

package sqltestutil

import (
	"context"
	"reflect"
	"testing"

	"github.com/google/uuid"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/envtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// This file collects useful test table definitions and functions for SQL tests to use. It primarily defines a table
// name, schema, and some sample rows to use in tests, as well as functions for creating and seeding a test database,
// transforming row results, and so on.

const (
	IdTag = iota
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
	EpisodeIdTag = iota
	EpNameTag
	EpAirDateTag
	EpRatingTag
)

const (
	AppCharacterTag = iota
	AppEpTag
	AppCommentsTag
)

const (
	homerId = iota
	margeId
	bartId
	lisaId
	moeId
	barneyId
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
	colColl, _ := schema.NewColCollection(
		schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("is_married", IsMarriedTag, types.BoolKind, false),
		schema.NewColumn("age", AgeTag, types.IntKind, false),
		//		schema.NewColumn("empty", emptyTag, types.IntKind, false),
		schema.NewColumn("rating", RatingTag, types.FloatKind, false),
		schema.NewColumn("uuid", UuidTag, types.UUIDKind, false),
		schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
	)
	return schema.SchemaFromCols(colColl)
}

func createEpisodesTestSchema() schema.Schema {
	colColl, _ := schema.NewColCollection(
		schema.NewColumn("id", EpisodeIdTag, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("name", EpNameTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("air_date", EpAirDateTag, types.IntKind, false),
		schema.NewColumn("rating", EpRatingTag, types.FloatKind, false),
	)
	return schema.SchemaFromCols(colColl)
}

func createAppearancesTestSchema() schema.Schema {
	colColl, _ := schema.NewColCollection(
		schema.NewColumn("character_id", AppCharacterTag, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("episode_id", AppEpTag, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("comments", AppCommentsTag, types.StringKind, false),
	)
	return schema.SchemaFromCols(colColl)
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

	r, err := row.New(types.Format_7_18, PeopleTestSchema, vals)

	if err != nil {
		panic(err)
	}

	return r
}

func newEpsRow(id int, name string, airdate int, rating float64) row.Row {
	vals := row.TaggedValues{
		EpisodeIdTag: types.Int(id),
		EpNameTag:    types.String(name),
		EpAirDateTag: types.Int(airdate),
		EpRatingTag:  types.Float(rating),
	}

	r, err := row.New(types.Format_7_18, EpisodesTestSchema, vals)

	if err != nil {
		panic(err)
	}

	return r
}

func newAppsRow(charId, epId int, comment string) row.Row {
	vals := row.TaggedValues{
		AppCharacterTag: types.Int(charId),
		AppEpTag:        types.Int(epId),
		AppCommentsTag:  types.String(comment),
	}

	r, err := row.New(types.Format_7_18, AppearancesTestSchema, vals)

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

	r, err := row.New(types.Format_7_18, PeopleTestSchema, vals)

	if err != nil {
		panic(err)
	}

	return r
}

// 6 characters
var Homer = NewPeopleRow(homerId, "Homer", "Simpson", true, 40, 8.5)
var Marge = NewPeopleRowWithOptionalFields(margeId, "Marge", "Simpson", true, 38, 8, uuid.MustParse("00000000-0000-0000-0000-000000000001"), 111)
var Bart = NewPeopleRowWithOptionalFields(bartId, "Bart", "Simpson", false, 10, 9, uuid.MustParse("00000000-0000-0000-0000-000000000002"), 222)
var Lisa = NewPeopleRowWithOptionalFields(lisaId, "Lisa", "Simpson", false, 8, 10, uuid.MustParse("00000000-0000-0000-0000-000000000003"), 333)
var Moe = NewPeopleRowWithOptionalFields(moeId, "Moe", "Szyslak", false, 48, 6.5, uuid.MustParse("00000000-0000-0000-0000-000000000004"), 444)
var Barney = NewPeopleRowWithOptionalFields(barneyId, "Barney", "Gumble", false, 40, 4, uuid.MustParse("00000000-0000-0000-0000-000000000005"), 555)
var AllPeopleRows = Rs(Homer, Marge, Bart, Lisa, Moe, Barney)

// Actually the first 4 episodes of the show
var Ep1 = newEpsRow(1, "Simpsons Roasting On an Open Fire", 629953200, 8.0)
var Ep2 = newEpsRow(2, "Bart the Genius", 632372400, 9.0)
var Ep3 = newEpsRow(3, "Homer's Odyssey", 632977200, 7.0)
var Ep4 = newEpsRow(4, "There's No Disgrace Like Home", 633582000, 8.5)
var AllEpsRows = Rs(Ep1, Ep2, Ep3, Ep4)

// These are made up, not the actual show data
var app1 = newAppsRow(homerId, 1, "Homer is great in this one")
var app2 = newAppsRow(margeId, 1, "Marge is here too")
var app3 = newAppsRow(homerId, 2, "Homer is great in this one too")
var app4 = newAppsRow(bartId, 2, "This episode is named after Bart")
var app5 = newAppsRow(lisaId, 2, "Lisa is here too")
var app6 = newAppsRow(moeId, 2, "I think there's a prank call scene")
var app7 = newAppsRow(homerId, 3, "Homer is in every episode")
var app8 = newAppsRow(margeId, 3, "Marge shows up a lot too")
var app9 = newAppsRow(lisaId, 3, "Lisa is the best Simpson")
var app10 = newAppsRow(barneyId, 3, "I'm making this all up")

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
func MutateRow(r row.Row, tagsAndVals ...interface{}) row.Row {
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
			default:
				panic("Unhandled type " + reflect.TypeOf(val).String())
			}
		} else {
			nomsVal = nil
		}

		mutated, err = mutated.SetColVal(uint64(tag), nomsVal, PeopleTestSchema)
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

	rowData, err := table.GetRowData(ctx)

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
var ageColTag3TypeInt = schema.NewColumn("age", 3, types.IntKind, false)
var ageColTag4TypeUint = schema.NewColumn("age", 4, types.UintKind, false)

var diffSchema = envtestutils.MustSchema(
	schema.NewColumn("to_id", 0, types.IntKind, false),
	schema.NewColumn("to_first_name", 1, types.StringKind, false),
	schema.NewColumn("to_last", 2, types.StringKind, false),
	schema.NewColumn("to_age_Int_3", 3, types.IntKind, false),
	schema.NewColumn("to_age_Uint_4", 4, types.UintKind, false),
	schema.NewColumn("to_addr", 5, types.StringKind, false),
	schema.NewColumn("to_commit", 6, types.StringKind, false),
	schema.NewColumn("from_id", 7, types.IntKind, false),
	schema.NewColumn("from_first_name", 8, types.StringKind, false),
	schema.NewColumn("from_last", 9, types.StringKind, false),
	schema.NewColumn("from_age_Int_3", 10, types.IntKind, false),
	schema.NewColumn("from_age_Uint_4", 11, types.UintKind, false),
	schema.NewColumn("from_addr", 12, types.StringKind, false),
	schema.NewColumn("from_commit", 13, types.StringKind, false),
)

const tblName = "test_table"

var initialSch = envtestutils.MustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr)
var addAddrAt3Sch = envtestutils.MustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr, addrColTag3TypeStr)
var addAgeAt3Sch = envtestutils.MustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr, ageColTag3TypeInt)
var readdAgeAt4Sch = envtestutils.MustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr, addrColTag3TypeStr, ageColTag4TypeUint)

func CreateHistory(ctx context.Context, dEnv *env.DoltEnv, t *testing.T) []envtestutils.HistoryNode {
	vrw := dEnv.DoltDB.ValueReadWriter()

	return []envtestutils.HistoryNode{
		{
			Branch:    "seed",
			CommitMsg: "Seeding with initial user data",
			Updates: map[string]envtestutils.TableUpdate{
				tblName: {
					NewSch: initialSch,
					NewRowData: envtestutils.MustRowData(t, ctx, vrw, initialSch, []row.TaggedValues{
						{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son")},
						{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks")},
						{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn")},
					}),
				},
			},
			Children: []envtestutils.HistoryNode{
				{
					Branch:    "add-age",
					CommitMsg: "Adding int age to users with tag 3",
					Updates: map[string]envtestutils.TableUpdate{
						tblName: {
							NewSch: addAgeAt3Sch,
							NewRowData: envtestutils.MustRowData(t, ctx, vrw, addAgeAt3Sch, []row.TaggedValues{
								{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 3: types.Int(35)},
								{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 3: types.Int(38)},
								{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 3: types.Int(37)},
								{0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave"), 3: types.Int(37)},
							}),
						},
					},
					Children: nil,
				},
				{
					Branch:    "master",
					CommitMsg: "Adding string address to users with tag 3",
					Updates: map[string]envtestutils.TableUpdate{
						tblName: {
							NewSch: addAddrAt3Sch,
							NewRowData: envtestutils.MustRowData(t, ctx, vrw, addAddrAt3Sch, []row.TaggedValues{
								{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 3: types.String("123 Fake St")},
								{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 3: types.String("456 Bull Ln")},
								{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 3: types.String("789 Not Real Ct")},
								{0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave")},
								{0: types.Int(4), 1: types.String("Matt"), 2: types.String("Jesuele")},
							}),
						},
					},
					Children: []envtestutils.HistoryNode{
						{
							Branch:    "master",
							CommitMsg: "Re-add age as a uint with tag 4",
							Updates: map[string]envtestutils.TableUpdate{
								tblName: {
									NewSch: readdAgeAt4Sch,
									NewRowData: envtestutils.MustRowData(t, ctx, vrw, readdAgeAt4Sch, []row.TaggedValues{
										{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 3: types.String("123 Fake St"), 4: types.Uint(35)},
										{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 3: types.String("456 Bull Ln"), 4: types.Uint(38)},
										{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 3: types.String("789 Not Real Ct"), 4: types.Uint(37)},
										{0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave"), 3: types.String("-1 Imaginary Wy"), 4: types.Uint(37)},
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

func CreateWorkingRootUpdate() map[string]envtestutils.TableUpdate {
	return map[string]envtestutils.TableUpdate{
		tblName: {
			RowUpdates: []row.Row{
				mustRow(row.New(types.Format_Default, readdAgeAt4Sch, row.TaggedValues{
					0: types.Int(6), 1: types.String("Katie"), 2: types.String("McCulloch"),
				})),
			},
		},
	}
}
