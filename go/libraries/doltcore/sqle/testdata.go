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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/google/uuid"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
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
var PeopleTableName = "people"

var EpisodesTestSchema = createEpisodesTestSchema()
var EpisodesTableName = "episodes"

var AppearancesTestSchema = createAppearancesTestSchema()
var AppearancesTableName = "appearances"

func createPeopleTestSchema() schema.Schema {
	colColl := schema.NewColCollection(
		schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("is_married", IsMarriedTag, types.IntKind, false),
		schema.NewColumn("age", AgeTag, types.IntKind, false),
		schema.NewColumn("rating", RatingTag, types.FloatKind, false),
		schema.NewColumn("uuid", UuidTag, types.StringKind, false),
		schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
	)
	sch := schema.MustSchemaFromCols(colColl)
	return sch
}

func createEpisodesTestSchema() schema.Schema {
	colColl := schema.NewColCollection(
		schema.NewColumn("id", EpisodeIdTag, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("name", EpNameTag, types.StringKind, false, schema.NotNullConstraint{}),
		newColumnWithTypeInfo("air_date", EpAirDateTag, typeinfo.DatetimeType, false),
		schema.NewColumn("rating", EpRatingTag, types.FloatKind, false),
	)
	sch := schema.MustSchemaFromCols(colColl)
	return sch
}

func createAppearancesTestSchema() schema.Schema {
	colColl := schema.NewColCollection(
		schema.NewColumn("character_id", AppCharacterTag, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("episode_id", AppEpTag, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("comments", AppCommentsTag, types.StringKind, false),
	)
	sch := schema.MustSchemaFromCols(colColl)
	return sch
}

func newColumnWithTypeInfo(name string, tag uint64, info typeinfo.TypeInfo, partOfPk bool, constraints ...schema.ColConstraint) schema.Column {
	col, err := schema.NewColumnWithTypeInfo(name, tag, info, partOfPk, "", false, "", constraints...)
	if err != nil {
		panic(fmt.Sprintf("unexpected error creating column: %s", err.Error()))
	}
	return col
}

func NewPeopleRow(id int, first, last string, isMarried bool, age int, rating float64) row.Row {
	isMarriedVal := types.Int(0)
	if isMarried {
		isMarriedVal = types.Int(1)
	}

	vals := row.TaggedValues{
		IdTag:        types.Int(id),
		FirstNameTag: types.String(first),
		LastNameTag:  types.String(last),
		IsMarriedTag: isMarriedVal,
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
	isMarriedVal := types.Int(0)
	if isMarried {
		isMarriedVal = types.Int(1)
	}

	vals := row.TaggedValues{
		IdTag:          types.Int(id),
		FirstNameTag:   types.String(first),
		LastNameTag:    types.String(last),
		IsMarriedTag:   isMarriedVal,
		AgeTag:         types.Int(age),
		RatingTag:      types.Float(rating),
		UuidTag:        types.String(uid.String()),
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
				nomsVal = types.String(v.String())
			case bool:
				nomsVal = types.Int(0)
				if v {
					nomsVal = types.Int(1)
				}
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

func GetAllRows(root doltdb.RootValue, tableName string) ([]sql.UntypedSqlRow, error) {
	ctx := context.Background()
	table, _, err := root.GetTable(ctx, doltdb.TableName{Name: tableName})
	if err != nil {
		return nil, err
	}

	rowIdx, err := table.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	return SqlRowsFromDurableIndex(rowIdx, sch)
}
