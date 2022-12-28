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

package sqle

import (
	"context"
	"math/rand"
	"sort"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

func TestSqlBatchInserts(t *testing.T) {
	insertStatements := []string{
		`insert into people (id, first_name, last_name, is_married, age, rating, uuid, num_episodes) values
					(7, "Maggie", "Simpson", false, 1, 5.1, '00000000-0000-0000-0000-000000000007', 677)`,
		`insert into people values
					(8, "Milhouse", "VanHouten", false, 1, 5.1, '00000000-0000-0000-0000-000000000008', 677)`,
		`insert into people (id, first_name, last_name) values (9, "Clancey", "Wiggum")`,
		`insert into people (id, first_name, last_name) values
					(10, "Montgomery", "Burns"), (11, "Ned", "Flanders")`,
		`insert into episodes (id, name) values (5, "Bart the General"), (6, "Moaning Lisa")`,
		`insert into episodes (id, name) values (7, "The Call of the Simpsons"), (8, "The Telltale Head")`,
		`insert into episodes (id, name) values (9, "Life on the Fast Lane")`,
		`insert into appearances (character_id, episode_id) values (7,5), (7,6)`,
		`insert into appearances (character_id, episode_id) values (8,7)`,
		`insert into appearances (character_id, episode_id) values (9,8), (9,9)`,
		`insert into appearances (character_id, episode_id) values (10,5), (10,6)`,
		`insert into appearances (character_id, episode_id) values (11,9)`,
	}

	// Shuffle the inserts so that different tables are interleaved. We're not giving a seed here, so this is
	// deterministic.
	rand.Shuffle(len(insertStatements),
		func(i, j int) {
			insertStatements[i], insertStatements[j] = insertStatements[j], insertStatements[i]
		})

	ctx := context.Background()
	dEnv, err := CreateTestDatabase()
	require.NoError(t, err)

	root, err := dEnv.WorkingRoot(ctx)

	tmpDir, err := dEnv.TempTableFilesDir()
	require.NoError(t, err)
	opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: tmpDir}
	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(), opts)
	require.NoError(t, err)

	engine, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
	require.NoError(t, err)
	dsess.DSessFromSess(sqlCtx.Session).EnableBatchedMode()

	for _, stmt := range insertStatements {
		_, rowIter, err := engine.Query(sqlCtx, stmt)
		require.NoError(t, err)
		require.NoError(t, drainIter(sqlCtx, rowIter))
	}

	// Before committing the batch, the database should be unchanged from its original state
	allPeopleRows, err := GetAllRows(root, PeopleTableName)
	require.NoError(t, err)
	allEpsRows, err := GetAllRows(root, EpisodesTableName)
	require.NoError(t, err)
	allAppearanceRows, err := GetAllRows(root, AppearancesTableName)
	require.NoError(t, err)

	AllPeopleSqlRows := ToSqlRows(PeopleTestSchema, AllPeopleRows...)
	AllEpsSqlRows := ToSqlRows(EpisodesTestSchema, AllEpsRows...)
	AllAppsSqlRows := ToSqlRows(AppearancesTestSchema, AllAppsRows...)
	assert.ElementsMatch(t, AllPeopleSqlRows, allPeopleRows)
	assert.ElementsMatch(t, AllEpsSqlRows, allEpsRows)
	assert.ElementsMatch(t, AllAppsSqlRows, allAppearanceRows)

	// Now commit the batch and check for new rows
	err = db.Flush(sqlCtx)
	require.NoError(t, err)

	var expectedPeople, expectedEpisodes, expectedAppearances []sql.Row

	expectedPeople = append(expectedPeople, AllPeopleSqlRows...)
	expectedPeople = append(expectedPeople, ToSqlRows(PeopleTestSchema,
		NewPeopleRowWithOptionalFields(7, "Maggie", "Simpson", false, 1, 5.1, uuid.MustParse("00000000-0000-0000-0000-000000000007"), 677),
		NewPeopleRowWithOptionalFields(8, "Milhouse", "VanHouten", false, 1, 5.1, uuid.MustParse("00000000-0000-0000-0000-000000000008"), 677),
		newPeopleRow(9, "Clancey", "Wiggum"),
		newPeopleRow(10, "Montgomery", "Burns"),
		newPeopleRow(11, "Ned", "Flanders"),
	)...)

	expectedEpisodes = append(expectedEpisodes, AllEpsSqlRows...)
	expectedEpisodes = append(expectedEpisodes, ToSqlRows(EpisodesTestSchema,
		newEpsRow(5, "Bart the General"),
		newEpsRow(6, "Moaning Lisa"),
		newEpsRow(7, "The Call of the Simpsons"),
		newEpsRow(8, "The Telltale Head"),
		newEpsRow(9, "Life on the Fast Lane"),
	)...)

	expectedAppearances = append(expectedAppearances, AllAppsSqlRows...)
	expectedAppearances = append(expectedAppearances, ToSqlRows(AppearancesTestSchema,
		newAppsRow(7, 5),
		newAppsRow(7, 6),
		newAppsRow(8, 7),
		newAppsRow(9, 8),
		newAppsRow(9, 9),
		newAppsRow(10, 5),
		newAppsRow(10, 6),
		newAppsRow(11, 9),
	)...)

	root, err = db.GetRoot(sqlCtx)
	require.NoError(t, err)
	allPeopleRows, err = GetAllRows(root, PeopleTableName)
	require.NoError(t, err)
	allEpsRows, err = GetAllRows(root, EpisodesTableName)
	require.NoError(t, err)
	allAppearanceRows, err = GetAllRows(root, AppearancesTableName)
	require.NoError(t, err)

	assertRowSetsEqual(t, PeopleTestSchema, expectedPeople, allPeopleRows)
	assertRowSetsEqual(t, EpisodesTestSchema, expectedEpisodes, allEpsRows)
	assertRowSetsEqual(t, AppearancesTestSchema, expectedAppearances, allAppearanceRows)
}

func TestSqlBatchInsertIgnoreReplace(t *testing.T) {
	t.Skip("Skipped until insert ignore statements supported in go-mysql-server")

	insertStatements := []string{
		`replace into people (id, first_name, last_name, is_married, age, rating, uuid, num_episodes) values
					(0, "Maggie", "Simpson", false, 1, 5.1, '00000000-0000-0000-0000-000000000007', 677)`,
		`insert ignore into people values
					(2, "Milhouse", "VanHouten", false, 1, 5.1, '00000000-0000-0000-0000-000000000008', 677)`,
	}

	ctx := context.Background()
	dEnv, err := CreateTestDatabase()
	require.NoError(t, err)

	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	tmpDir, err := dEnv.TempTableFilesDir()
	require.NoError(t, err)
	opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: tmpDir}
	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(), opts)
	require.NoError(t, err)

	engine, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
	require.NoError(t, err)
	dsess.DSessFromSess(sqlCtx.Session).EnableBatchedMode()

	for _, stmt := range insertStatements {
		_, rowIter, err := engine.Query(sqlCtx, stmt)
		require.NoError(t, err)
		drainIter(sqlCtx, rowIter)
	}

	// Before committing the batch, the database should be unchanged from its original state
	allPeopleRows, err := GetAllRows(root, PeopleTableName)
	assert.NoError(t, err)
	assert.ElementsMatch(t, AllPeopleRows, allPeopleRows)

	// Now commit the batch and check for new rows
	err = db.Flush(sqlCtx)
	require.NoError(t, err)

	var expectedPeople []row.Row

	expectedPeople = append(expectedPeople, AllPeopleRows[1:]...) // skip homer
	expectedPeople = append(expectedPeople,
		NewPeopleRowWithOptionalFields(0, "Maggie", "Simpson", false, 1, 5.1, uuid.MustParse("00000000-0000-0000-0000-000000000007"), 677),
	)

	allPeopleRows, err = GetAllRows(root, PeopleTableName)
	assert.NoError(t, err)
	assertRowSetsEqual(t, PeopleTestSchema, ToSqlRows(PeopleTestSchema, expectedPeople...), allPeopleRows)
}

func TestSqlBatchInsertErrors(t *testing.T) {
	ctx := context.Background()
	dEnv, err := CreateTestDatabase()
	require.NoError(t, err)

	tmpDir, err := dEnv.TempTableFilesDir()
	require.NoError(t, err)
	opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: tmpDir}
	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(), opts)
	require.NoError(t, err)

	engine, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
	require.NoError(t, err)
	dsess.DSessFromSess(sqlCtx.Session).EnableBatchedMode()

	_, rowIter, err := engine.Query(sqlCtx, `insert into people (id, first_name, last_name, is_married, age, rating, uuid, num_episodes) values
					(0, "Maggie", "Simpson", false, 1, 5.1, '00000000-0000-0000-0000-000000000007', 677)`)
	assert.NoError(t, err)
	assert.Error(t, drainIter(sqlCtx, rowIter))

	// This generates an error at insert time because of the bad type for the uuid column
	_, rowIter, err = engine.Query(sqlCtx, `insert into people values
					(2, "Milhouse", "VanHouten", false, 1, 5.1, true, 677)`)
	assert.NoError(t, err)
	assert.Error(t, drainIter(sqlCtx, rowIter))

	assert.NoError(t, db.Flush(sqlCtx))
}

func assertRowSetsEqual(t *testing.T, sch schema.Schema, expected, actual []sql.Row) {
	require.Equal(t, len(expected), len(actual),
		"Sets have different sizes: expected %d, was %d", len(expected), len(actual))
	sqlSch, err := sqlutil.FromDoltSchema("", sch)
	require.NoError(t, err)
	sortSqlRows(t, sqlSch.Schema, expected)
	sortSqlRows(t, sqlSch.Schema, actual)
	if !assert.Equal(t, expected, actual) {
		t.Skip("")
	}
}

func sortSqlRows(t *testing.T, sch sql.Schema, rows []sql.Row) {
	sort.Slice(rows, func(i, j int) bool {
		l, r := rows[i], rows[j]
		for idx, col := range sch {
			c, err := col.Type.Compare(l[idx], r[idx])
			require.NoError(t, err)
			if c == 0 {
				continue
			}
			return c < 0
		}
		return false
	})
}

func newPeopleRow(id int, firstName, lastName string) row.Row {
	vals := row.TaggedValues{
		IdTag:        types.Int(id),
		FirstNameTag: types.String(firstName),
		LastNameTag:  types.String(lastName),
	}

	r, err := row.New(types.Format_Default, PeopleTestSchema, vals)

	if err != nil {
		panic(err)
	}

	return r
}

func newEpsRow(id int, name string) row.Row {
	vals := row.TaggedValues{
		EpisodeIdTag: types.Int(id),
		EpNameTag:    types.String(name),
	}

	r, err := row.New(types.Format_Default, EpisodesTestSchema, vals)

	if err != nil {
		panic(err)
	}

	return r
}

func newAppsRow(charId int, epId int) row.Row {
	vals := row.TaggedValues{
		AppCharacterTag: types.Int(charId),
		AppEpTag:        types.Int(epId),
	}

	r, err := row.New(types.Format_Default, AppearancesTestSchema, vals)

	if err != nil {
		panic(err)
	}

	return r
}
