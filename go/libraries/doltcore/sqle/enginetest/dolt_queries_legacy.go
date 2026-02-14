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

package enginetest

// This file contains tests converted from the legacy sqle package test format
// (sqlselect_test.go, sqlinsert_test.go, sqlupdate_test.go, sqldelete_test.go,
// sqlreplace_test.go) into the modern enginetest ScriptTest format.
//
// Some assertions from the legacy tests are not expressible in the ScriptTest
// framework (e.g., schema assertions, tests marked SkipOnSqlEngine, system
// table tests that depend on commit hashes). These have been omitted.

import (
	"time"

	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

// concatSQLRows concatenates multiple sql.Row values into a single sql.Row.
// Used to construct expected rows for cross-product and join queries.
func concatSQLRows(rows ...sql.Row) sql.Row {
	var result sql.Row
	for _, r := range rows {
		result = append(result, r...)
	}
	return result
}

// People rows as returned by SELECT * FROM people (full Simpsons data set)
var (
	lHomer  = sql.Row{int64(0), "Homer", "Simpson", int64(1), int64(40), 8.5, nil, nil}
	lMarge  = sql.Row{int64(1), "Marge", "Simpson", int64(1), int64(38), 8.0, "00000000-0000-0000-0000-000000000001", uint64(111)}
	lBart   = sql.Row{int64(2), "Bart", "Simpson", int64(0), int64(10), 9.0, "00000000-0000-0000-0000-000000000002", uint64(222)}
	lLisa   = sql.Row{int64(3), "Lisa", "Simpson", int64(0), int64(8), 10.0, "00000000-0000-0000-0000-000000000003", uint64(333)}
	lMoe    = sql.Row{int64(4), "Moe", "Szyslak", int64(0), int64(48), 6.5, "00000000-0000-0000-0000-000000000004", uint64(444)}
	lBarney = sql.Row{int64(5), "Barney", "Gumble", int64(0), int64(40), 4.0, "00000000-0000-0000-0000-000000000005", uint64(555)}
)

// Episode rows as returned by SELECT * FROM episodes
var (
	lEp1 = sql.Row{int64(1), "Simpsons Roasting On an Open Fire", time.Date(1989, time.December, 18, 3, 0, 0, 0, time.UTC), 8.0}
	lEp2 = sql.Row{int64(2), "Bart the Genius", time.Date(1990, time.January, 15, 3, 0, 0, 0, time.UTC), 9.0}
	lEp3 = sql.Row{int64(3), "Homer's Odyssey", time.Date(1990, time.January, 22, 3, 0, 0, 0, time.UTC), 7.0}
	lEp4 = sql.Row{int64(4), "There's No Disgrace Like Home", time.Date(1990, time.January, 29, 3, 0, 0, 0, time.UTC), 8.5}
)

// Setup script that creates and populates the Simpsons test tables.
// Used by select, update, and delete tests.
var legacySetupWithData = []string{
	`CREATE TABLE people (
		id bigint primary key,
		first_name varchar(1024) NOT NULL,
		last_name varchar(1024) NOT NULL,
		is_married bigint,
		age bigint,
		rating double,
		uuid varchar(1024),
		num_episodes bigint unsigned
	)`,
	`CREATE TABLE episodes (
		id bigint primary key,
		name varchar(1024) NOT NULL,
		air_date datetime,
		rating double
	)`,
	`CREATE TABLE appearances (
		character_id bigint NOT NULL,
		episode_id bigint NOT NULL,
		comments varchar(1024),
		PRIMARY KEY (character_id, episode_id)
	)`,
	`INSERT INTO people VALUES
		(0, 'Homer', 'Simpson', 1, 40, 8.5, NULL, NULL),
		(1, 'Marge', 'Simpson', 1, 38, 8.0, '00000000-0000-0000-0000-000000000001', 111),
		(2, 'Bart', 'Simpson', 0, 10, 9.0, '00000000-0000-0000-0000-000000000002', 222),
		(3, 'Lisa', 'Simpson', 0, 8, 10.0, '00000000-0000-0000-0000-000000000003', 333),
		(4, 'Moe', 'Szyslak', 0, 48, 6.5, '00000000-0000-0000-0000-000000000004', 444),
		(5, 'Barney', 'Gumble', 0, 40, 4.0, '00000000-0000-0000-0000-000000000005', 555)`,
	`INSERT INTO episodes VALUES
		(1, 'Simpsons Roasting On an Open Fire', '1989-12-18 03:00:00', 8.0),
		(2, 'Bart the Genius', '1990-01-15 03:00:00', 9.0),
		(3, 'Homer''s Odyssey', '1990-01-22 03:00:00', 7.0),
		(4, 'There''s No Disgrace Like Home', '1990-01-29 03:00:00', 8.5)`,
	`INSERT INTO appearances VALUES
		(0, 1, 'Homer is great in this one'),
		(1, 1, 'Marge is here too'),
		(0, 2, 'Homer is great in this one too'),
		(2, 2, 'This episode is named after Bart'),
		(3, 2, 'Lisa is here too'),
		(4, 2, 'I think there''s a prank call scene'),
		(0, 3, 'Homer is in every episode'),
		(1, 3, 'Marge shows up a lot too'),
		(3, 3, 'Lisa is the best Simpson'),
		(5, 3, 'I''m making this all up')`,
}

// Setup script that creates empty Simpsons test tables (no data).
// Used by insert and replace tests.
var legacyEmptySetup = []string{
	`CREATE TABLE people (
		id bigint primary key,
		first_name varchar(1024) NOT NULL,
		last_name varchar(1024) NOT NULL,
		is_married bigint,
		age bigint,
		rating double,
		uuid varchar(1024),
		num_episodes bigint unsigned
	)`,
	`CREATE TABLE episodes (
		id bigint primary key,
		name varchar(1024) NOT NULL,
		air_date datetime,
		rating double
	)`,
	`CREATE TABLE appearances (
		character_id bigint NOT NULL,
		episode_id bigint NOT NULL,
		comments varchar(1024),
		PRIMARY KEY (character_id, episode_id)
	)`,
}

// LegacySelectScriptTests are basic SELECT tests converted from sqle/sqlselect_test.go BasicSelectTests.
var LegacySelectScriptTests = []queries.ScriptTest{
	{
		Name:        "legacy basic select tests",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from people where id = 2",
				Expected: []sql.Row{lBart},
			},
			{
				Query:    "select * from people",
				Expected: []sql.Row{lHomer, lMarge, lBart, lLisa, lMoe, lBarney},
			},
			{
				Query:    "select * from people limit 1",
				Expected: []sql.Row{lHomer},
			},
			{
				Query:    "select * from people limit 0,1",
				Expected: []sql.Row{lHomer},
			},
			{
				Query:    "select * from people limit 1 offset 1",
				Expected: []sql.Row{lMarge},
			},
			{
				Query:    "select * from people limit 5,1",
				Expected: []sql.Row{lBarney},
			},
			{
				Query:    "select * from people limit 6,1",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from people limit 0",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from people limit 0,0",
				Expected: []sql.Row{},
			},
			{
				Query:          "select * from people limit -1",
				ExpectedErrStr: "syntax error at position 29 near 'limit'",
			},
			{
				Query:          "select * from people limit -1,1",
				ExpectedErrStr: "syntax error at position 29 near 'limit'",
			},
			{
				Query:    "select * from people limit 100",
				Expected: []sql.Row{lHomer, lMarge, lBart, lLisa, lMoe, lBarney},
			},
			{
				Query:    "select * from people where age < 40",
				Expected: []sql.Row{lMarge, lBart, lLisa},
			},
			{
				Query:    "select * from people where age < 40 limit 1",
				Expected: []sql.Row{lMarge},
			},
			{
				Query:    "select * from people where age < 40 limit 2",
				Expected: []sql.Row{lMarge, lBart},
			},
			{
				Query:    "select * from people where age < 40 limit 100",
				Expected: []sql.Row{lMarge, lBart, lLisa},
			},
			{
				Query:    "select * from people order by id",
				Expected: []sql.Row{lHomer, lMarge, lBart, lLisa, lMoe, lBarney},
			},
			{
				Query:    "select * from people order by id desc",
				Expected: []sql.Row{lBarney, lMoe, lLisa, lBart, lMarge, lHomer},
			},
			{
				Query:    "select * from people order by rating",
				Expected: []sql.Row{lBarney, lMoe, lMarge, lHomer, lBart, lLisa},
			},
			{
				Query:    "select * from people order by first_name",
				Expected: []sql.Row{lBarney, lBart, lHomer, lLisa, lMarge, lMoe},
			},
			{
				Query:    "select * from people order by last_name desc, first_name asc",
				Expected: []sql.Row{lMoe, lBart, lHomer, lLisa, lMarge, lBarney},
			},
			{
				Query:    "select * from people order by first_name limit 2",
				Expected: []sql.Row{lBarney, lBart},
			},
			{
				Query:    "select * from people order by last_name desc, first_name asc limit 2",
				Expected: []sql.Row{lMoe, lBart},
			},
			{
				Query:    "select * from people where 40 > age",
				Expected: []sql.Row{lMarge, lBart, lLisa},
			},
			{
				Query:    "select * from people where age <= 40",
				Expected: []sql.Row{lHomer, lMarge, lBart, lLisa, lBarney},
			},
			{
				Query:    "select * from people where 40 >= age",
				Expected: []sql.Row{lHomer, lMarge, lBart, lLisa, lBarney},
			},
			{
				Query:    "select * from people where age > 40",
				Expected: []sql.Row{lMoe},
			},
			{
				Query:    "select * from people where 40 < age",
				Expected: []sql.Row{lMoe},
			},
			{
				Query:    "select * from people where age >= 40",
				Expected: []sql.Row{lHomer, lMoe, lBarney},
			},
			{
				Query:    "select * from people where 40 <= age",
				Expected: []sql.Row{lHomer, lMoe, lBarney},
			},
			{
				Query:    "select * from people where last_name > 'Simpson'",
				Expected: []sql.Row{lMoe},
			},
			{
				Query:    "select * from people where last_name < 'Simpson'",
				Expected: []sql.Row{lBarney},
			},
			{
				Query:    "select * from people where last_name = 'Simpson'",
				Expected: []sql.Row{lHomer, lMarge, lBart, lLisa},
			},
			{
				Query:    "select * from people where rating > 8.0 order by id",
				Expected: []sql.Row{lHomer, lBart, lLisa},
			},
			{
				Query:    "select * from people where rating < 8.0",
				Expected: []sql.Row{lMoe, lBarney},
			},
			{
				Query:    "select * from people where rating = 8.0",
				Expected: []sql.Row{lMarge},
			},
			{
				Query:    "select * from people where 8.0 < rating",
				Expected: []sql.Row{lHomer, lBart, lLisa},
			},
			{
				Query:    "select * from people where 8.0 > rating",
				Expected: []sql.Row{lMoe, lBarney},
			},
			{
				Query:    "select * from people where 8.0 = rating",
				Expected: []sql.Row{lMarge},
			},
			{
				Query:    "select * from people where is_married = true",
				Expected: []sql.Row{lHomer, lMarge},
			},
			{
				Query:    "select * from people where is_married = false",
				Expected: []sql.Row{lBart, lLisa, lMoe, lBarney},
			},
			{
				Query:    "select * from people where is_married <> false",
				Expected: []sql.Row{lHomer, lMarge},
			},
			{
				Query:    "select * from people where is_married",
				Expected: []sql.Row{lHomer, lMarge},
			},
			{
				Query:    "select * from people where is_married and age > 38",
				Expected: []sql.Row{lHomer},
			},
			{
				Query:    "select * from people where is_married or age < 20",
				Expected: []sql.Row{lHomer, lMarge, lBart, lLisa},
			},
			{
				Query:    "select * from people where first_name in ('Homer', 'Marge')",
				Expected: []sql.Row{lHomer, lMarge},
			},
			{
				Query:    "select * from people where age in (-10, 40)",
				Expected: []sql.Row{lHomer, lBarney},
			},
			{
				Query:    "select * from people where rating in (-10.0, 8.5)",
				Expected: []sql.Row{lHomer},
			},
			{
				Query:    "select * from people where first_name in ('Homer', 40)",
				Expected: []sql.Row{lHomer},
			},
			{
				Query:    "select * from people where age in (-10.0, 40)",
				Expected: []sql.Row{lHomer, lBarney},
			},
			{
				Query:    "select * from people where first_name not in ('Homer', 'Marge')",
				Expected: []sql.Row{lBart, lLisa, lMoe, lBarney},
			},
			{
				Query:    "select * from people where first_name in ('Homer')",
				Expected: []sql.Row{lHomer},
			},
			{
				Query:    "select * from people where first_name in (1.0)",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from people where uuid is null",
				Expected: []sql.Row{lHomer},
			},
			{
				Query:    "select * from people where uuid is not null",
				Expected: []sql.Row{lMarge, lBart, lLisa, lMoe, lBarney},
			},
			{
				Query:    "select * from people where is_married is true",
				Expected: []sql.Row{lHomer, lMarge},
			},
			{
				Query:    "select * from people where is_married is not true",
				Expected: []sql.Row{lBart, lLisa, lMoe, lBarney},
			},
			{
				Query:    "select * from people where is_married is false",
				Expected: []sql.Row{lBart, lLisa, lMoe, lBarney},
			},
			{
				Query:    "select * from people where is_married is not false",
				Expected: []sql.Row{lHomer, lMarge},
			},
			{
				Query:    "select * from people where age is true",
				Expected: []sql.Row{lHomer, lMarge, lBart, lLisa, lMoe, lBarney},
			},
			{
				Query:    "select age + 1 as a from people where is_married order by a",
				Expected: []sql.Row{{int64(39)}, {int64(41)}},
			},
			{
				Query:    "select is_married and age >= 40 from people where last_name = 'Simpson' order by id limit 2",
				Expected: []sql.Row{{true}, {false}},
			},
			{
				Query:    "select first_name, age <= 10 or age >= 40 as not_marge from people where last_name = 'Simpson' order by id desc",
				Expected: []sql.Row{{"Lisa", true}, {"Bart", true}, {"Marge", false}, {"Homer", true}},
			},
			{
				Query:    "select -age as age from people where is_married order by age",
				Expected: []sql.Row{{int64(-40)}, {int64(-38)}},
			},
			{
				Query:    "select * from people where -rating = -8.5",
				Expected: []sql.Row{lHomer},
			},
			// -first_name evaluates to 0 for all rows (string->num conversion), 'Homer' also evaluates to 0
			{
				Query:    "select * from people where -first_name = 'Homer'",
				Expected: []sql.Row{lHomer, lMarge, lBart, lLisa, lMoe, lBarney},
			},
			{
				Query:    "select * from people where age + 1 = 41",
				Expected: []sql.Row{lHomer, lBarney},
			},
			{
				Query:    "select * from people where age - 1 = 39",
				Expected: []sql.Row{lHomer, lBarney},
			},
			{
				Query:    "select * from people where age / 2 = 20",
				Expected: []sql.Row{lHomer, lBarney},
			},
			{
				Query:    "select * from people where age * 2 = 80",
				Expected: []sql.Row{lHomer, lBarney},
			},
			{
				Query:    "select * from people where age % 4 = 0",
				Expected: []sql.Row{lHomer, lLisa, lMoe, lBarney},
			},
			{
				Query:    "select * from people where age / 4 + 2 * 2 = 14",
				Expected: []sql.Row{lHomer, lBarney},
			},
			{
				Query:    "select * from people where first_name + 1 = 41",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from people where first_name - 1 = 39",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from people where first_name / 2 = 20",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from people where first_name * 2 = 80",
				Expected: []sql.Row{},
			},
			// 0 % 4 = 0 for all rows (string first_name -> 0)
			{
				Query:    "select * from people where first_name % 4 = 0",
				Expected: []sql.Row{lHomer, lMarge, lBart, lLisa, lMoe, lBarney},
			},
			{
				Query:    "select * from people where `uuid` is not null and first_name <> 'Marge' order by last_name desc, age",
				Expected: []sql.Row{lMoe, lLisa, lBart, lBarney},
			},
			{
				Query:    "select first_name, last_name from people where age >= 40",
				Expected: []sql.Row{{"Homer", "Simpson"}, {"Moe", "Szyslak"}, {"Barney", "Gumble"}},
			},
			{
				Query:    "select first_name as f, last_name as l from people where age >= 40",
				Expected: []sql.Row{{"Homer", "Simpson"}, {"Moe", "Szyslak"}, {"Barney", "Gumble"}},
			},
			{
				Query:    "select first_name as f, last_name as f from people where age >= 40",
				Expected: []sql.Row{{"Homer", "Simpson"}, {"Moe", "Szyslak"}, {"Barney", "Gumble"}},
			},
			{
				Query:    "select first_name, first_name from people where age >= 40 order by id",
				Expected: []sql.Row{{"Homer", "Homer"}, {"Moe", "Moe"}, {"Barney", "Barney"}},
			},
			{
				Query:          "select first_name as f, last_name as f from people, people where age >= 40",
				ExpectedErrStr: `ambiguous column name "age", it's present in all these tables: [people people]`,
			},
			{
				Query:          "select * from people p, people p where age >= 40",
				ExpectedErrStr: `ambiguous column name "age", it's present in all these tables: [p p]`,
			},
			{
				Query:    "select first_name from people order by age, first_name",
				Expected: []sql.Row{{"Lisa"}, {"Bart"}, {"Marge"}, {"Barney"}, {"Homer"}, {"Moe"}},
			},
			{
				Query:    "select first_name as f from people order by age, f",
				Expected: []sql.Row{{"Lisa"}, {"Bart"}, {"Marge"}, {"Barney"}, {"Homer"}, {"Moe"}},
			},
			{
				Query:    "select p.first_name as f, p.last_name as l from people p where p.first_name = 'Homer'",
				Expected: []sql.Row{{"Homer", "Simpson"}},
			},
			{
				Query:    "select p.first_name, p.last_name from people p where p.first_name = 'Homer'",
				Expected: []sql.Row{{"Homer", "Simpson"}},
			},
			{
				Query:          "select m.first_name as f, p.last_name as l from people p where p.f = 'Homer'",
				ExpectedErrStr: `table "p" does not have column "f"`,
			},
			{
				Query: `select id as i, first_name as f, last_name as l, is_married as m, age as a,
						rating as r, uuid as u, num_episodes as n from people where age >= 40`,
				Expected: []sql.Row{
					{int64(0), "Homer", "Simpson", int64(1), int64(40), 8.5, nil, nil},
					{int64(4), "Moe", "Szyslak", int64(0), int64(48), 6.5, "00000000-0000-0000-0000-000000000004", uint64(444)},
					{int64(5), "Barney", "Gumble", int64(0), int64(40), 4.0, "00000000-0000-0000-0000-000000000005", uint64(555)},
				},
			},
			{
				Query:    "select * from people where age <> 40",
				Expected: []sql.Row{lMarge, lBart, lLisa, lMoe},
			},
			{
				Query:    "select * from people where age > 80",
				Expected: []sql.Row{},
			},
			{
				Query:    "select id, age from people where age > 80",
				Expected: []sql.Row{},
			},
			{
				Query:          "select * from dne",
				ExpectedErrStr: "table not found: dne",
			},
			{
				Query:          "select * from people join dne",
				ExpectedErrStr: "table not found: dne",
			},
			{
				Query:    "select 1",
				Expected: []sql.Row{{1}},
			},
			{
				Query:          "select * from people where dne > 8.0",
				ExpectedErrStr: `column "dne" could not be found in any table in scope`,
			},
			{
				Query:          "select * from people where rating > 8.0 order by dne",
				ExpectedErrStr: `column "dne" could not be found in any table in scope`,
			},
			{
				Query:          "select * from people where function(first_name)",
				ExpectedErrStr: "syntax error at position 37 near 'function'",
			},
			{
				Query:    `select * from people where id = "0"`,
				Expected: []sql.Row{lHomer},
			},
		},
	},
}

// LegacyJoinScriptTests are JOIN tests converted from sqle/sqlselect_test.go JoinTests.
var LegacyJoinScriptTests = []queries.ScriptTest{
	{
		Name:        "legacy join tests",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `select * from people, episodes order by people.id, episodes.id`,
				Expected: []sql.Row{
					concatSQLRows(lHomer, lEp1), concatSQLRows(lHomer, lEp2), concatSQLRows(lHomer, lEp3), concatSQLRows(lHomer, lEp4),
					concatSQLRows(lMarge, lEp1), concatSQLRows(lMarge, lEp2), concatSQLRows(lMarge, lEp3), concatSQLRows(lMarge, lEp4),
					concatSQLRows(lBart, lEp1), concatSQLRows(lBart, lEp2), concatSQLRows(lBart, lEp3), concatSQLRows(lBart, lEp4),
					concatSQLRows(lLisa, lEp1), concatSQLRows(lLisa, lEp2), concatSQLRows(lLisa, lEp3), concatSQLRows(lLisa, lEp4),
					concatSQLRows(lMoe, lEp1), concatSQLRows(lMoe, lEp2), concatSQLRows(lMoe, lEp3), concatSQLRows(lMoe, lEp4),
					concatSQLRows(lBarney, lEp1), concatSQLRows(lBarney, lEp2), concatSQLRows(lBarney, lEp3), concatSQLRows(lBarney, lEp4),
				},
			},
			{
				Query: `select * from people p, episodes e where e.id = p.id order by p.id`,
				Expected: []sql.Row{
					concatSQLRows(lMarge, lEp1),
					concatSQLRows(lBart, lEp2),
					concatSQLRows(lLisa, lEp3),
					concatSQLRows(lMoe, lEp4),
				},
			},
			{
				Query: `select p.*, e.* from people p, episodes e, appearances a where a.episode_id = e.id and a.character_id = p.id order by p.id, e.id`,
				Expected: []sql.Row{
					concatSQLRows(lHomer, lEp1),
					concatSQLRows(lHomer, lEp2),
					concatSQLRows(lHomer, lEp3),
					concatSQLRows(lMarge, lEp1),
					concatSQLRows(lMarge, lEp3),
					concatSQLRows(lBart, lEp2),
					concatSQLRows(lLisa, lEp2),
					concatSQLRows(lLisa, lEp3),
					concatSQLRows(lMoe, lEp2),
					concatSQLRows(lBarney, lEp3),
				},
			},
			{
				Query:          `select id from people p, episodes e, appearances a where a.episode_id = e.id and a.character_id = p.id`,
				ExpectedErrStr: `ambiguous column name "id", it's present in all these tables: [e p]`,
			},
			{
				Query:          `select p.*, e.* from people p, episodes e, appearances a where a.episode_id = id and a.character_id = id`,
				ExpectedErrStr: `ambiguous column name "id", it's present in all these tables: [e p]`,
			},
			{
				Query: `select e.id, p.id, e.name, p.first_name, p.last_name from people p, episodes e where e.id = p.id order by p.id`,
				Expected: []sql.Row{
					{int64(1), int64(1), "Simpsons Roasting On an Open Fire", "Marge", "Simpson"},
					{int64(2), int64(2), "Bart the Genius", "Bart", "Simpson"},
					{int64(3), int64(3), "Homer's Odyssey", "Lisa", "Simpson"},
					{int64(4), int64(4), "There's No Disgrace Like Home", "Moe", "Szyslak"},
				},
			},
			{
				Query: "select e.id as eid, p.id as pid, e.name as ename, p.first_name as pfirst_name, p.last_name last_name from people p, episodes e where e.id = p.id order by pid",
				Expected: []sql.Row{
					{int64(1), int64(1), "Simpsons Roasting On an Open Fire", "Marge", "Simpson"},
					{int64(2), int64(2), "Bart the Genius", "Bart", "Simpson"},
					{int64(3), int64(3), "Homer's Odyssey", "Lisa", "Simpson"},
					{int64(4), int64(4), "There's No Disgrace Like Home", "Moe", "Szyslak"},
				},
			},
			{
				Query: "select e.id as eid, p.id as `p.id`, e.name as ename, p.first_name as pfirst_name, p.last_name last_name from people p, episodes e where e.id = p.id order by `p.id`",
				Expected: []sql.Row{
					{int64(1), int64(1), "Simpsons Roasting On an Open Fire", "Marge", "Simpson"},
					{int64(2), int64(2), "Bart the Genius", "Bart", "Simpson"},
					{int64(3), int64(3), "Homer's Odyssey", "Lisa", "Simpson"},
					{int64(4), int64(4), "There's No Disgrace Like Home", "Moe", "Szyslak"},
				},
			},
			{
				Query: `select * from people p join episodes e on e.id = p.id order by p.id`,
				Expected: []sql.Row{
					concatSQLRows(lMarge, lEp1),
					concatSQLRows(lBart, lEp2),
					concatSQLRows(lLisa, lEp3),
					concatSQLRows(lMoe, lEp4),
				},
			},
			{
				Query: `select p.*, e.* from people p join appearances a on a.character_id = p.id join episodes e on a.episode_id = e.id order by p.id, e.id`,
				Expected: []sql.Row{
					concatSQLRows(lHomer, lEp1),
					concatSQLRows(lHomer, lEp2),
					concatSQLRows(lHomer, lEp3),
					concatSQLRows(lMarge, lEp1),
					concatSQLRows(lMarge, lEp3),
					concatSQLRows(lBart, lEp2),
					concatSQLRows(lLisa, lEp2),
					concatSQLRows(lLisa, lEp3),
					concatSQLRows(lMoe, lEp2),
					concatSQLRows(lBarney, lEp3),
				},
			},
			{
				Query: `select e.id, p.id, e.name, p.first_name, p.last_name from people p join episodes e on e.id = p.id order by p.id`,
				Expected: []sql.Row{
					{int64(1), int64(1), "Simpsons Roasting On an Open Fire", "Marge", "Simpson"},
					{int64(2), int64(2), "Bart the Genius", "Bart", "Simpson"},
					{int64(3), int64(3), "Homer's Odyssey", "Lisa", "Simpson"},
					{int64(4), int64(4), "There's No Disgrace Like Home", "Moe", "Szyslak"},
				},
			},
			{
				Query: `select e.name, p.first_name, p.last_name from people p join episodes e on e.id = p.id order by p.id`,
				Expected: []sql.Row{
					{"Simpsons Roasting On an Open Fire", "Marge", "Simpson"},
					{"Bart the Genius", "Bart", "Simpson"},
					{"Homer's Odyssey", "Lisa", "Simpson"},
					{"There's No Disgrace Like Home", "Moe", "Szyslak"},
				},
			},
			{
				Query: `select e.id, p.id, e.name, p.first_name, p.last_name from people p
								join episodes e on e.id = p.id
								order by e.name`,
				Expected: []sql.Row{
					{int64(2), int64(2), "Bart the Genius", "Bart", "Simpson"},
					{int64(3), int64(3), "Homer's Odyssey", "Lisa", "Simpson"},
					{int64(1), int64(1), "Simpsons Roasting On an Open Fire", "Marge", "Simpson"},
					{int64(4), int64(4), "There's No Disgrace Like Home", "Moe", "Szyslak"},
				},
			},
			{
				Query: `select e.id, p.id, e.name, p.first_name, p.last_name from people p
								join episodes e on e.id = p.id
								order by age`,
				Expected: []sql.Row{
					{int64(3), int64(3), "Homer's Odyssey", "Lisa", "Simpson"},
					{int64(2), int64(2), "Bart the Genius", "Bart", "Simpson"},
					{int64(1), int64(1), "Simpsons Roasting On an Open Fire", "Marge", "Simpson"},
					{int64(4), int64(4), "There's No Disgrace Like Home", "Moe", "Szyslak"},
				},
			},
			{
				Query: "select e.id as eid, p.id as pid, e.name as ename, p.first_name as pfirst_name, p.last_name last_name from people p join episodes e on e.id = p.id order by pid",
				Expected: []sql.Row{
					{int64(1), int64(1), "Simpsons Roasting On an Open Fire", "Marge", "Simpson"},
					{int64(2), int64(2), "Bart the Genius", "Bart", "Simpson"},
					{int64(3), int64(3), "Homer's Odyssey", "Lisa", "Simpson"},
					{int64(4), int64(4), "There's No Disgrace Like Home", "Moe", "Szyslak"},
				},
			},
			{
				Query: "select e.id as eid, p.id as pid, e.name as ename, p.first_name as pfirst_name, p.last_name last_name from people p join episodes e on e.id = p.id order by ename",
				Expected: []sql.Row{
					{int64(2), int64(2), "Bart the Genius", "Bart", "Simpson"},
					{int64(3), int64(3), "Homer's Odyssey", "Lisa", "Simpson"},
					{int64(1), int64(1), "Simpsons Roasting On an Open Fire", "Marge", "Simpson"},
					{int64(4), int64(4), "There's No Disgrace Like Home", "Moe", "Szyslak"},
				},
			},
			{
				Query: "select e.id as eid, p.id as `p.id`, e.name as ename, p.first_name as pfirst_name, p.last_name last_name from people p join episodes e on e.id = p.id order by `p.id`",
				Expected: []sql.Row{
					{int64(1), int64(1), "Simpsons Roasting On an Open Fire", "Marge", "Simpson"},
					{int64(2), int64(2), "Bart the Genius", "Bart", "Simpson"},
					{int64(3), int64(3), "Homer's Odyssey", "Lisa", "Simpson"},
					{int64(4), int64(4), "There's No Disgrace Like Home", "Moe", "Szyslak"},
				},
			},
			{
				Query: `select a.episode_id as eid, p.id as pid, p.first_name
						from appearances a join people p on a.character_id = p.id order by eid, pid`,
				Expected: []sql.Row{
					{int64(1), int64(0), "Homer"},
					{int64(1), int64(1), "Marge"},
					{int64(2), int64(0), "Homer"},
					{int64(2), int64(2), "Bart"},
					{int64(2), int64(3), "Lisa"},
					{int64(2), int64(4), "Moe"},
					{int64(3), int64(0), "Homer"},
					{int64(3), int64(1), "Marge"},
					{int64(3), int64(3), "Lisa"},
					{int64(3), int64(5), "Barney"},
				},
			},
		},
	},
}

// LegacyInsertScriptTests are INSERT tests converted from sqle/sqlinsert_test.go BasicInsertTests.
var LegacyInsertScriptTests = []queries.ScriptTest{
	{
		Name:        "insert no columns",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into people values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002', 222)", SkipResultsCheck: true},
			{Query: "select * from people where id = 2 ORDER BY id", Expected: []sql.Row{lBart}},
		},
	},
	{
		Name:        "insert no columns too few values",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into people values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002')", ExpectedErrStr: "number of values does not match number of columns provided"},
		},
	},
	{
		Name:        "insert no columns too many values",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into people values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002', 222, 'abc')", ExpectedErrStr: "number of values does not match number of columns provided"},
		},
	},
	{
		Name:        "insert full columns",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into people (id, first_name, last_name, is_married, age, rating, uuid, num_episodes) values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002', 222)", SkipResultsCheck: true},
			{Query: "select * from people where id = 2 ORDER BY id", Expected: []sql.Row{lBart}},
		},
	},
	{
		Name:        "insert full columns mixed order",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into people (num_episodes, uuid, rating, age, is_married, last_name, first_name, id) values (222, '00000000-0000-0000-0000-000000000002', 9, 10, false, 'Simpson', 'Bart', 2)", SkipResultsCheck: true},
			{Query: "select * from people where id = 2 ORDER BY id", Expected: []sql.Row{lBart}},
		},
	},
	{
		Name:        "insert full columns negative values",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `insert into people (id, first_name, last_name, is_married, age, rating, uuid, num_episodes) values (-7, 'Maggie', 'Simpson', false, -1, -5.1, '00000000-0000-0000-0000-000000000005', 677)`, SkipResultsCheck: true},
			{Query: "select * from people where id = -7 ORDER BY id", Expected: []sql.Row{
				{int64(-7), "Maggie", "Simpson", int64(0), int64(-1), -5.1, "00000000-0000-0000-0000-000000000005", uint64(677)},
			}},
		},
	},
	{
		Name:        "insert full columns null values",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into people (id, first_name, last_name, is_married, age, rating, uuid, num_episodes) values (2, 'Bart', 'Simpson', null, null, null, null, null)", SkipResultsCheck: true},
			{Query: "select * from people where id = 2 ORDER BY id", Expected: []sql.Row{
				{int64(2), "Bart", "Simpson", nil, nil, nil, nil, nil},
			}},
		},
	},
	{
		Name:        "insert partial columns",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into people (id, first_name, last_name) values (2, 'Bart', 'Simpson')", SkipResultsCheck: true},
			{Query: "select id, first_name, last_name from people where id = 2 ORDER BY id", Expected: []sql.Row{
				{int64(2), "Bart", "Simpson"},
			}},
		},
	},
	{
		Name:        "insert partial columns mixed order",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into people (last_name, first_name, id) values ('Simpson', 'Bart', 2)", SkipResultsCheck: true},
			{Query: "select id, first_name, last_name from people where id = 2 ORDER BY id", Expected: []sql.Row{
				{int64(2), "Bart", "Simpson"},
			}},
		},
	},
	{
		Name:        "insert partial columns duplicate column",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into people (id, first_name, last_name, first_name) values (2, 'Bart', 'Simpson', 'Bart')", ExpectedErrStr: "column 'first_name' specified twice"},
		},
	},
	{
		Name:        "insert partial columns invalid column",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into people (id, first_name, last_name, middle) values (2, 'Bart', 'Simpson', 'Nani')", ExpectedErrStr: "Unknown column 'middle' in 'people'"},
		},
	},
	{
		Name:        "insert missing non-nullable column",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into people (id, first_name) values (2, 'Bart')", ExpectedErrStr: "Field 'last_name' doesn't have a default value"},
		},
	},
	{
		Name:        "insert partial columns mismatch too many values",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into people (id, first_name, last_name) values (2, 'Bart', 'Simpson', false)", ExpectedErrStr: "number of values does not match number of columns provided"},
		},
	},
	{
		Name:        "insert partial columns mismatch too few values",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into people (id, first_name, last_name) values (2, 'Bart')", ExpectedErrStr: "number of values does not match number of columns provided"},
		},
	},
	{
		Name:        "insert partial columns functions",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into people (id, first_name, last_name) values (2, UPPER('Bart'), 'Simpson')", SkipResultsCheck: true},
			{Query: "select id, first_name, last_name from people where id = 2 ORDER BY id", Expected: []sql.Row{
				{int64(2), "BART", "Simpson"},
			}},
		},
	},
	{
		Name:        "insert partial columns multiple rows 2",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into people (id, first_name, last_name) values (0, 'Bart', 'Simpson'), (1, 'Homer', 'Simpson')", SkipResultsCheck: true},
			{Query: "select id, first_name, last_name from people where id < 2 order by id", Expected: []sql.Row{
				{int64(0), "Bart", "Simpson"},
				{int64(1), "Homer", "Simpson"},
			}},
		},
	},
	{
		Name:        "insert partial columns multiple rows 5",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `insert into people (id, first_name, last_name, is_married, age, rating) values
					(7, 'Maggie', 'Simpson', false, 1, 5.1),
					(8, 'Milhouse', 'Van Houten', false, 8, 3.5),
					(9, 'Jacqueline', 'Bouvier', true, 80, 2),
					(10, 'Patty', 'Bouvier', false, 40, 7),
					(11, 'Selma', 'Bouvier', false, 40, 7)`, SkipResultsCheck: true},
			{Query: "select id, first_name, last_name, is_married, age, rating from people where id > 6 ORDER BY id", Expected: []sql.Row{
				{int64(7), "Maggie", "Simpson", int64(0), int64(1), 5.1},
				{int64(8), "Milhouse", "Van Houten", int64(0), int64(8), 3.5},
				{int64(9), "Jacqueline", "Bouvier", int64(1), int64(80), 2.0},
				{int64(10), "Patty", "Bouvier", int64(0), int64(40), 7.0},
				{int64(11), "Selma", "Bouvier", int64(0), int64(40), 7.0},
			}},
		},
	},
	{
		Name:        "insert partial columns multiple rows null pk",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into people (id, first_name, last_name) values (0, 'Bart', 'Simpson'), (1, 'Homer', null)", ExpectedErrStr: "column name 'last_name' is non-nullable but attempted to set a value of null"},
		},
	},
	{
		Name:        "insert partial columns multiple rows duplicate",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into people (id, first_name, last_name) values (2, 'Bart', 'Simpson'), (2, 'Bart', 'Simpson')", ExpectedErrStr: "duplicate primary key given: [2]"},
		},
	},
	{
		Name: "insert partial columns existing pk",
		SetUpScript: append(legacyEmptySetup,
			"CREATE TABLE temppeople (id bigint primary key, first_name varchar(1023), last_name varchar(1023))",
			"INSERT INTO temppeople VALUES (2, 'Bart', 'Simpson')",
		),
		Assertions: []queries.ScriptTestAssertion{
			{Query: "insert into temppeople (id, first_name, last_name) values (2, 'Bart', 'Simpson')", ExpectedErrStr: "duplicate primary key given: [2]"},
		},
	},
	{
		Name:        "insert type mismatch int -> string",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `insert into people (id, first_name, last_name, is_married, age, rating) values (7, 'Maggie', 100, false, 1, 5.1)`, SkipResultsCheck: true},
			{Query: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id", Expected: []sql.Row{
				{int64(7), "Maggie", "100", int64(0), int64(1), 5.1},
			}},
		},
	},
	{
		Name:        "insert type mismatch int -> bool",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `insert into people (id, first_name, last_name, is_married, age, rating) values (7, 'Maggie', 'Simpson', 1, 1, 5.1)`, SkipResultsCheck: true},
			{Query: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id", Expected: []sql.Row{
				{int64(7), "Maggie", "Simpson", int64(1), int64(1), 5.1},
			}},
		},
	},
	{
		Name:        "insert type mismatch string -> int",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `insert into people (id, first_name, last_name, is_married, age, rating) values ('7', 'Maggie', 'Simpson', false, 1, 5.1)`, SkipResultsCheck: true},
			{Query: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id", Expected: []sql.Row{
				{int64(7), "Maggie", "Simpson", int64(0), int64(1), 5.1},
			}},
		},
	},
	{
		Name:        "insert type mismatch string -> float",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `insert into people (id, first_name, last_name, is_married, age, rating) values (7, 'Maggie', 'Simpson', false, 1, '5.1')`, SkipResultsCheck: true},
			{Query: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id", Expected: []sql.Row{
				{int64(7), "Maggie", "Simpson", int64(0), int64(1), 5.1},
			}},
		},
	},
	{
		Name:        "insert type mismatch string -> uint",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `insert into people (id, first_name, last_name, is_married, age, num_episodes) values (7, 'Maggie', 'Simpson', false, 1, '100')`, SkipResultsCheck: true},
			{Query: "select id, first_name, last_name, is_married, age, num_episodes from people where id = 7 ORDER BY id", Expected: []sql.Row{
				{int64(7), "Maggie", "Simpson", int64(0), int64(1), uint64(100)},
			}},
		},
	},
	{
		Name:        "insert type mismatch float -> string",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `insert into people (id, first_name, last_name, is_married, age, rating) values (7, 8.1, 'Simpson', false, 1, 5.1)`, SkipResultsCheck: true},
			{Query: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id", Expected: []sql.Row{
				{int64(7), "8.1", "Simpson", int64(0), int64(1), 5.1},
			}},
		},
	},
	{
		Name:        "insert type mismatch float -> bool",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `insert into people (id, first_name, last_name, is_married, age, rating) values (7, 'Maggie', 'Simpson', 0.5, 1, 5.1)`, SkipResultsCheck: true},
			{Query: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id", Expected: []sql.Row{
				{int64(7), "Maggie", "Simpson", int64(1), int64(1), 5.1},
			}},
		},
	},
	{
		Name:        "insert type mismatch float -> int",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `insert into people (id, first_name, last_name, is_married, age, rating) values (7, 'Maggie', 'Simpson', false, 1.0, 5.1)`, SkipResultsCheck: true},
			{Query: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id", Expected: []sql.Row{
				{int64(7), "Maggie", "Simpson", int64(0), int64(1), 5.1},
			}},
		},
	},
	{
		Name:        "insert type mismatch bool -> int",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `insert into people (id, first_name, last_name, is_married, age, rating) values (true, 'Maggie', 'Simpson', false, 1, 5.1)`, SkipResultsCheck: true},
			{Query: "select id, first_name, last_name, is_married, age, rating from people where id = 1 ORDER BY id", Expected: []sql.Row{
				{int64(1), "Maggie", "Simpson", int64(0), int64(1), 5.1},
			}},
		},
	},
	{
		Name:        "insert type mismatch bool -> float",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `insert into people (id, first_name, last_name, is_married, age, rating) values (7, 'Maggie', 'Simpson', false, 1, true)`, SkipResultsCheck: true},
			{Query: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id", Expected: []sql.Row{
				{int64(7), "Maggie", "Simpson", int64(0), int64(1), 1.0},
			}},
		},
	},
	{
		Name:        "insert type mismatch bool -> string",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `insert into people (id, first_name, last_name, is_married, age, rating) values (7, true, 'Simpson', false, 1, 5.1)`, SkipResultsCheck: true},
			{Query: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id", Expected: []sql.Row{
				{int64(7), "1", "Simpson", int64(0), int64(1), 5.1},
			}},
		},
	},
}

// LegacyUpdateScriptTests are UPDATE tests converted from sqle/sqlupdate_test.go BasicUpdateTests.
var LegacyUpdateScriptTests = []queries.ScriptTest{
	{
		Name:        "update one row, one col, primary key where clause",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set first_name = 'Domer' where id = 0`, SkipResultsCheck: true},
			{Query: `select * from people where id = 0`, Expected: []sql.Row{
				{int64(0), "Domer", "Simpson", int64(1), int64(40), 8.5, nil, nil},
			}},
		},
	},
	{
		Name:        "update one row, one col, non-primary key where clause",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set first_name = 'Domer' where first_name = 'Homer'`, SkipResultsCheck: true},
			{Query: `select * from people where first_name = 'Domer'`, Expected: []sql.Row{
				{int64(0), "Domer", "Simpson", int64(1), int64(40), 8.5, nil, nil},
			}},
		},
	},
	{
		Name:        "update one row, two cols, primary key where clause",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set first_name = 'Ned', last_name = 'Flanders' where id = 0`, SkipResultsCheck: true},
			{Query: `select * from people where id = 0`, Expected: []sql.Row{
				{int64(0), "Ned", "Flanders", int64(1), int64(40), 8.5, nil, nil},
			}},
		},
	},
	{
		Name:        "update one row, all cols, non-primary key where clause",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set first_name = 'Ned', last_name = 'Flanders', is_married = false, rating = 10,
				age = 45, num_episodes = 150, uuid = '00000000-0000-0000-0000-000000000050'
				where age = 38`, SkipResultsCheck: true},
			{Query: `select * from people where uuid = '00000000-0000-0000-0000-000000000050'`, Expected: []sql.Row{
				{int64(1), "Ned", "Flanders", int64(0), int64(45), 10.0, "00000000-0000-0000-0000-000000000050", uint64(150)},
			}},
		},
	},
	{
		Name:        "update one row, set columns to existing values",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set first_name = 'Homer', last_name = 'Simpson', is_married = true, rating = 8.5, age = 40,
				num_episodes = null, uuid = null where id = 0`, SkipResultsCheck: true},
			{Query: `select * from people where id = 0`, Expected: []sql.Row{lHomer}},
		},
	},
	{
		Name:        "update one row, null out existing values",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set first_name = 'Homer', last_name = 'Simpson', is_married = null, rating = null, age = null,
				num_episodes = null, uuid = null where first_name = 'Homer'`, SkipResultsCheck: true},
			{Query: `select * from people where first_name = 'Homer'`, Expected: []sql.Row{
				{int64(0), "Homer", "Simpson", nil, nil, nil, nil, nil},
			}},
		},
	},
	{
		Name:        "update multiple rows, set two columns",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set first_name = 'Changed', rating = 0.0 where last_name = 'Simpson'`, SkipResultsCheck: true},
			{Query: `select * from people where last_name = 'Simpson'`, Expected: []sql.Row{
				{int64(0), "Changed", "Simpson", int64(1), int64(40), 0.0, nil, nil},
				{int64(1), "Changed", "Simpson", int64(1), int64(38), 0.0, "00000000-0000-0000-0000-000000000001", uint64(111)},
				{int64(2), "Changed", "Simpson", int64(0), int64(10), 0.0, "00000000-0000-0000-0000-000000000002", uint64(222)},
				{int64(3), "Changed", "Simpson", int64(0), int64(8), 0.0, "00000000-0000-0000-0000-000000000003", uint64(333)},
			}},
		},
	},
	{
		Name:        "update no matching rows",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set first_name = 'Changed', rating = 0.0 where last_name = 'Flanders'`, SkipResultsCheck: true},
			{Query: `select * from people`, Expected: []sql.Row{lHomer, lMarge, lBart, lLisa, lMoe, lBarney}},
		},
	},
	{
		Name:        "update without where clause",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set first_name = 'Changed', rating = 0.0`, SkipResultsCheck: true},
			{Query: `select * from people`, Expected: []sql.Row{
				{int64(0), "Changed", "Simpson", int64(1), int64(40), 0.0, nil, nil},
				{int64(1), "Changed", "Simpson", int64(1), int64(38), 0.0, "00000000-0000-0000-0000-000000000001", uint64(111)},
				{int64(2), "Changed", "Simpson", int64(0), int64(10), 0.0, "00000000-0000-0000-0000-000000000002", uint64(222)},
				{int64(3), "Changed", "Simpson", int64(0), int64(8), 0.0, "00000000-0000-0000-0000-000000000003", uint64(333)},
				{int64(4), "Changed", "Szyslak", int64(0), int64(48), 0.0, "00000000-0000-0000-0000-000000000004", uint64(444)},
				{int64(5), "Changed", "Gumble", int64(0), int64(40), 0.0, "00000000-0000-0000-0000-000000000005", uint64(555)},
			}},
		},
	},
	{
		Name:        "update set first_name = last_name",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set first_name = last_name`, SkipResultsCheck: true},
			{Query: `select * from people`, Expected: []sql.Row{
				{int64(0), "Simpson", "Simpson", int64(1), int64(40), 8.5, nil, nil},
				{int64(1), "Simpson", "Simpson", int64(1), int64(38), 8.0, "00000000-0000-0000-0000-000000000001", uint64(111)},
				{int64(2), "Simpson", "Simpson", int64(0), int64(10), 9.0, "00000000-0000-0000-0000-000000000002", uint64(222)},
				{int64(3), "Simpson", "Simpson", int64(0), int64(8), 10.0, "00000000-0000-0000-0000-000000000003", uint64(333)},
				{int64(4), "Szyslak", "Szyslak", int64(0), int64(48), 6.5, "00000000-0000-0000-0000-000000000004", uint64(444)},
				{int64(5), "Gumble", "Gumble", int64(0), int64(40), 4.0, "00000000-0000-0000-0000-000000000005", uint64(555)},
			}},
		},
	},
	{
		Name:        "update increment age",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set age = age + 1`, SkipResultsCheck: true},
			{Query: `select * from people`, Expected: []sql.Row{
				{int64(0), "Homer", "Simpson", int64(1), int64(41), 8.5, nil, nil},
				{int64(1), "Marge", "Simpson", int64(1), int64(39), 8.0, "00000000-0000-0000-0000-000000000001", uint64(111)},
				{int64(2), "Bart", "Simpson", int64(0), int64(11), 9.0, "00000000-0000-0000-0000-000000000002", uint64(222)},
				{int64(3), "Lisa", "Simpson", int64(0), int64(9), 10.0, "00000000-0000-0000-0000-000000000003", uint64(333)},
				{int64(4), "Moe", "Szyslak", int64(0), int64(49), 6.5, "00000000-0000-0000-0000-000000000004", uint64(444)},
				{int64(5), "Barney", "Gumble", int64(0), int64(41), 4.0, "00000000-0000-0000-0000-000000000005", uint64(555)},
			}},
		},
	},
	{
		Name:        "update reverse rating",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set rating = -rating`, SkipResultsCheck: true},
			{Query: `select * from people`, Expected: []sql.Row{
				{int64(0), "Homer", "Simpson", int64(1), int64(40), -8.5, nil, nil},
				{int64(1), "Marge", "Simpson", int64(1), int64(38), -8.0, "00000000-0000-0000-0000-000000000001", uint64(111)},
				{int64(2), "Bart", "Simpson", int64(0), int64(10), -9.0, "00000000-0000-0000-0000-000000000002", uint64(222)},
				{int64(3), "Lisa", "Simpson", int64(0), int64(8), -10.0, "00000000-0000-0000-0000-000000000003", uint64(333)},
				{int64(4), "Moe", "Szyslak", int64(0), int64(48), -6.5, "00000000-0000-0000-0000-000000000004", uint64(444)},
				{int64(5), "Barney", "Gumble", int64(0), int64(40), -4.0, "00000000-0000-0000-0000-000000000005", uint64(555)},
			}},
		},
	},
	{
		Name:        "update datetime field",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update episodes set air_date = '1993-03-24 20:00:00' where id = 1`, SkipResultsCheck: true},
			{Query: `select * from episodes where id = 1`, Expected: []sql.Row{
				{int64(1), "Simpsons Roasting On an Open Fire", time.Date(1993, time.March, 24, 20, 0, 0, 0, time.UTC), 8.0},
			}},
		},
	},
	{
		Name:        "update name field",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update episodes set name = 'fake_name' where id = 1`, SkipResultsCheck: true},
			{Query: `select * from episodes where id = 1`, Expected: []sql.Row{
				{int64(1), "fake_name", time.Date(1989, time.December, 18, 3, 0, 0, 0, time.UTC), 8.0},
			}},
		},
	},
	{
		Name:        "update multiple rows, =",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set first_name = 'Homer' where last_name = 'Simpson'`, SkipResultsCheck: true},
			{Query: `select * from people where last_name = 'Simpson'`, Expected: []sql.Row{
				lHomer,
				{int64(1), "Homer", "Simpson", int64(1), int64(38), 8.0, "00000000-0000-0000-0000-000000000001", uint64(111)},
				{int64(2), "Homer", "Simpson", int64(0), int64(10), 9.0, "00000000-0000-0000-0000-000000000002", uint64(222)},
				{int64(3), "Homer", "Simpson", int64(0), int64(8), 10.0, "00000000-0000-0000-0000-000000000003", uint64(333)},
			}},
		},
	},
	{
		Name:        "update multiple rows, <>",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set last_name = 'Simpson' where last_name <> 'Simpson'`, SkipResultsCheck: true},
			{Query: `select * from people`, Expected: []sql.Row{
				lHomer, lMarge, lBart, lLisa,
				{int64(4), "Moe", "Simpson", int64(0), int64(48), 6.5, "00000000-0000-0000-0000-000000000004", uint64(444)},
				{int64(5), "Barney", "Simpson", int64(0), int64(40), 4.0, "00000000-0000-0000-0000-000000000005", uint64(555)},
			}},
		},
	},
	{
		Name:        "update multiple rows pk increment order by desc",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set id = id + 1 order by id desc`, SkipResultsCheck: true},
			{Query: `select * from people order by id`, Expected: []sql.Row{
				{int64(1), "Homer", "Simpson", int64(1), int64(40), 8.5, nil, nil},
				{int64(2), "Marge", "Simpson", int64(1), int64(38), 8.0, "00000000-0000-0000-0000-000000000001", uint64(111)},
				{int64(3), "Bart", "Simpson", int64(0), int64(10), 9.0, "00000000-0000-0000-0000-000000000002", uint64(222)},
				{int64(4), "Lisa", "Simpson", int64(0), int64(8), 10.0, "00000000-0000-0000-0000-000000000003", uint64(333)},
				{int64(5), "Moe", "Szyslak", int64(0), int64(48), 6.5, "00000000-0000-0000-0000-000000000004", uint64(444)},
				{int64(6), "Barney", "Gumble", int64(0), int64(40), 4.0, "00000000-0000-0000-0000-000000000005", uint64(555)},
			}},
		},
	},
	{
		Name:        "update multiple rows pk increment order by asc",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set id = id + 1 order by id asc`, ExpectedErrStr: "duplicate primary key given: [1]"},
		},
	},
	{
		Name:        "update primary key col",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set id = 0 where first_name = 'Marge'`, ExpectedErrStr: "duplicate primary key given: [0]"},
		},
	},
	{
		Name:        "update null constraint failure",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `update people set first_name = null where id = 0`, ExpectedErrStr: "column name 'first_name' is non-nullable but attempted to set a value of null"},
		},
	},
}

// LegacyDeleteScriptTests are DELETE tests converted from sqle/sqldelete_test.go BasicDeleteTests.
var LegacyDeleteScriptTests = []queries.ScriptTest{
	{
		Name:        "delete everything",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "delete from people", SkipResultsCheck: true},
			{Query: "select * from people", Expected: []sql.Row{}},
		},
	},
	{
		Name:        "delete where id equals",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "delete from people where id = 2", SkipResultsCheck: true},
			{Query: "select * from people", Expected: []sql.Row{lHomer, lMarge, lLisa, lMoe, lBarney}},
		},
	},
	{
		Name:        "delete where id less than",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "delete from people where id < 3", SkipResultsCheck: true},
			{Query: "select * from people", Expected: []sql.Row{lLisa, lMoe, lBarney}},
		},
	},
	{
		Name:        "delete where id greater than",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "delete from people where id > 3", SkipResultsCheck: true},
			{Query: "select * from people", Expected: []sql.Row{lHomer, lMarge, lBart, lLisa}},
		},
	},
	{
		Name:        "delete where id less than or equal",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "delete from people where id <= 3", SkipResultsCheck: true},
			{Query: "select * from people", Expected: []sql.Row{lMoe, lBarney}},
		},
	},
	{
		Name:        "delete where id greater than or equal",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "delete from people where id >= 3", SkipResultsCheck: true},
			{Query: "select * from people", Expected: []sql.Row{lHomer, lMarge, lBart}},
		},
	},
	{
		Name:        "delete where id equals nothing",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "delete from people where id = 9999", SkipResultsCheck: true},
			{Query: "select * from people", Expected: []sql.Row{lHomer, lMarge, lBart, lLisa, lMoe, lBarney}},
		},
	},
	{
		Name:        "delete where last_name matches some =",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "delete from people where last_name = 'Simpson'", SkipResultsCheck: true},
			{Query: "select * from people", Expected: []sql.Row{lMoe, lBarney}},
		},
	},
	{
		Name:        "delete where last_name matches some <>",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "delete from people where last_name <> 'Simpson'", SkipResultsCheck: true},
			{Query: "select * from people", Expected: []sql.Row{lHomer, lMarge, lBart, lLisa}},
		},
	},
	{
		Name:        "delete where last_name matches some like",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "delete from people where last_name like '%pson'", SkipResultsCheck: true},
			{Query: "select * from people", Expected: []sql.Row{lMoe, lBarney}},
		},
	},
	{
		Name:        "delete order by",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "delete from people order by id", SkipResultsCheck: true},
			{Query: "select * from people", Expected: []sql.Row{}},
		},
	},
	{
		Name:        "delete order by asc limit",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "delete from people order by id asc limit 3", SkipResultsCheck: true},
			{Query: "select * from people", Expected: []sql.Row{lLisa, lMoe, lBarney}},
		},
	},
	{
		Name:        "delete order by desc limit",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "delete from people order by id desc limit 3", SkipResultsCheck: true},
			{Query: "select * from people", Expected: []sql.Row{lHomer, lMarge, lBart}},
		},
	},
	{
		Name:        "delete order by desc limit offset",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "delete from people order by id desc limit 3 offset 1", SkipResultsCheck: true},
			{Query: "select * from people", Expected: []sql.Row{lHomer, lMarge, lBarney}},
		},
	},
	{
		Name:        "delete invalid table",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "delete from nobody", ExpectedErrStr: "table not found: nobody"},
		},
	},
	{
		Name:        "delete invalid column",
		SetUpScript: legacySetupWithData,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "delete from people where z = 'dne'", ExpectedErrStr: `column "z" could not be found in any table in scope`},
		},
	},
}

// LegacyReplaceScriptTests are REPLACE tests converted from sqle/sqlreplace_test.go BasicReplaceTests.
var LegacyReplaceScriptTests = []queries.ScriptTest{
	{
		Name:        "replace no columns",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002', 222)", SkipResultsCheck: true},
			{Query: "select * from people where id = 2 ORDER BY id", Expected: []sql.Row{lBart}},
		},
	},
	{
		Name:        "replace set",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people set id = 2, first_name = 'Bart', last_name = 'Simpson', is_married = false, age = 10, rating = 9, uuid = '00000000-0000-0000-0000-000000000002', num_episodes = 222", SkipResultsCheck: true},
			{Query: "select * from people where id = 2 ORDER BY id", Expected: []sql.Row{lBart}},
		},
	},
	{
		Name:        "replace no columns too few values",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002')", ExpectedErrStr: "number of values does not match number of columns provided"},
		},
	},
	{
		Name:        "replace no columns too many values",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002', 222, 'abc')", ExpectedErrStr: "number of values does not match number of columns provided"},
		},
	},
	{
		Name:        "replace full columns",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people (id, first_name, last_name, is_married, age, rating, uuid, num_episodes) values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002', 222)", SkipResultsCheck: true},
			{Query: "select * from people where id = 2 ORDER BY id", Expected: []sql.Row{lBart}},
		},
	},
	{
		Name:        "replace full columns mixed order",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people (num_episodes, uuid, rating, age, is_married, last_name, first_name, id) values (222, '00000000-0000-0000-0000-000000000002', 9, 10, false, 'Simpson', 'Bart', 2)", SkipResultsCheck: true},
			{Query: "select * from people where id = 2 ORDER BY id", Expected: []sql.Row{lBart}},
		},
	},
	{
		Name:        "replace full columns negative values",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `replace into people (id, first_name, last_name, is_married, age, rating, uuid, num_episodes) values (-7, 'Maggie', 'Simpson', false, -1, -5.1, '00000000-0000-0000-0000-000000000005', 677)`, SkipResultsCheck: true},
			{Query: "select * from people where id = -7 ORDER BY id", Expected: []sql.Row{
				{int64(-7), "Maggie", "Simpson", int64(0), int64(-1), -5.1, "00000000-0000-0000-0000-000000000005", uint64(677)},
			}},
		},
	},
	{
		Name:        "replace full columns null values",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people (id, first_name, last_name, is_married, age, rating, uuid, num_episodes) values (2, 'Bart', 'Simpson', null, null, null, null, null)", SkipResultsCheck: true},
			{Query: "select * from people where id = 2 ORDER BY id", Expected: []sql.Row{
				{int64(2), "Bart", "Simpson", nil, nil, nil, nil, nil},
			}},
		},
	},
	{
		Name:        "replace partial columns",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people (id, first_name, last_name) values (2, 'Bart', 'Simpson')", SkipResultsCheck: true},
			{Query: "select id, first_name, last_name from people where id = 2 ORDER BY id", Expected: []sql.Row{
				{int64(2), "Bart", "Simpson"},
			}},
		},
	},
	{
		Name:        "replace partial columns mixed order",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people (last_name, first_name, id) values ('Simpson', 'Bart', 2)", SkipResultsCheck: true},
			{Query: "select id, first_name, last_name from people where id = 2 ORDER BY id", Expected: []sql.Row{
				{int64(2), "Bart", "Simpson"},
			}},
		},
	},
	{
		Name:        "replace partial columns duplicate column",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people (id, first_name, last_name, first_name) values (2, 'Bart', 'Simpson', 'Bart')", ExpectedErrStr: "column 'first_name' specified twice"},
		},
	},
	{
		Name:        "replace partial columns invalid column",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people (id, first_name, last_name, middle) values (2, 'Bart', 'Simpson', 'Nani')", ExpectedErrStr: "Unknown column 'middle' in 'people'"},
		},
	},
	{
		Name:        "replace missing non-nullable column",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people (id, first_name) values (2, 'Bart')", ExpectedErrStr: "Field 'last_name' doesn't have a default value"},
		},
	},
	{
		Name:        "replace partial columns mismatch too many values",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people (id, first_name, last_name) values (2, 'Bart', 'Simpson', false)", ExpectedErrStr: "number of values does not match number of columns provided"},
		},
	},
	{
		Name:        "replace partial columns mismatch too few values",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people (id, first_name, last_name) values (2, 'Bart')", ExpectedErrStr: "number of values does not match number of columns provided"},
		},
	},
	{
		Name:        "replace partial columns functions",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people (id, first_name, last_name) values (2, UPPER('Bart'), 'Simpson')", SkipResultsCheck: true},
			{Query: "select id, first_name, last_name from people where id = 2 ORDER BY id", Expected: []sql.Row{
				{int64(2), "BART", "Simpson"},
			}},
		},
	},
	{
		Name:        "replace partial columns multiple rows 2",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people (id, first_name, last_name) values (0, 'Bart', 'Simpson'), (1, 'Homer', 'Simpson')", SkipResultsCheck: true},
			{Query: "select id, first_name, last_name from people where id < 2 order by id", Expected: []sql.Row{
				{int64(0), "Bart", "Simpson"},
				{int64(1), "Homer", "Simpson"},
			}},
		},
	},
	{
		Name:        "replace partial columns multiple rows 5",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: `replace into people (id, first_name, last_name, is_married, age, rating) values
					(7, 'Maggie', 'Simpson', false, 1, 5.1),
					(8, 'Milhouse', 'Van Houten', false, 8, 3.5),
					(9, 'Jacqueline', 'Bouvier', true, 80, 2),
					(10, 'Patty', 'Bouvier', false, 40, 7),
					(11, 'Selma', 'Bouvier', false, 40, 7)`, SkipResultsCheck: true},
			{Query: "select id, first_name, last_name, is_married, age, rating from people where id > 6 ORDER BY id", Expected: []sql.Row{
				{int64(7), "Maggie", "Simpson", int64(0), int64(1), 5.1},
				{int64(8), "Milhouse", "Van Houten", int64(0), int64(8), 3.5},
				{int64(9), "Jacqueline", "Bouvier", int64(1), int64(80), 2.0},
				{int64(10), "Patty", "Bouvier", int64(0), int64(40), 7.0},
				{int64(11), "Selma", "Bouvier", int64(0), int64(40), 7.0},
			}},
		},
	},
	{
		Name:        "replace partial columns multiple rows null pk",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people (id, first_name, last_name) values (0, 'Bart', 'Simpson'), (1, 'Homer', null)", ExpectedErrStr: "column name 'last_name' is non-nullable but attempted to set a value of null"},
		},
	},
	{
		Name:        "replace partial columns multiple rows duplicate",
		SetUpScript: legacyEmptySetup,
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into people (id, first_name, last_name) values (2, 'Bart', 'Simpson'), (2, 'Bart', 'Simpson')", SkipResultsCheck: true},
			{Query: "select id, first_name, last_name from people where id = 2 ORDER BY id", Expected: []sql.Row{
				{int64(2), "Bart", "Simpson"},
			}},
		},
	},
	{
		Name: "replace partial columns multiple rows replace existing pk",
		SetUpScript: append(legacyEmptySetup,
			`INSERT INTO people VALUES
				(0, 'Homer', 'Simpson', 1, 40, 8.5, NULL, NULL),
				(1, 'Marge', 'Simpson', 1, 38, 8.0, '00000000-0000-0000-0000-000000000001', 111)`,
		),
		Assertions: []queries.ScriptTestAssertion{
			{Query: `replace into people (id, first_name, last_name, is_married, age, rating) values
					(0, 'Homer', 'Simpson', true, 45, 100),
					(8, 'Milhouse', 'Van Houten', false, 8, 100)`, SkipResultsCheck: true},
			{Query: "select id, first_name, last_name, is_married, age, rating from people where rating = 100 order by id", Expected: []sql.Row{
				{int64(0), "Homer", "Simpson", int64(1), int64(45), 100.0},
				{int64(8), "Milhouse", "Van Houten", int64(0), int64(8), 100.0},
			}},
		},
	},
	{
		Name: "replace partial columns existing pk",
		SetUpScript: append(legacyEmptySetup,
			"CREATE TABLE temppeople (id bigint primary key, first_name varchar(1023), last_name varchar(1023), num bigint)",
			"INSERT INTO temppeople VALUES (2, 'Bart', 'Simpson', 44)",
		),
		Assertions: []queries.ScriptTestAssertion{
			{Query: "replace into temppeople (id, first_name, last_name, num) values (2, 'Bart', 'Simpson', 88)", SkipResultsCheck: true},
			{Query: "select id, first_name, last_name, num from temppeople where id = 2 ORDER BY id", Expected: []sql.Row{
				{int64(2), "Bart", "Simpson", int64(88)},
			}},
		},
	},
}
