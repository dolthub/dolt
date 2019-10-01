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
	"github.com/google/uuid"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
)

// Structure for a test of an update query
type UpdateTest struct {
	// The name of this test. Names should be unique and descriptive.
	Name string
	// The update query to run
	UpdateQuery string
	// The select query to run to verify the results
	SelectQuery string
	// The schema of the result of the query, nil if an error is expected
	ExpectedSchema schema.Schema
	// The rows this query should return, nil if an error is expected
	ExpectedRows []row.Row
	// An expected error string
	ExpectedErr string
	// Setup logic to run before executing this test, after initial tables have been created and populated
	AdditionalSetup SetupFn
	// Whether to skip this test on SqlEngine (go-mysql-server) execution.
	// Over time, this should become false for every query.
	SkipOnSqlEngine bool
}

// BasicUpdateTests cover basic update statement features and error handling
var BasicUpdateTests = []UpdateTest{
	{
		Name:           "update one row, one col, primary key where clause",
		UpdateQuery:    `update people set first = "Domer" where id = 0`,
		SelectQuery:    `select * from people where id = 0`,
		ExpectedRows:   CompressRows(PeopleTestSchema, MutateRow(Homer, FirstTag, "Domer")),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "update one row, one col, non-primary key where clause",
		UpdateQuery:    `update people set first = "Domer" where first = "Homer"`,
		SelectQuery:    `select * from people where first = "Domer"`,
		ExpectedRows:   CompressRows(PeopleTestSchema, MutateRow(Homer, FirstTag, "Domer")),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "update one row, two cols, primary key where clause",
		UpdateQuery:    `update people set first = "Ned", last = "Flanders" where id = 0`,
		SelectQuery:    `select * from people where id = 0`,
		ExpectedRows:   CompressRows(PeopleTestSchema, MutateRow(Homer, FirstTag, "Ned", LastTag, "Flanders")),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name: "update one row, all cols, non-primary key where clause",
		UpdateQuery: `update people set first = "Ned", last = "Flanders", is_married = false, rating = 10,
				age = 45, num_episodes = 150, uuid = '00000000-0000-0000-0000-000000000050'
				where age = 38`,
		SelectQuery: `select * from people where uuid = '00000000-0000-0000-0000-000000000050'`,
		ExpectedRows: CompressRows(PeopleTestSchema,
			MutateRow(Marge, FirstTag, "Ned", LastTag, "Flanders", IsMarriedTag, false,
				RatingTag, 10.0, AgeTag, 45, NumEpisodesTag, uint64(150),
				UuidTag, uuid.MustParse("00000000-0000-0000-0000-000000000050"))),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name: "update one row, set columns to existing values",
		UpdateQuery: `update people set first = "Homer", last = "Simpson", is_married = true, rating = 8.5, age = 40,
				num_episodes = null, uuid = null
				where id = 0`,
		SelectQuery:    `select * from people where id = 0`,
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name: "update one row, null out existing values",
		UpdateQuery: `update people set first = "Homer", last = "Simpson", is_married = null, rating = null, age = null,
				num_episodes = null, uuid = null
				where first = "Homer"`,
		SelectQuery:    `select * from people where first = "Homer"`,
		ExpectedRows:   CompressRows(PeopleTestSchema, MutateRow(Homer, IsMarriedTag, nil, RatingTag, nil, AgeTag, nil)),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name: "update multiple rows, set two columns",
		UpdateQuery: `update people set first = "Changed", rating = 0.0
				where last = "Simpson"`,
		SelectQuery: `select * from people where last = "Simpson"`,
		ExpectedRows: CompressRows(PeopleTestSchema,
			MutateRow(Homer, FirstTag, "Changed", RatingTag, 0.0),
			MutateRow(Marge, FirstTag, "Changed", RatingTag, 0.0),
			MutateRow(Bart, FirstTag, "Changed", RatingTag, 0.0),
			MutateRow(Lisa, FirstTag, "Changed", RatingTag, 0.0),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "update no matching rows",
		UpdateQuery:    `update people set first = "Changed", rating = 0.0 where last = "Flanders"`,
		SelectQuery:    `select * from people`,
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update without where clause",
		UpdateQuery: `update people set first = "Changed", rating = 0.0`,
		SelectQuery: `select * from people`,
		ExpectedRows: CompressRows(PeopleTestSchema,
			MutateRow(Homer, FirstTag, "Changed", RatingTag, 0.0),
			MutateRow(Marge, FirstTag, "Changed", RatingTag, 0.0),
			MutateRow(Bart, FirstTag, "Changed", RatingTag, 0.0),
			MutateRow(Lisa, FirstTag, "Changed", RatingTag, 0.0),
			MutateRow(Moe, FirstTag, "Changed", RatingTag, 0.0),
			MutateRow(Barney, FirstTag, "Changed", RatingTag, 0.0),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update set first = last",
		UpdateQuery: `update people set first = last`,
		SelectQuery: `select * from people`,
		ExpectedRows: CompressRows(PeopleTestSchema,
			MutateRow(Homer, FirstTag, "Simpson"),
			MutateRow(Marge, FirstTag, "Simpson"),
			MutateRow(Bart, FirstTag, "Simpson"),
			MutateRow(Lisa, FirstTag, "Simpson"),
			MutateRow(Moe, FirstTag, "Szyslak"),
			MutateRow(Barney, FirstTag, "Gumble"),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update increment age",
		UpdateQuery: `update people set age = age + 1`,
		SelectQuery: `select * from people`,
		ExpectedRows: CompressRows(PeopleTestSchema,
			MutateRow(Homer, AgeTag, 41),
			MutateRow(Marge, AgeTag, 39),
			MutateRow(Bart, AgeTag, 11),
			MutateRow(Lisa, AgeTag, 9),
			MutateRow(Moe, AgeTag, 49),
			MutateRow(Barney, AgeTag, 41),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update reverse rating",
		UpdateQuery: `update people set rating = -rating`,
		SelectQuery: `select * from people`,
		ExpectedRows: CompressRows(PeopleTestSchema,
			MutateRow(Homer, RatingTag, -8.5),
			MutateRow(Marge, RatingTag, -8.0),
			MutateRow(Bart, RatingTag, -9.0),
			MutateRow(Lisa, RatingTag, -10.0),
			MutateRow(Moe, RatingTag, -6.5),
			MutateRow(Barney, RatingTag, -4.0),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update multiple rows, =",
		UpdateQuery: `update people set first = "Homer" where last = "Simpson"`,
		SelectQuery: `select * from people where last = "Simpson"`,
		ExpectedRows: CompressRows(PeopleTestSchema,
			Homer,
			MutateRow(Marge, FirstTag, "Homer"),
			MutateRow(Bart, FirstTag, "Homer"),
			MutateRow(Lisa, FirstTag, "Homer"),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update multiple rows, <>",
		UpdateQuery: `update people set last = "Simpson" where last <> "Simpson"`,
		SelectQuery: `select * from people`,
		ExpectedRows: CompressRows(PeopleTestSchema,
			Homer,
			Marge,
			Bart,
			Lisa,
			MutateRow(Moe, LastTag, "Simpson"),
			MutateRow(Barney, LastTag, "Simpson"),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update multiple rows, >",
		UpdateQuery: `update people set first = "Homer" where age > 10`,
		SelectQuery: `select * from people where age > 10`,
		ExpectedRows: CompressRows(PeopleTestSchema,
			Homer,
			MutateRow(Marge, FirstTag, "Homer"),
			MutateRow(Moe, FirstTag, "Homer"),
			MutateRow(Barney, FirstTag, "Homer"),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update multiple rows, >=",
		UpdateQuery: `update people set first = "Homer" where age >= 10`,
		SelectQuery: `select * from people where age >= 10`,
		ExpectedRows: CompressRows(PeopleTestSchema,
			Homer,
			MutateRow(Marge, FirstTag, "Homer"),
			MutateRow(Bart, FirstTag, "Homer"),
			MutateRow(Moe, FirstTag, "Homer"),
			MutateRow(Barney, FirstTag, "Homer"),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update multiple rows, <",
		UpdateQuery: `update people set first = "Bart" where age < 40`,
		SelectQuery: `select * from people where age < 40`,
		ExpectedRows: CompressRows(PeopleTestSchema,
			MutateRow(Marge, FirstTag, "Bart"),
			Bart,
			MutateRow(Lisa, FirstTag, "Bart"),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update multiple rows, <=",
		UpdateQuery: `update people set first = "Homer" where age <= 40`,
		SelectQuery: `select * from people where age <= 40`,
		ExpectedRows: CompressRows(PeopleTestSchema,
			Homer,
			MutateRow(Marge, FirstTag, "Homer"),
			MutateRow(Bart, FirstTag, "Homer"),
			MutateRow(Lisa, FirstTag, "Homer"),
			MutateRow(Barney, FirstTag, "Homer"),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update multiple rows pk increment order by desc",
		UpdateQuery: `update people set id = id + 1 order by id desc`,
		SelectQuery: `select * from people`,
		ExpectedRows: CompressRows(PeopleTestSchema,
			MutateRow(Homer, IdTag, homerId+1),
			MutateRow(Marge, IdTag, margeId+1),
			MutateRow(Bart, IdTag, bartId+1),
			MutateRow(Lisa, IdTag, lisaId+1),
			MutateRow(Moe, IdTag, moeId+1),
			MutateRow(Barney, IdTag, barneyId+1),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update multiple rows pk increment order by asc",
		UpdateQuery: `update people set id = id + 1 order by id asc`,
		ExpectedErr: "cannot update primary key column",
	},
	{
		Name:        "update primary key col",
		UpdateQuery: `update people set id = 0 where first = "Marge"`,
		ExpectedErr: "Cannot update primary key column 'id'",
	},
	{
		Name:        "null constraint failure",
		UpdateQuery: `update people set first = null where id = 0`,
		ExpectedErr: "Constraint failed for column 'first': Not null",
	},
	{
		Name:        "type mismatch list -> string",
		UpdateQuery: `update people set first = ("one", "two") where id = 0`,
		ExpectedErr: "Type mismatch",
	},
	{
		Name:        "type mismatch int -> uuid",
		UpdateQuery: `update people set uuid = 0 where id = 0`,
		ExpectedErr: "Type mismatch",
	},
	{
		Name:        "type mismatch string -> int",
		UpdateQuery: `update people set age = "pretty old" where id = 0`,
		ExpectedErr: "Type mismatch",
	},
	{
		Name:        "type mismatch string -> float",
		UpdateQuery: `update people set rating = "great" where id = 0`,
		ExpectedErr: "Type mismatch",
	},
	{
		Name:        "type mismatch string -> uint",
		UpdateQuery: `update people set num_episodes = "all of them" where id = 0`,
		ExpectedErr: "Type mismatch",
	},
	{
		Name:        "type mismatch string -> uuid",
		UpdateQuery: `update people set uuid = "not a uuid string" where id = 0`,
		ExpectedErr: "Type mismatch",
	},
	{
		Name:        "type mismatch bool -> uuid",
		UpdateQuery: `update people set uuid = false where id = 0`,
		ExpectedErr: "Type mismatch",
	},
	{
		Name:            "type mismatch in where clause",
		UpdateQuery:     `update people set first = "Homer" where id = "id"`,
		ExpectedErr:     "Type mismatch",
		SkipOnSqlEngine: true,
	},
}
