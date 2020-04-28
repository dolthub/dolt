/*
 * // Copyright 2020 Liquidata, Inc.
 * //
 * // Licensed under the Apache License, Version 2.0 (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * //     http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */

package sqle

import (
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/src-d/go-mysql-server/sql"

	. "github.com/liquidata-inc/dolt/go/libraries/doltcore/sql/sqltestutil"
)

// Structure for a test of a insert query
type DeleteTest struct {
	// The name of this test. Names should be unique and descriptive.
	Name string
	// The delete query to run
	DeleteQuery string
	// The select query to run to verify the results
	SelectQuery string
	// The schema of the result of the query, nil if an error is expected
	ExpectedSchema schema.Schema
	// The rows this query should return, nil if an error is expected
	ExpectedRows []sql.Row
	// An expected error string
	ExpectedErr string
	// Setup logic to run before executing this test, after initial tables have been created and populated
	AdditionalSetup SetupFn
	// Whether to skip this test on SqlEngine (go-mysql-server) execution.
	// Over time, this should become false for every query.
	SkipOnSqlEngine bool
}

// BasicDeleteTests cover basic delete statement features and error handling
var BasicDeleteTests = []DeleteTest{
	{
		Name:           "delete everything",
		DeleteQuery:    "delete from people",
		SelectQuery:    "select * from people",
		ExpectedRows:   nil,
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where id equals",
		DeleteQuery:    "delete from people where id = 2",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where id less than",
		DeleteQuery:    "delete from people where id < 3",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where id greater than",
		DeleteQuery:    "delete from people where id > 3",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart, Lisa),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where id less than or equal",
		DeleteQuery:    "delete from people where id <= 3",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where id greater than or equal",
		DeleteQuery:    "delete from people where id >= 3",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where id equals nothing",
		DeleteQuery:    "delete from people where id = 9999",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where last_name matches some =",
		DeleteQuery:    "delete from people where last_name = 'Simpson'",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where last_name matches some <>",
		DeleteQuery:    "delete from people where last_name <> 'Simpson'",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart, Lisa),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where last_name matches some like",
		DeleteQuery:    "delete from people where last_name like '%pson'",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete order by",
		DeleteQuery:    "delete from people order by id",
		SelectQuery:    "select * from people",
		ExpectedRows:   nil,
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete order by asc limit",
		DeleteQuery:    "delete from people order by id asc limit 3",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete order by desc limit",
		DeleteQuery:    "delete from people order by id desc limit 3",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete order by desc limit",
		DeleteQuery:    "delete from people order by id desc limit 3 offset 1",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "delete invalid table",
		DeleteQuery: "delete from nobody",
		ExpectedErr: "invalid table",
	},
	{
		Name:        "delete invalid column",
		DeleteQuery: "delete from people where z = 'dne'",
		ExpectedErr: "invalid column",
	},
	{
		Name:        "delete negative limit",
		DeleteQuery: "delete from people limit -1",
		ExpectedErr: "invalid limit number",
	},
	{
		Name:        "delete negative offset",
		DeleteQuery: "delete from people limit 1 offset -1",
		ExpectedErr: "invalid limit number",
	},
}
