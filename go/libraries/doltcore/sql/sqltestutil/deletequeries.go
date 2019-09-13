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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
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
	ExpectedRows []row.Row
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
		ExpectedRows:   Rs(),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where id equals",
		DeleteQuery:    "delete from people where id = 2",
		SelectQuery:    "select * from people",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Marge, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where id less than",
		DeleteQuery:    "delete from people where id < 3",
		SelectQuery:    "select * from people",
		ExpectedRows:   CompressRows(PeopleTestSchema, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete order by",
		DeleteQuery:    "delete from people order by id",
		SelectQuery:    "select * from people",
		ExpectedRows:   Rs(),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete order by asc limit",
		DeleteQuery:    "delete from people order by id asc limit 3",
		SelectQuery:    "select * from people",
		ExpectedRows:   CompressRows(PeopleTestSchema, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete order by desc limit",
		DeleteQuery:    "delete from people order by id desc limit 3",
		SelectQuery:    "select * from people",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Marge, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
}
