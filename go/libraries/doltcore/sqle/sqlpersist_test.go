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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/variables"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// Structure for a test of a insert query
type PersistTest struct {
	// The name of this test. Names should be unique and descriptive.
	Name string
	// The insert query to run
	PersistQuery string
	// The insert query to run
	SelectQuery string
	// The schema of the result of the query, nil if an error is expected
	ExpectedSchema schema.Schema
	// The rows this query should return, nil if an error is expected
	ExpectedRows []sql.Row
	// The rows this query should return, nil if an error is expected
	ExpectedConfig map[string]string
	// An expected error string
	ExpectedErr string
	// Setup logic to run before executing this test, after initial tables have been created and populated
	AdditionalSetup SetupFn
}

const maxConnTag = 0

var MaxConnSchema = createMaxConnSchema()

func createMaxConnSchema() schema.Schema {
	colColl := schema.NewColCollection(
		schema.NewColumn("@@GLOBAL.max_connections", maxConnTag, types.IntKind, false, schema.NotNullConstraint{}),
	)
	return schema.MustSchemaFromCols(colColl)
}

func NewMaxConnRow(value int) row.Row {
	vals := row.TaggedValues{
		maxConnTag: types.Int(value),
	}

	r, _ := row.New(types.Format_Default, MaxConnSchema, vals)
	return r
}

func TestExecutePersist(t *testing.T) {
	var persistTests = []PersistTest{
		{
			Name:           "SET PERSIST a system variable",
			PersistQuery:   "SET PERSIST max_connections = 1000;",
			ExpectedConfig: map[string]string{"max_connections": "1000"},
			SelectQuery:    "SELECT @@GLOBAL.max_connections",
			ExpectedRows:   ToSqlRows(MaxConnSchema, NewMaxConnRow(1000)),
		},
		{
			Name:           "PERSIST ONLY a system variable",
			PersistQuery:   "SET PERSIST_ONLY max_connections = 1000;",
			ExpectedConfig: map[string]string{"max_connections": "1000"},
			SelectQuery:    "SELECT @@GLOBAL.max_connections",
			ExpectedRows:   ToSqlRows(MaxConnSchema, NewMaxConnRow(151)),
		},
	}
	for _, test := range persistTests {
		t.Run(test.Name, func(t *testing.T) {
			testPersistQuery(t, test)
		})
	}
}

// Tests the given query on a freshly created dataset, asserting that the result has the given schema and rows. If
// expectedErr is set, asserts instead that the execution returns an error that matches.
func testPersistQuery(t *testing.T, test PersistTest) {
	ctx := context.Background()
	dEnv, err := CreateEmptyTestDatabase()
	require.NoError(t, err)
	defer dEnv.DoltDB(ctx).Close()

	if test.AdditionalSetup != nil {
		test.AdditionalSetup(t, dEnv)
	}

	variables.InitSystemVariables()

	root, _ := dEnv.WorkingRoot(ctx)
	root, err = executeModify(t, ctx, dEnv, root, test.PersistQuery)
	if len(test.ExpectedErr) > 0 {
		require.Error(t, err)
		return
	} else {
		require.NoError(t, err)
	}

	actualRows, _, err := executeSelect(t, ctx, dEnv, root, test.SelectQuery)
	require.NoError(t, err)

	assert.Equal(t, test.ExpectedRows, actualRows)
}
