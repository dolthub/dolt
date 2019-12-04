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

package sqle

import (
	"context"
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	. "github.com/liquidata-inc/dolt/go/libraries/doltcore/sql/sqltestutil"
)

type tableEditorTest struct {
	// The name of this test. Names should be unique and descriptive.
	name string
	// Test setup to run
	setup func(ctx *sql.Context, t *testing.T, ed *tableEditor)
	// The select query to run to verify the results
	selectQuery string
	// The rows this query should return, nil if an error is expected
	expectedRows []row.Row
	// Expected error string, if any
	expectedErr string
}

func TestTableEditor(t *testing.T) {
	edna := NewPeopleRow(10, "Edna", "Krabapple", false, 38, 8.0)
	krusty := NewPeopleRow(11, "Krusty", "Klown", false, 48, 9.5)
	smithers := NewPeopleRow(12, "Waylon", "Smithers", false, 44, 7.1)
	ralph := NewPeopleRow(13, "Ralph", "Wiggum", false, 9, 9.1)
	martin := NewPeopleRow(14, "Martin", "Prince", false, 11, 6.3)
	skinner := NewPeopleRow(15, "Seymore", "Skinner", false, 50, 8.3)
	fatTony := NewPeopleRow(16, "Fat", "Tony", false, 53, 5.0)
	troyMclure := NewPeopleRow(17, "Troy", "McClure", false, 58, 7.0)

	var expectedErr error
	// Some of these are pretty exotic use cases, but since we support all these operations it's nice to know they work
	// in tandem.
	testCases := []tableEditorTest{
		{
			name: "all inserts",
			setup: func(ctx *sql.Context, t *testing.T, ed *tableEditor) {
				require.NoError(t, ed.Insert(ctx, r(edna, PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(krusty, PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(smithers, PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(ralph, PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(martin, PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(skinner, PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(fatTony, PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(troyMclure, PeopleTestSchema)))
			},
			selectQuery: "select * from people where id >= 10",
			expectedRows: CompressRows(PeopleTestSchema,
				edna, krusty, smithers, ralph, martin, skinner, fatTony, troyMclure,
			),
		},
		{
			name: "inserts and deletes",
			setup: func(ctx *sql.Context, t *testing.T, ed *tableEditor) {
				require.NoError(t, ed.Insert(ctx, r(edna, PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(krusty, PeopleTestSchema)))
				require.NoError(t, ed.Delete(ctx, r(edna, PeopleTestSchema)))
			},
			selectQuery: "select * from people where id >= 10",
			expectedRows: CompressRows(PeopleTestSchema,
				krusty,
			),
		},
		{
			name: "inserts and deletes 2",
			setup: func(ctx *sql.Context, t *testing.T, ed *tableEditor) {
				require.NoError(t, ed.Insert(ctx, r(edna, PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(krusty, PeopleTestSchema)))
				require.NoError(t, ed.Delete(ctx, r(edna, PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(fatTony, PeopleTestSchema)))
				require.NoError(t, ed.Delete(ctx, r(Homer, PeopleTestSchema)))
			},
			selectQuery: "select * from people where id >= 10 or id = 0",
			expectedRows: CompressRows(PeopleTestSchema,
				krusty, fatTony,
			),
		},
		{
			name: "inserts and updates",
			setup: func(ctx *sql.Context, t *testing.T, ed *tableEditor) {
				require.NoError(t, ed.Insert(ctx, r(edna, PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(krusty, PeopleTestSchema)))
				require.NoError(t, ed.Update(ctx, r(edna, PeopleTestSchema), r(MutateRow(edna, AgeTag, 1), PeopleTestSchema)))
			},
			selectQuery: "select * from people where id >= 10",
			expectedRows: CompressRows(PeopleTestSchema,
				MutateRow(edna, AgeTag, 1),
				krusty,
			),
		},
		{
			name: "inserts updates and deletes",
			setup: func(ctx *sql.Context, t *testing.T, ed *tableEditor) {
				require.NoError(t, ed.Insert(ctx, r(edna, PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(krusty, PeopleTestSchema)))
				require.NoError(t, ed.Update(ctx, r(edna, PeopleTestSchema), r(MutateRow(edna, AgeTag, 1), PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(smithers, PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(ralph, PeopleTestSchema)))
				require.NoError(t, ed.Update(ctx, r(smithers, PeopleTestSchema), r(MutateRow(smithers, AgeTag, 1), PeopleTestSchema)))
				require.NoError(t, ed.Delete(ctx, r(smithers, PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(skinner, PeopleTestSchema)))
				require.NoError(t, ed.Delete(ctx, r(ralph, PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(ralph, PeopleTestSchema)))
			},
			selectQuery: "select * from people where id >= 10",
			expectedRows: CompressRows(PeopleTestSchema,
				MutateRow(edna, AgeTag, 1),
				krusty,
				ralph,
				skinner,
			),
		},
		{
			name: "inserts and updates to primary key",
			setup: func(ctx *sql.Context, t *testing.T, ed *tableEditor) {
				require.NoError(t, ed.Insert(ctx, r(edna, PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(krusty, PeopleTestSchema)))
				require.NoError(t, ed.Update(ctx, r(edna, PeopleTestSchema), r(MutateRow(edna, IdTag, 30), PeopleTestSchema)))
			},
			selectQuery: "select * from people where id >= 10",
			expectedRows: CompressRows(PeopleTestSchema,
				krusty,
				MutateRow(edna, IdTag, 30),
			),
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			expectedErr = nil

			dEnv := dtestutils.CreateTestEnv()
			CreateTestDatabase(dEnv, t)

			ctx := sql.NewEmptyContext()
			root, _ := dEnv.WorkingRoot(context.Background())
			db := NewDatabase("dolt", root, dEnv)
			peopleTable, _, err := db.GetTableInsensitive(ctx, "people")
			require.NoError(t, err)

			dt := peopleTable.(sql.UpdatableTable)
			ed := dt.Updater(ctx).(*tableEditor)

			test.setup(ctx, t, ed)
			if len(test.expectedErr) > 0 {
				require.Error(t, expectedErr)
				assert.Contains(t, expectedErr.Error(), test.expectedErr)
				return
			} else {
				require.NoError(t, ed.Close(ctx))
			}

			root = db.Root()
			actualRows, _, err := executeSelect(context.Background(), dEnv, CompressSchema(PeopleTestSchema), root, test.selectQuery)
			require.NoError(t, err)
			assert.Equal(t, test.expectedRows, actualRows)
		})
	}
}

func r(row row.Row, sch schema.Schema) sql.Row {
	sqlRow, err := doltRowToSqlRow(row, sch)
	if err != nil {
		panic(err)
	}
	return sqlRow
}
