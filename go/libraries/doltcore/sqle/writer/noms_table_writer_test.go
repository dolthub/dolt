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

package writer_test

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

type tableEditorTest struct {
	// The name of this test. Names should be unique and descriptive.
	name string
	// Test setup to run
	setup func(ctx *sql.Context, t *testing.T, ed writer.TableWriter)
	// The select query to run to verify the results
	selectQuery string
	// The rows this query should return, nil if an error is expected
	expectedRows []sql.Row
	// Expected error string, if any
	expectedErr string
}

func TestTableEditor(t *testing.T) {
	if types.Format_Default != types.Format_LD_1 {
		t.Skip()
	}

	edna := sqle.NewPeopleRow(10, "Edna", "Krabapple", false, 38, 8.0)
	krusty := sqle.NewPeopleRow(11, "Krusty", "Klown", false, 48, 9.5)
	smithers := sqle.NewPeopleRow(12, "Waylon", "Smithers", false, 44, 7.1)
	ralph := sqle.NewPeopleRow(13, "Ralph", "Wiggum", false, 9, 9.1)
	martin := sqle.NewPeopleRow(14, "Martin", "Prince", false, 11, 6.3)
	skinner := sqle.NewPeopleRow(15, "Seymore", "Skinner", false, 50, 8.3)
	fatTony := sqle.NewPeopleRow(16, "Fat", "Tony", false, 53, 5.0)
	troyMclure := sqle.NewPeopleRow(17, "Troy", "McClure", false, 58, 7.0)

	var expectedErr error
	// Some of these are pretty exotic use cases, but since we support all these operations it's nice to know they work
	// in tandem.
	testCases := []tableEditorTest{
		{
			name: "all inserts",
			setup: func(ctx *sql.Context, t *testing.T, ed writer.TableWriter) {
				require.NoError(t, ed.Insert(ctx, r(edna, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(krusty, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(smithers, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(ralph, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(martin, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(skinner, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(fatTony, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(troyMclure, sqle.PeopleTestSchema)))
			},
			selectQuery: "select * from people where id >= 10 ORDER BY id",
			expectedRows: sqle.ToSqlRows(sqle.PeopleTestSchema,
				edna, krusty, smithers, ralph, martin, skinner, fatTony, troyMclure,
			),
		},
		{
			name: "inserts and deletes",
			setup: func(ctx *sql.Context, t *testing.T, ed writer.TableWriter) {
				require.NoError(t, ed.Insert(ctx, r(edna, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(krusty, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Delete(ctx, r(edna, sqle.PeopleTestSchema)))
			},
			selectQuery: "select * from people where id >= 10 ORDER BY id",
			expectedRows: sqle.ToSqlRows(sqle.PeopleTestSchema,
				krusty,
			),
		},
		{
			name: "inserts and deletes 2",
			setup: func(ctx *sql.Context, t *testing.T, ed writer.TableWriter) {
				require.NoError(t, ed.Insert(ctx, r(edna, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(krusty, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Delete(ctx, r(edna, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(fatTony, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Delete(ctx, r(sqle.Homer, sqle.PeopleTestSchema)))
			},
			selectQuery: "select * from people where id >= 10 or id = 0 ORDER BY id",
			expectedRows: sqle.ToSqlRows(sqle.PeopleTestSchema,
				krusty, fatTony,
			),
		},
		{
			name: "inserts and updates",
			setup: func(ctx *sql.Context, t *testing.T, ed writer.TableWriter) {
				require.NoError(t, ed.Insert(ctx, r(edna, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(krusty, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Update(ctx, r(edna, sqle.PeopleTestSchema), r(sqle.MutateRow(sqle.PeopleTestSchema, edna, sqle.AgeTag, 1), sqle.PeopleTestSchema)))
			},
			selectQuery: "select * from people where id >= 10 ORDER BY id",
			expectedRows: sqle.ToSqlRows(sqle.PeopleTestSchema,
				sqle.MutateRow(sqle.PeopleTestSchema, edna, sqle.AgeTag, 1),
				krusty,
			),
		},
		{
			name: "inserts updates and deletes",
			setup: func(ctx *sql.Context, t *testing.T, ed writer.TableWriter) {
				require.NoError(t, ed.Insert(ctx, r(edna, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(krusty, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Update(ctx, r(edna, sqle.PeopleTestSchema), r(sqle.MutateRow(sqle.PeopleTestSchema, edna, sqle.AgeTag, 1), sqle.PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(smithers, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(ralph, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Update(ctx, r(smithers, sqle.PeopleTestSchema), r(sqle.MutateRow(sqle.PeopleTestSchema, smithers, sqle.AgeTag, 1), sqle.PeopleTestSchema)))
				require.NoError(t, ed.Delete(ctx, r(smithers, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(skinner, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Delete(ctx, r(ralph, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(ralph, sqle.PeopleTestSchema)))
			},
			selectQuery: "select * from people where id >= 10 ORDER BY id",
			expectedRows: sqle.ToSqlRows(sqle.PeopleTestSchema,
				sqle.MutateRow(sqle.PeopleTestSchema, edna, sqle.AgeTag, 1),
				krusty,
				ralph,
				skinner,
			),
		},
		{
			name: "inserts and updates to primary key",
			setup: func(ctx *sql.Context, t *testing.T, ed writer.TableWriter) {
				require.NoError(t, ed.Insert(ctx, r(edna, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Insert(ctx, r(krusty, sqle.PeopleTestSchema)))
				require.NoError(t, ed.Update(ctx, r(edna, sqle.PeopleTestSchema), r(sqle.MutateRow(sqle.PeopleTestSchema, edna, sqle.IdTag, 30), sqle.PeopleTestSchema)))
			},
			selectQuery: "select * from people where id >= 10 ORDER BY id",
			expectedRows: sqle.ToSqlRows(sqle.PeopleTestSchema,
				krusty,
				sqle.MutateRow(sqle.PeopleTestSchema, edna, sqle.IdTag, 30),
			),
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			expectedErr = nil

			dEnv, err := sqle.CreateTestDatabase()
			require.NoError(t, err)

			ctx := sqle.NewTestSQLCtx(context.Background())
			root, err := dEnv.WorkingRoot(context.Background())
			require.NoError(t, err)

			tmpDir, err := dEnv.TempTableFilesDir()
			require.NoError(t, err)
			opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: tmpDir}
			db, err := sqle.NewDatabase(ctx, "dolt", dEnv.DbData(), opts)
			require.NoError(t, err)

			err = dsess.DSessFromSess(ctx.Session).AddDB(ctx, getDbState(t, db, dEnv))
			require.NoError(t, err)

			ctx.SetCurrentDatabase(db.Name())
			err = db.SetRoot(ctx, root)
			require.NoError(t, err)
			peopleTable, _, err := db.GetTableInsensitive(ctx, "people")
			require.NoError(t, err)

			dt := peopleTable.(sql.UpdatableTable)
			ed := dt.Updater(ctx).(writer.TableWriter)

			test.setup(ctx, t, ed)
			if len(test.expectedErr) > 0 {
				require.Error(t, expectedErr)
				assert.Contains(t, expectedErr.Error(), test.expectedErr)
				return
			} else {
				require.NoError(t, ed.Close(ctx))
			}

			root, err = db.GetRoot(ctx)
			require.NoError(t, err)

			require.NoError(t, dEnv.UpdateWorkingRoot(context.Background(), root))

			actualRows, err := sqle.ExecuteSelect(dEnv, root, test.selectQuery)
			require.NoError(t, err)

			assert.Equal(t, test.expectedRows, actualRows)
		})
	}
}

func r(r row.Row, sch schema.Schema) sql.Row {
	sqlRow, err := sqlutil.DoltRowToSqlRow(r, sch)
	if err != nil {
		panic(err)
	}
	return sqlRow
}

func getDbState(t *testing.T, db sql.Database, dEnv *env.DoltEnv) dsess.InitialDbState {
	ctx := context.Background()

	head := dEnv.RepoStateReader().CWBHeadSpec()
	headCommit, err := dEnv.DoltDB.Resolve(ctx, head, dEnv.RepoStateReader().CWBHeadRef())
	require.NoError(t, err)

	ws, err := dEnv.WorkingSet(ctx)
	require.NoError(t, err)

	return dsess.InitialDbState{
		Db:         db,
		HeadCommit: headCommit,
		WorkingSet: ws,
		DbData:     dEnv.DbData(),
		Remotes:    dEnv.RepoState.Remotes,
	}
}
