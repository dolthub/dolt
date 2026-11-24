// Copyright 2026 Dolthub, Inc.
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

package dtables_test

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
)

type staticRoots doltdb.Roots

func (r staticRoots) GetRoots(*sql.Context) (doltdb.Roots, error) {
	return doltdb.Roots(r), nil
}

// TestIgnoredNewTablesMatchesEachSchemaAgainstItsOwnPatterns builds a root whose default schema
// ignores keep_* (and has a conflicting pair of patterns), whose schema |other| additionally ignores
// scratch_*, and in which schema |missing| has no dolt_ignore table.
func TestIgnoredNewTablesMatchesEachSchemaAgainstItsOwnPatterns(t *testing.T) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	_, err := sqle.ExecuteSql(ctx, dEnv, "INSERT INTO dolt_ignore VALUES "+
		"('keep_*', true), ('*_ignore', true), ('commit_*', false);\n"+
		"CREATE TABLE new_table (pk int primary key);")
	require.NoError(t, err)
	defaultRoot, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	defaultIgnore, ok, err := defaultRoot.GetTable(ctx, doltdb.TableName{Name: doltdb.IgnoreTableName})
	require.NoError(t, err)
	require.True(t, ok)
	newTable, ok, err := defaultRoot.GetTable(ctx, doltdb.TableName{Name: "new_table"})
	require.NoError(t, err)
	require.True(t, ok)

	root, err := sqle.ExecuteSql(ctx, dEnv, "INSERT INTO dolt_ignore VALUES ('scratch_*', true);")
	require.NoError(t, err)
	otherIgnore, ok, err := root.GetTable(ctx, doltdb.TableName{Name: doltdb.IgnoreTableName})
	require.NoError(t, err)
	require.True(t, ok)
	root, err = root.PutTable(ctx, doltdb.TableName{Name: doltdb.IgnoreTableName}, defaultIgnore)
	require.NoError(t, err)
	root, err = root.CreateDatabaseSchema(ctx, schema.DatabaseSchema{Name: "other"})
	require.NoError(t, err)
	root, err = root.PutTable(ctx, doltdb.TableName{Name: doltdb.IgnoreTableName, Schema: "other"}, otherIgnore)
	require.NoError(t, err)
	rp := staticRoots{Head: root, Staged: root, Working: root}

	added := func(schemaName, tableName string) diff.TableDelta {
		return diff.TableDelta{ToName: doltdb.TableName{Name: tableName, Schema: schemaName}, ToTable: newTable}
	}
	modified := added("other", "scratch_modified")
	modified.FromName = modified.ToName
	modified.FromTable = newTable

	deltas := []diff.TableDelta{
		added("", "keep_one"),
		added("", "scratch_one"),
		added("other", "scratch_one"),
		added("other", "keep_one"),
		added("missing", "keep_one"),
		modified,
	}
	sqlCtx := sql.NewEmptyContext()

	ignored, err := dtables.IgnoredNewTables(sqlCtx, rp, deltas, false)
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{"keep_one": true, "other.scratch_one": true, "other.keep_one": true}, ignored)

	conflicting := append(deltas, added("", "commit_ignore"))
	_, err = dtables.IgnoredNewTables(sqlCtx, rp, conflicting, false)
	require.Error(t, err)
	assert.NotNil(t, doltdb.AsDoltIgnoreInConflict(err))

	ignored, err = dtables.IgnoredNewTables(sqlCtx, rp, conflicting, true)
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{"keep_one": true, "other.scratch_one": true, "other.keep_one": true}, ignored)
}
