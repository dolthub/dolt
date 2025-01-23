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

package dtables_test

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
)

func TestInsertIntoQueryCatalogTable(t *testing.T) {
	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()

	ctx := context.Background()
	root, _ := dEnv.WorkingRoot(ctx)

	_, ok, err := root.GetTable(ctx, doltdb.TableName{Name: doltdb.DoltQueryCatalogTableName})
	require.NoError(t, err)
	require.False(t, ok)

	queryStr := "select 1 from dual"
	sq, root, err := dtables.NewQueryCatalogEntryWithRandID(ctx, root, "name", queryStr, "description")
	require.NoError(t, err)
	require.True(t, sq.ID != "")
	assert.Equal(t, queryStr, sq.Query)
	assert.Equal(t, "name", sq.Name)
	assert.Equal(t, "description", sq.Description)

	retrieved, err := dtables.RetrieveFromQueryCatalog(ctx, root, sq.ID)
	require.NoError(t, err)
	assert.Equal(t, sq, retrieved)

	_, ok, err = root.GetTable(ctx, doltdb.TableName{Name: doltdb.DoltQueryCatalogTableName})
	require.NoError(t, err)
	require.True(t, ok)

	err = dEnv.UpdateWorkingRoot(ctx, root)
	require.NoError(t, err)

	rows, err := sqle.ExecuteSelect(ctx, dEnv, root, "select display_order, query, name, description from "+doltdb.DoltQueryCatalogTableName)
	require.NoError(t, err)
	expectedRows := []sql.Row{
		{uint64(1), "select 1 from dual", "name", "description"},
	}

	assert.Equal(t, expectedRows, rows)

	queryStr2 := "select 2 from dual"
	sq2, root, err := dtables.NewQueryCatalogEntryWithNameAsID(ctx, root, "name2", queryStr2, "description2")
	require.NoError(t, err)
	assert.Equal(t, "name2", sq2.ID)
	assert.Equal(t, "name2", sq2.Name)
	assert.Equal(t, queryStr2, sq2.Query)
	assert.Equal(t, "description2", sq2.Description)

	retrieved2, err := dtables.RetrieveFromQueryCatalog(ctx, root, sq2.ID)
	require.NoError(t, err)
	assert.Equal(t, sq2, retrieved2)

	err = dEnv.UpdateWorkingRoot(ctx, root)
	require.NoError(t, err)

	rows, err = sqle.ExecuteSelect(ctx, dEnv, root, "select display_order, query, name, description from "+doltdb.DoltQueryCatalogTableName+" order by display_order")
	require.NoError(t, err)
	expectedRows = []sql.Row{
		{uint64(1), "select 1 from dual", "name", "description"},
		{uint64(2), "select 2 from dual", "name2", "description2"},
	}

	assert.Equal(t, expectedRows, rows)

	rows, err = sqle.ExecuteSelect(ctx, dEnv, root, "select id from "+doltdb.DoltQueryCatalogTableName)
	require.NoError(t, err)
	for _, r := range rows {
		assert.NotEmpty(t, r)
		assert.NotEmpty(t, r[0])
	}

	queryStr3 := "select 3 from dual"
	sq3, root, err := dtables.NewQueryCatalogEntryWithNameAsID(ctx, root, "name2", queryStr3, "description3")
	require.NoError(t, err)
	assert.Equal(t, "name2", sq3.ID)
	assert.Equal(t, "name2", sq3.Name)
	assert.Equal(t, queryStr3, sq3.Query)
	assert.Equal(t, "description3", sq3.Description)
	assert.Equal(t, sq2.Order, sq3.Order)
}
