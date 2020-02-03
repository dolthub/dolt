// Copyright 2020 Liquidata, Inc.
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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"

	. "github.com/liquidata-inc/dolt/go/libraries/doltcore/sql/sqltestutil"
)

func TestInsertIntoQueryCatalogTable(t *testing.T) {
	dEnv := dtestutils.CreateTestEnv()
	CreateTestDatabase(dEnv, t)

	ctx := context.Background()
	root, _ := dEnv.WorkingRoot(ctx)

	_, ok, err := root.GetTable(ctx, doltdb.DoltQueryCatalogTableName)
	require.NoError(t, err)
	require.False(t, ok)

	root, err = NewQueryCatalogEntry(ctx, root,  "name", "select 1 from dual", "description")
	require.NoError(t, err)

	_, ok, err = root.GetTable(ctx, doltdb.DoltQueryCatalogTableName)
	require.NoError(t, err)
	require.True(t, ok)

	rows, err := ExecuteSelect(root, "select display_order, query, name, description from " + doltdb.DoltQueryCatalogTableName)
	require.NoError(t, err)
	expectedRows := []sql.Row {
		{uint64(1), "select 1 from dual", "name", "description"},
	}

	assert.Equal(t, expectedRows, rows)

	root, err = NewQueryCatalogEntry(ctx, root, "name2", "select 2 from dual", "description2")
	require.NoError(t, err)

	rows, err = ExecuteSelect(root, "select display_order, query, name, description from "+ doltdb.DoltQueryCatalogTableName + " order by display_order")
	require.NoError(t, err)
	expectedRows = []sql.Row {
		{uint64(1), "select 1 from dual", "name", "description"},
		{uint64(2), "select 2 from dual", "name2", "description2"},
	}

	assert.Equal(t, expectedRows, rows)

	rows, err = ExecuteSelect(root, "select id from " + doltdb.DoltQueryCatalogTableName)
	require.NoError(t, err)
	for _, r := range rows {
		assert.NotEmpty(t, r)
		assert.NotEmpty(t, r[0])
	}
}