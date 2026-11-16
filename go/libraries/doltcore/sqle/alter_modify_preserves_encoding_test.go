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

package sqle

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/val"
)

// TestAlterModifyTextColumnPreservesEncoding checks tht an ALTER TABLE statement preserves the
// existing encoding for a modified column
func TestAlterModifyTextColumnPreservesEncoding(t *testing.T) {
	ctx := context.Background()

	// First write a text column with an older encoding
	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	_, err := ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL);`)
	require.NoError(t, err)

	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	require.Equal(t, val.StringAddrEnc, descriptionColumnEncoding(t, ctx, root))

	// Then modify it with adaptive encoding enabled
	typeinfo.UseAdaptiveEncoding = true
	_, err = ExecuteSql(ctx, dEnv,
		`ALTER TABLE issues MODIFY COLUMN description LONGTEXT NOT NULL;`)
	require.NoError(t, err)

	root, err = dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	require.Equal(t, val.StringAddrEnc, descriptionColumnEncoding(t, ctx, root))
}

// TestAlterDropColumnPreservesEncoding tests that column encodings don't change as a result of DROP COLUMN statements
func TestAlterDropColumnPreservesEncoding(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	_, err := ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL, notes TEXT);`)
	require.NoError(t, err)

	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	require.Equal(t, val.StringAddrEnc, descriptionColumnEncoding(t, ctx, root))

	typeinfo.UseAdaptiveEncoding = true
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	_, err = ExecuteSql(ctx, dEnv, `ALTER TABLE issues DROP COLUMN notes;`)
	require.NoError(t, err)

	root, err = dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	require.Equal(t, val.StringAddrEnc, descriptionColumnEncoding(t, ctx, root))
}

// descriptionColumnEncoding returns the persisted val.Encoding for the `description`
// column of the `issues` table at the given root.
func descriptionColumnEncoding(t *testing.T, ctx context.Context, root doltdb.RootValue) val.Encoding {
	t.Helper()
	table, _, ok, err := doltdb.GetTableInsensitive(ctx, root, doltdb.TableName{Name: "issues"})
	require.NoError(t, err)
	require.True(t, ok, "table `issues` not found")
	sch, err := table.GetSchema(ctx)
	require.NoError(t, err)
	col, ok := sch.GetAllCols().GetByName("description")
	require.True(t, ok, "column `description` not found")
	return col.TypeInfo.Encoding()
}
