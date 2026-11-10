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

package binlogreplication

import (
	"context"
	"errors"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

func TestCheckDatabases(t *testing.T) {
	errToy := errors.New("toy")
	var checked []string
	prev := checkDatabase
	t.Cleanup(func() { checkDatabase = prev })
	checkDatabase = func(name string, ddb *doltdb.DoltDB) error {
		checked = append(checked, name)
		return errToy
	}

	ctx := context.Background()
	dEnv := sqle.CreateTestEnv()
	t.Cleanup(func() { dEnv.Close() })
	db, err := sqle.NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)
	pro, err := sqle.NewDoltDatabaseProviderWithDatabase("main", dEnv.FS, db, dEnv.FS, sql.EngineOverrides{})
	require.NoError(t, err)

	assert.ErrorIs(t, CheckDatabases(pro), errToy)
	assert.Equal(t, []string{"dolt"}, checked)

	assert.ErrorIs(t, CheckDatabaseHook(sql.NewEmptyContext(), pro, "created", dEnv, db), errToy)
	assert.Equal(t, []string{"dolt", "created"}, checked)
}
