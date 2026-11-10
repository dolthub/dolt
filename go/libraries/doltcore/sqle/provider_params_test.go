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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/config"
)

func TestProviderCreateDatabaseWithParams(t *testing.T) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()
	pro, err := NewDoltDatabaseProvider("main", dEnv.FS, sql.EngineOverrides{})
	require.NoError(t, err)
	parent := config.NewMapConfig(map[string]string{"k": "v"})
	pro.SetParentConfig(parent)
	cfg, _ := dEnv.Config.GetConfig(env.GlobalConfig)
	sqlCtx := NewTestSQLCtxWithProvider(ctx, pro, cfg, nil, nil)

	var created *env.DoltEnv
	pro.AddInitDatabaseHook(func(_ *sql.Context, _ *DoltDatabaseProvider, _ string, denv *env.DoltEnv, _ dsess.SqlDatabase) error {
		created = denv
		return nil
	})
	require.NoError(t, pro.CreateDatabaseWithParams(sqlCtx, "with_params", map[string]string{"toy": "1"}))
	require.NotNil(t, created)
	assert.Equal(t, "1", created.DBLoadParams["toy"], "the database loads with its params")
	assert.Same(t, parent, created.ParentConfig)
}
