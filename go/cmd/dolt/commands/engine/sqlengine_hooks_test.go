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

package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/mysql_file_handler"
	"github.com/dolthub/dolt/go/libraries/utils/config"
)

func TestSqlEngineHooksAreReached(t *testing.T) {
	ctx := context.Background()
	mrEnv, cfg, _ := lifecycleEngineEnvironment(t, ctx, t.TempDir(), t.TempDir())
	savedPersister, savedBranchControl := newPrivilegePersister, loadBranchControl
	t.Cleanup(func() { newPrivilegePersister, loadBranchControl = savedPersister, savedBranchControl })
	var configs []config.ReadableConfig
	newPrivilegePersister = func(c config.ReadableConfig, privFilePath, doltCfgDirPath string) *mysql_file_handler.Persister {
		configs = append(configs, c)
		return savedPersister(c, privFilePath, doltCfgDirPath)
	}
	loadBranchControl = func(ctx context.Context, c config.ReadableConfig, branchCtrlFilePath, doltCfgDirPath string) (*branch_control.Controller, error) {
		configs = append(configs, c)
		return savedBranchControl(ctx, c, branchCtrlFilePath, doltCfgDirPath)
	}

	se, err := NewSqlEngine(ctx, mrEnv, cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, se.Close()) }()
	require.Equal(t, []config.ReadableConfig{mrEnv.Config(), mrEnv.Config()}, configs)
}
