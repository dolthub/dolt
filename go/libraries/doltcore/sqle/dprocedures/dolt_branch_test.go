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

package dprocedures

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
)

func TestLoadConfigDoesNotOpenDatabase(t *testing.T) {
	t.Setenv(dconfig.EnvDoltRootPath, t.TempDir())
	repoDir := t.TempDir()
	t.Chdir(repoDir)

	nomsDir := filepath.Join(repoDir, dbfactory.DoltDataDir)
	require.NoError(t, os.MkdirAll(nomsDir, 0o755))
	localConfig := filepath.Join(repoDir, dbfactory.DoltDir, "config.json")
	require.NoError(t, os.WriteFile(localConfig, []byte(`{"branch.autosetupmerge":"false"}`), 0o644))

	cfg := loadConfig(sql.NewEmptyContext())
	require.Equal(t, "false", cfg.GetStringOrDefault("branch.autosetupmerge", ""))

	// Opening a store would write its lock file and manifest here.
	entries, err := os.ReadDir(nomsDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}
