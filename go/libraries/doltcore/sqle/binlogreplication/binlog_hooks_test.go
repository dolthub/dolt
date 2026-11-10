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
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
)

func TestCheckLoggableDatabaseIsReached(t *testing.T) {
	ctx := context.Background()
	refused := errors.New("refused")
	saved := checkLoggableDatabase
	t.Cleanup(func() { checkLoggableDatabase = saved })
	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()
	var checked *doltdb.DoltDB
	checkLoggableDatabase = func(name string, ddb *doltdb.DoltDB) error {
		require.Equal(t, "db", name)
		checked = ddb
		return refused
	}

	err := NewBinlogInitDatabaseHook(ctx, nil)(sql.NewEmptyContext(), nil, "db", dEnv, nil)
	require.ErrorIs(t, err, refused)
	require.Same(t, dEnv.DoltDB(ctx), checked)
}
