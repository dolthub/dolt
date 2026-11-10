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

package statspro

import (
	"context"
	"sync"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

type recordingGuard struct {
	mu    sync.Mutex
	calls map[string]bool
}

func (g *recordingGuard) record(call string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls[call] = true
}

func (g *recordingGuard) useEnv(*env.DoltEnv) {
	g.record("useEnv")
}

func (g *recordingGuard) memOnly() bool {
	g.record("memOnly")
	return false
}

func (g *recordingGuard) noteDBs(context.Context, *StatsController, []sql.Database) {
	g.record("noteDBs")
}

func (g *recordingGuard) noteDB(context.Context, *StatsController, *doltdb.DoltDB) {
	g.record("noteDB")
}

func (g *recordingGuard) lockedNoteDB(context.Context, *StatsController, *doltdb.DoltDB) {
	g.record("lockedNoteDB")
}

func (g *recordingGuard) lockedRotateStorage(ctx context.Context, sc *StatsController) error {
	g.record("lockedRotateStorage")
	return sc.lockedRotateStorage(ctx)
}

func (g *recordingGuard) storageParams(map[string]interface{}) {
	g.record("storageParams")
}

func (g *recordingGuard) checkStorage(*doltdb.DoltDB) error {
	g.record("checkStorage")
	return nil
}

func TestStorageGuardIsReached(t *testing.T) {
	g := &recordingGuard{calls: make(map[string]bool)}
	saved := newStorageGuard
	t.Cleanup(func() { newStorageGuard = saved })
	newStorageGuard = func(*env.DoltEnv) storageGuard { return g }

	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, _ := defaultSetupDetail(t, threads, false, false, true)
	require.NoError(t, executeQuery(ctx, sqlEng, "analyze table xy"))

	sc := NewStatsController(logrus.StandardLogger(), threads, nil)
	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()
	require.NoError(t, NewInitDatabaseHook(sc)(ctx, nil, "db", dEnv, nil))

	for _, call := range []string{"useEnv", "memOnly", "noteDBs", "noteDB", "lockedNoteDB", "lockedRotateStorage", "storageParams", "checkStorage"} {
		require.True(t, g.calls[call], call)
	}
}
