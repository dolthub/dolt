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
	"errors"
	"sync"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

// toyGuard guards stats databases once |noting| is set, and refuses all of them while |refuse| is set.
type toyGuard struct {
	mu      sync.Mutex
	noting  bool
	refuse  bool
	guarded []bool
}

var errToyGuard = errors.New("toy guard")

func (g *toyGuard) note(*doltdb.DoltDB) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.noting
}

func (g *toyGuard) params(params map[string]interface{}, guarded bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.guarded = append(g.guarded, guarded)
	params["toy_guard"] = guarded
}

func (g *toyGuard) check(*doltdb.DoltDB) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.refuse {
		return errToyGuard
	}
	return nil
}

func withStorageGuard(t *testing.T, g storageGuard) {
	prev := newStorageGuard
	t.Cleanup(func() { newStorageGuard = prev })
	newStorageGuard = func(*env.DoltEnv) storageGuard { return g }
}

func TestStorageGuardKeepsStatsInMemory(t *testing.T) {
	g := &toyGuard{}
	withStorageGuard(t, g)
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, _, sc := defaultSetup(t, threads, false, false)

	prev, ok := sc.kv.(*prollyStats)
	require.True(t, ok, "an unguarded stats database holds statistics")
	require.NotContains(t, g.guarded, true)

	sc.noteDB(ctx, prev.ddb)
	_, ok = sc.kv.(*prollyStats)
	assert.True(t, ok, "a stats database that passes the check is kept")

	g.mu.Lock()
	g.noting, g.refuse = true, true
	g.mu.Unlock()
	sc.noteDB(ctx, prev.ddb)
	assert.True(t, sc.guarded)
	assert.Same(t, prev.mem, sc.kv, "statistics move to memory when no stats database passes the check")
	assert.True(t, sc.memOnly)
	assert.Nil(t, sc.statsBackingDb)
	assert.Equal(t, 4, sc.kv.Len())
	assert.Contains(t, g.guarded, true, "a guarded stats database is opened with the guard's params")
}

func TestStorageGuardRefusesStoredStats(t *testing.T) {
	g := &toyGuard{noting: true, refuse: true}
	withStorageGuard(t, g)
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	_, _, sc := emptySetup(t, threads, false, false)

	assert.True(t, sc.guarded)
	_, isMem := sc.kv.(*memStats)
	assert.True(t, isMem)
	assert.True(t, sc.memOnly)
}
