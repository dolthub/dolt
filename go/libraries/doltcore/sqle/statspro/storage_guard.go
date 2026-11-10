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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// storageGuard decides which stats databases may hold statistics. Once it guards them, statistics stay in memory when
// no stats database passes its check.
type storageGuard interface {
	// note is called with each database whose statistics are collected, and returns true if stats databases must be
	// guarded because of |ddb|.
	note(ddb *doltdb.DoltDB) bool
	// params sets the |params| that a stats database is opened with.
	params(params map[string]interface{}, guarded bool)
	// check returns an error if guarded stats database |ddb| may not hold statistics.
	check(ddb *doltdb.DoltDB) error
}

// newStorageGuard returns the storage guard for the databases of |dEnv|, which may be nil. Dolt Enterprise replaces it.
var newStorageGuard = func(dEnv *env.DoltEnv) storageGuard {
	return plainStorageGuard{}
}

type plainStorageGuard struct{}

func (plainStorageGuard) note(*doltdb.DoltDB) bool {
	return false
}

func (plainStorageGuard) params(map[string]interface{}, bool) {}

func (plainStorageGuard) check(*doltdb.DoltDB) error {
	return nil
}

func (sc *StatsController) noteDBs(ctx context.Context, dbs []sql.Database) {
	for _, db := range dbs {
		if sdb, ok := db.(dsess.SqlDatabase); ok {
			sc.noteDB(ctx, sdb.DbData().Ddb)
		}
	}
}

func (sc *StatsController) noteDB(ctx context.Context, ddb *doltdb.DoltDB) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lockedNoteDB(ctx, ddb)
}

// lockedNoteDB starts guarding stats databases if |ddb| needs it, moving statistics out of one that fails the check.
func (sc *StatsController) lockedNoteDB(ctx context.Context, ddb *doltdb.DoltDB) {
	if sc.guarded || !sc.guard.note(ddb) {
		return
	}
	sc.guarded = true
	p, ok := sc.kv.(*prollyStats)
	if !ok || sc.guard.check(p.ddb) == nil {
		return
	}
	var err error
	if sc.memOnly {
		_, err = sc.lockedStatsToMemory()
		sc.statsBackingDb = nil
	} else {
		err = sc.lockedRotateStorage(ctx)
	}
	if err != nil {
		sc.descError("moving statistics out of a guarded stats database", err)
	}
}
