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
)

// storageGuard decides where a StatsController may keep statistics, which hold row values.
type storageGuard interface {
	// useEnv takes the settings of |dEnv|, whose databases the controller serves.
	useEnv(dEnv *env.DoltEnv)
	// memOnly reports whether statistics must stay in memory.
	memOnly() bool
	// noteDBs and noteDB see databases whose statistics the controller keeps; lockedNoteDB is noteDB with sc.mu held.
	noteDBs(ctx context.Context, sc *StatsController, dbs []sql.Database)
	noteDB(ctx context.Context, sc *StatsController, ddb *doltdb.DoltDB)
	lockedNoteDB(ctx context.Context, sc *StatsController, ddb *doltdb.DoltDB)
	// lockedRotateStorage replaces the stats database of |sc|. Callers hold sc.mu, except Init, which runs before the
	// controller's worker starts.
	lockedRotateStorage(ctx context.Context, sc *StatsController) error
	// storageParams sets the params that open a stats database. checkStorage refuses stats database |ddb|, and
	// closes it if it does. Both run with or without sc.mu held.
	storageParams(params map[string]interface{})
	checkStorage(ddb *doltdb.DoltDB) error
}

// newStorageGuard returns the storage guard of a controller for the databases of |dEnv|, which may be nil.
var newStorageGuard = func(dEnv *env.DoltEnv) storageGuard {
	return anyStorage{}
}

// anyStorage keeps statistics wherever the controller does.
type anyStorage struct{}

func (anyStorage) useEnv(*env.DoltEnv) {}

func (anyStorage) memOnly() bool {
	return false
}

func (anyStorage) noteDBs(context.Context, *StatsController, []sql.Database) {}

func (anyStorage) noteDB(context.Context, *StatsController, *doltdb.DoltDB) {}

func (anyStorage) lockedNoteDB(context.Context, *StatsController, *doltdb.DoltDB) {}

func (anyStorage) lockedRotateStorage(ctx context.Context, sc *StatsController) error {
	return sc.lockedRotateStorage(ctx)
}

func (anyStorage) storageParams(map[string]interface{}) {}

func (anyStorage) checkStorage(*doltdb.DoltDB) error {
	return nil
}
