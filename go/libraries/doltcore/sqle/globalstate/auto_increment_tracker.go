// Copyright 2021 Dolthub, Inc.
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

package globalstate

import (
	"context"
	"math"
	"strings"
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

type AutoIncrementTracker struct {
	dbName string
	sequences map[string]uint64
	mu        *sync.Mutex
}

// NewAutoIncrementTracker returns a new autoincrement tracker for the roots given. All roots sets must be
// considered because the auto increment value for a table is tracked globally, across all branches.
// Roots provided should be the working sets when available, or the branches when they are not (e.g. for remote
// branches that don't have a local working set)
func NewAutoIncrementTracker(ctx context.Context, dbName string, roots ...doltdb.Rootish) (AutoIncrementTracker, error) {
	ait := AutoIncrementTracker{
		dbName:    dbName,
		sequences: make(map[string]uint64),
		mu:        &sync.Mutex{},
	}

	for _, root := range roots {
		root, err := root.ResolveRootValue(ctx)
		if err != nil {
			return AutoIncrementTracker{}, err
		}

		err = root.IterTables(ctx, func(tableName string, table *doltdb.Table, sch schema.Schema) (bool, error) {
			ok := schema.HasAutoIncrement(sch)
			if !ok {
				return false, nil
			}

			tableName = strings.ToLower(tableName)

			seq, err := table.GetAutoIncrementValue(ctx)
			if err != nil {
				return true, err
			}

			if seq > ait.sequences[tableName] {
				ait.sequences[tableName] = seq
			}

			return false, nil
		})

		if err != nil {
			return AutoIncrementTracker{}, err
		}
	}

	return ait, nil
}

// Current returns the next value to be generated in the auto increment sequence for the table named
func (a AutoIncrementTracker) Current(tableName string) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sequences[strings.ToLower(tableName)]
}

// Next returns the next auto increment value for the table named using the provided value from an insert (which may
// be null or 0, in which case it will be generated from the sequence).
func (a AutoIncrementTracker) Next(tbl string, insertVal interface{}) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	tbl = strings.ToLower(tbl)

	given, err := CoerceAutoIncrementValue(insertVal)
	if err != nil {
		return 0, err
	}

	curr := a.sequences[tbl]

	if given == 0 {
		// |given| is 0 or NULL
		a.sequences[tbl]++
		return curr, nil
	}

	if given >= curr {
		a.sequences[tbl] = given
		a.sequences[tbl]++
		return given, nil
	}

	// |given| < curr
	return given, nil
}

// CoerceAutoIncrementValue converts |val| into an AUTO_INCREMENT sequence value
func CoerceAutoIncrementValue(val interface{}) (uint64, error) {
	switch typ := val.(type) {
	case float32:
		val = math.Round(float64(typ))
	case float64:
		val = math.Round(typ)
	}

	var err error
	val, _, err = types.Uint64.Convert(val)
	if err != nil {
		return 0, err
	}
	if val == nil || val == uint64(0) {
		return 0, nil
	}
	return val.(uint64), nil
}

// Set sets the auto increment value for the table named, if it's greater than the one already registered for this
// table. Otherwise, the update is silently disregarded. So far this matches the MySQL behavior, but Dolt uses the
// maximum value for this table across all branches.
func (a AutoIncrementTracker) Set(ctx *sql.Context, tableName string, newAutoIncVal uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	tableName = strings.ToLower(tableName)

	existing := a.sequences[tableName]
	if newAutoIncVal > existing {
		a.sequences[strings.ToLower(tableName)] = newAutoIncVal
	} else {
		// re-establish our baseline for this table on all branches before making our decision 
		return a.deepSet(ctx, tableName, newAutoIncVal)
	}

	return nil
}

// deepSet sets the auto increment value for the table named, if it's greater than the one on any branch head for this 
// database, ignoring the current in-memory tracker value
func (a AutoIncrementTracker) deepSet(ctx *sql.Context, tableName string, newAutoIncVal uint64) error {
	sess := dsess.DSessFromSess(ctx.Session)
	db, ok := sess.Provider().BaseDatabase(ctx, a.dbName)

	// just give up if we can't find this db for any reason, or it's a non-versioned DB
	if !ok || !db.Versioned() {
		return nil
	}

	doltdbs := db.DoltDatabases()
	for _, db := range doltdbs {
		branches, err := db.GetBranches(ctx)
		if err != nil {
			return err
		}

		remotes, err := db.GetRemoteRefs(ctx)
		if err != nil {
			return err
		}

		rootRefs := make([]ref.DoltRef, 0, len(branches)+len(remotes))
		rootRefs = append(rootRefs, branches...)
		rootRefs = append(rootRefs, remotes...)

		var roots []doltdb.Rootish
		for _, b := range rootRefs {
			switch b.GetType() {
			case ref.BranchRefType:
				wsRef, err := ref.WorkingSetRefForHead(b)
				if err != nil {
					return err
				}

				ws, err := db.ResolveWorkingSet(ctx, wsRef)
				if err == doltdb.ErrWorkingSetNotFound {
					// use the branch head if there isn't a working set for it
					cm, err := db.ResolveCommitRef(ctx, b)
					if err != nil {
						return err
					}
					roots = append(roots, cm)
				} else if err != nil {
					return err
				} else {
					roots = append(roots, ws)
				}
			case ref.RemoteRefType:
				cm, err := db.ResolveCommitRef(ctx, b)
				if err != nil {
					return err
				}
				roots = append(roots, cm)
			}
		}

		for _, root := range roots {
			root, err := root.ResolveRootValue(ctx)
			if err != nil {
				return err
			}

			table, _, ok, err := root.GetTableInsensitive(ctx, tableName)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}

			sch, err := table.GetSchema(ctx)
			if err != nil {
				return err
			}

			if !schema.HasAutoIncrement(sch) {
				continue
			}

			tableName = strings.ToLower(tableName)
			seq, err := table.GetAutoIncrementValue(ctx)
			if err != nil {
				return err
			}

			// There's a value on this branch higher than the one given, so we can't use it
			if seq >= newAutoIncVal {
				return nil
			}
		}
	}

	// If we made it through the above loop, that means there is no value on any branch higher than the one given, 
	// so we can set it
	a.sequences[tableName] = newAutoIncVal
	return nil
}

// AddNewTable initializes a new table with an auto increment column to the tracker, as necessary
func (a AutoIncrementTracker) AddNewTable(tableName string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	tableName = strings.ToLower(tableName)
	// only initialize the sequence for this table if no other branch has such a table
	if _, ok := a.sequences[tableName]; !ok {
		a.sequences[tableName] = uint64(1)
	}
}

// DropTable drops the table with the name given.
// To establish the new auto increment value, callers must also pass all other working sets in scope that may include
// a table with the same name, omitting the working set that just deleted the table named.
func (a AutoIncrementTracker) DropTable(ctx context.Context, tableName string, wses ...*doltdb.WorkingSet) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// reset sequence to the minimum value
	a.sequences[strings.ToLower(tableName)] = 1

	// Get the new highest value from all tables in the working sets given
	for _, ws := range wses {
		table, _, exists, err := ws.WorkingRoot().GetTableInsensitive(ctx, tableName)
		if err != nil {
			return err
		}

		if !exists {
			continue
		}

		sch, err := table.GetSchema(ctx)
		if err != nil {
			return err
		}

		if schema.HasAutoIncrement(sch) {
			seq, err := table.GetAutoIncrementValue(ctx)
			if err != nil {
				return err
			}

			if seq > a.sequences[tableName] {
				a.sequences[tableName] = seq
			}
		}
	}

	return nil
}
