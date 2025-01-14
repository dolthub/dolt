// Copyright 2023 Dolthub, Inc.
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

package dsess

import (
	"context"
	"io"
	"math"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess/mutexmap"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type LockMode int64

var (
	LockMode_Traditional LockMode = 0
	LockMode_Concurret   LockMode = 1
	LockMode_Interleaved LockMode = 2
)

type AutoIncrementTracker struct {
	dbName    string
	sequences *sync.Map // map[string]uint64
	mm        *mutexmap.MutexMap
	lockMode  LockMode
}

var _ globalstate.AutoIncrementTracker = &AutoIncrementTracker{}

// NewAutoIncrementTracker returns a new autoincrement tracker for the roots given. All roots sets must be
// considered because the auto increment value for a table is tracked globally, across all branches.
// Roots provided should be the working sets when available, or the branches when they are not (e.g. for remote
// branches that don't have a local working set)
func NewAutoIncrementTracker(ctx context.Context, dbName string, roots ...doltdb.Rootish) (*AutoIncrementTracker, error) {
	ait := AutoIncrementTracker{
		dbName:    dbName,
		sequences: &sync.Map{},
		mm:        mutexmap.NewMutexMap(),
	}
	ait.InitWithRoots(ctx, roots...)
	return &ait, nil
}

func loadAutoIncValue(sequences *sync.Map, tableName string) uint64 {
	tableName = strings.ToLower(tableName)
	current, hasCurrent := sequences.Load(tableName)
	if !hasCurrent {
		return 0
	}
	return current.(uint64)
}

// Current returns the next value to be generated in the auto increment sequence for the table named
func (a *AutoIncrementTracker) Current(tableName string) uint64 {
	return loadAutoIncValue(a.sequences, tableName)
}

// Next returns the next auto increment value for the table named using the provided value from an insert (which may
// be null or 0, in which case it will be generated from the sequence).
func (a *AutoIncrementTracker) Next(tbl string, insertVal interface{}) (uint64, error) {
	tbl = strings.ToLower(tbl)

	given, err := CoerceAutoIncrementValue(insertVal)
	if err != nil {
		return 0, err
	}

	if a.lockMode == LockMode_Interleaved {
		release := a.mm.Lock(tbl)
		defer release()
	}

	curr := loadAutoIncValue(a.sequences, tbl)

	if given == 0 {
		// |given| is 0 or NULL
		a.sequences.Store(tbl, curr+1)
		return curr, nil
	}

	if given >= curr {
		a.sequences.Store(tbl, given+1)
		return given, nil
	}

	// |given| < curr
	return given, nil
}

func (a *AutoIncrementTracker) CoerceAutoIncrementValue(val interface{}) (uint64, error) {
	return CoerceAutoIncrementValue(val)
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
	val, _, err = gmstypes.Uint64.Convert(val)
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
func (a *AutoIncrementTracker) Set(ctx *sql.Context, tableName string, table *doltdb.Table, ws ref.WorkingSetRef, newAutoIncVal uint64) (*doltdb.Table, error) {
	tableName = strings.ToLower(tableName)

	release := a.mm.Lock(tableName)
	defer release()

	existing := loadAutoIncValue(a.sequences, tableName)
	if newAutoIncVal > existing {
		a.sequences.Store(tableName, newAutoIncVal)
		return table.SetAutoIncrementValue(ctx, newAutoIncVal)
	} else {
		// If the value is not greater than the current tracker, we have more work to do
		return a.deepSet(ctx, tableName, table, ws, newAutoIncVal)
	}
}

// deepSet sets the auto increment value for the table named, if it's greater than the one on any branch head for this
// database, ignoring the current in-memory tracker value
func (a *AutoIncrementTracker) deepSet(ctx *sql.Context, tableName string, table *doltdb.Table, ws ref.WorkingSetRef, newAutoIncVal uint64) (*doltdb.Table, error) {
	sess := DSessFromSess(ctx.Session)
	db, ok := sess.Provider().BaseDatabase(ctx, a.dbName)

	// just give up if we can't find this db for any reason, or it's a non-versioned DB
	if !ok || !db.Versioned() {
		return table, nil
	}

	// First, establish whether to update this table based on the given value and its current max value.
	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	aiCol, ok := schema.GetAutoIncrementColumn(sch)
	if !ok {
		return nil, nil
	}

	var indexData durable.Index
	aiIndex, ok := sch.Indexes().GetIndexByColumnNames(aiCol.Name)
	if ok {
		indexes, err := table.GetIndexSet(ctx)
		if err != nil {
			return nil, err
		}

		indexData, err = indexes.GetIndex(ctx, sch, nil, aiIndex.Name())
		if err != nil {
			return nil, err
		}
	} else {
		indexData, err = table.GetRowData(ctx)
		if err != nil {
			return nil, err
		}
	}

	currentMax, err := getMaxIndexValue(ctx, indexData)
	if err != nil {
		return nil, err
	}

	// If the given value is less than the current one, the operation is a no-op, bail out early
	if newAutoIncVal <= currentMax {
		return table, nil
	}

	table, err = table.SetAutoIncrementValue(ctx, newAutoIncVal)
	if err != nil {
		return nil, err
	}

	// Now that we have established the current max for this table, reset the global max accordingly
	maxAutoInc := newAutoIncVal
	doltdbs := db.DoltDatabases()
	for _, db := range doltdbs {
		branches, err := db.GetBranches(ctx)
		if err != nil {
			return nil, err
		}

		remotes, err := db.GetRemoteRefs(ctx)
		if err != nil {
			return nil, err
		}

		rootRefs := make([]ref.DoltRef, 0, len(branches)+len(remotes))
		rootRefs = append(rootRefs, branches...)
		rootRefs = append(rootRefs, remotes...)

		for _, b := range rootRefs {
			var rootish doltdb.Rootish
			switch b.GetType() {
			case ref.BranchRefType:
				wsRef, err := ref.WorkingSetRefForHead(b)
				if err != nil {
					return nil, err
				}

				if wsRef == ws {
					// we don't need to check the working set we're updating
					continue
				}

				ws, err := db.ResolveWorkingSet(ctx, wsRef)
				if err == doltdb.ErrWorkingSetNotFound {
					// use the branch head if there isn't a working set for it
					cm, err := db.ResolveCommitRef(ctx, b)
					if err != nil {
						return nil, err
					}
					rootish = cm
				} else if err != nil {
					return nil, err
				} else {
					rootish = ws
				}
			case ref.RemoteRefType:
				cm, err := db.ResolveCommitRef(ctx, b)
				if err != nil {
					return nil, err
				}
				rootish = cm
			}

			root, err := rootish.ResolveRootValue(ctx)
			if err != nil {
				return nil, err
			}

			table, _, ok, err := doltdb.GetTableInsensitive(ctx, root, doltdb.TableName{Name: tableName})
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}

			sch, err := table.GetSchema(ctx)
			if err != nil {
				return nil, err
			}

			if !schema.HasAutoIncrement(sch) {
				continue
			}

			tableName = strings.ToLower(tableName)
			seq, err := table.GetAutoIncrementValue(ctx)
			if err != nil {
				return nil, err
			}

			if seq > maxAutoInc {
				maxAutoInc = seq
			}
		}
	}

	a.sequences.Store(tableName, maxAutoInc)
	return table, nil
}

func getMaxIndexValue(ctx context.Context, indexData durable.Index) (uint64, error) {
	if types.IsFormat_DOLT(indexData.Format()) {
		idx := durable.ProllyMapFromIndex(indexData)

		iter, err := idx.IterAllReverse(ctx)
		if err != nil {
			return 0, err
		}

		kd, _ := idx.Descriptors()
		k, _, err := iter.Next(ctx)
		if err == io.EOF {
			return 0, nil
		} else if err != nil {
			return 0, err
		}

		// TODO: is the auto-inc column always the first column in the index?
		field, err := tree.GetField(ctx, kd, 0, k, idx.NodeStore())
		if err != nil {
			return 0, err
		}

		maxVal, err := CoerceAutoIncrementValue(field)
		if err != nil {
			return 0, err
		}

		return maxVal, nil
	}

	// For an LD format table, this operation won't succeed
	return math.MaxUint64, nil
}

// AddNewTable initializes a new table with an auto increment column to the tracker, as necessary
func (a *AutoIncrementTracker) AddNewTable(tableName string) {
	tableName = strings.ToLower(tableName)
	// only initialize the sequence for this table if no other branch has such a table
	a.sequences.LoadOrStore(tableName, uint64(1))
}

// DropTable drops the table with the name given.
// To establish the new auto increment value, callers must also pass all other working sets in scope that may include
// a table with the same name, omitting the working set that just deleted the table named.
func (a *AutoIncrementTracker) DropTable(ctx *sql.Context, tableName string, wses ...*doltdb.WorkingSet) error {
	tableName = strings.ToLower(tableName)

	release := a.mm.Lock(tableName)
	defer release()

	newHighestValue := uint64(1)

	// Get the new highest value from all tables in the working sets given
	for _, ws := range wses {
		table, _, exists, err := doltdb.GetTableInsensitive(ctx, ws.WorkingRoot(), doltdb.TableName{Name: tableName})
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

			if seq > newHighestValue {
				newHighestValue = seq
			}
		}
	}

	a.sequences.Store(tableName, newHighestValue)

	return nil
}

func (a *AutoIncrementTracker) AcquireTableLock(ctx *sql.Context, tableName string) (func(), error) {
	_, i, _ := sql.SystemVariables.GetGlobal("innodb_autoinc_lock_mode")
	lockMode := LockMode(i.(int64))
	if lockMode == LockMode_Interleaved {
		panic("Attempted to acquire AutoInc lock for entire insert operation, but lock mode was set to Interleaved")
	}
	a.lockMode = lockMode
	return a.mm.Lock(tableName), nil
}

func (a *AutoIncrementTracker) InitWithRoots(ctx context.Context, roots ...doltdb.Rootish) error {
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(128)

	for _, root := range roots {
		eg.Go(func() error {
			if egCtx.Err() != nil {
				return egCtx.Err()
			}

			r, rerr := root.ResolveRootValue(egCtx)
			if rerr != nil {
				return rerr
			}
			return r.IterTables(ctx, func(tableName doltdb.TableName, table *doltdb.Table, sch schema.Schema) (bool, error) {
				if !schema.HasAutoIncrement(sch) {
					return false, nil
				}

				seq, iErr := table.GetAutoIncrementValue(egCtx)
				if iErr != nil {
					return true, iErr
				}

				tableNameStr := tableName.ToLower().Name
				if oldValue, loaded := a.sequences.LoadOrStore(tableNameStr, seq); loaded && seq > oldValue.(uint64) {
					a.sequences.Store(tableNameStr, seq)
				}

				return false, nil
			})
		})
	}

	return eg.Wait()
}
