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
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/gcctx"
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

	// AutoIncrementTracker is lazily initialized by loading
	// tracker state for every given |root|.  On first access, we
	// block on initialization being completed and we terminally
	// return |initErr| if there was any error initializing.
	init    chan struct{}
	initErr error

	// To clean up effectively we need to stop all access to
	// storage. As part of that, we have the possibility to cancel
	// async initialization and block on the process completing.
	cancelInit chan struct{}
}

var _ globalstate.AutoIncrementTracker = &AutoIncrementTracker{}

// NewAutoIncrementTracker returns a new autoincrement tracker for the roots given. All roots sets must be
// considered because the auto increment value for a table is tracked globally, across all branches.
// Roots provided should be the working sets when available, or the branches when they are not (e.g. for remote
// branches that don't have a local working set)
func NewAutoIncrementTracker(ctx context.Context, dbName string, roots ...doltdb.Rootish) (*AutoIncrementTracker, error) {
	ait := AutoIncrementTracker{
		dbName:     dbName,
		sequences:  &sync.Map{},
		mm:         mutexmap.NewMutexMap(),
		init:       make(chan struct{}),
		cancelInit: make(chan struct{}),
	}
	gcSafepointController := getGCSafepointController(ctx)
	ctx = context.Background()
	if gcSafepointController != nil {
		ctx = gcctx.WithGCSafepointController(ctx, gcSafepointController)
	}
	go func() {
		if gcSafepointController != nil {
			defer gcctx.SessionEnd(ctx)
			gcctx.SessionCommandBegin(ctx)
			defer gcctx.SessionCommandEnd(ctx)
		}
		ait.initWithRoots(ctx, roots...)
	}()
	return &ait, nil
}

func getGCSafepointController(ctx context.Context) *gcctx.GCSafepointController {
	if sqlCtx, ok := ctx.(*sql.Context); ok {
		return DSessFromSess(sqlCtx.Session).GCSafepointController()
	}
	return gcctx.GetGCSafepointController(ctx)
}

func loadAutoIncValue(sequences *sync.Map, tableName string) uint64 {
	tableName = strings.ToLower(tableName)
	current, hasCurrent := sequences.Load(tableName)
	if !hasCurrent {
		return 0
	}
	return current.(uint64)
}

func (a *AutoIncrementTracker) Close() {
	close(a.cancelInit)
	<-a.init
}

// Current returns the next value to be generated in the auto increment sequence for the table named
func (a *AutoIncrementTracker) Current(tableName string) (uint64, error) {
	err := a.waitForInit()
	if err != nil {
		return 0, err
	}
	return loadAutoIncValue(a.sequences, tableName), nil
}

// Next returns the next auto increment value for the table named using the provided value from an insert (which may
// be null or 0, in which case it will be generated from the sequence).
func (a *AutoIncrementTracker) Next(ctx *sql.Context, tbl string, insertVal interface{}) (uint64, error) {
	err := a.waitForInit()
	if err != nil {
		return 0, err
	}

	tbl = strings.ToLower(tbl)

	given, err := CoerceAutoIncrementValue(ctx, insertVal)
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
		// Check if the given value is valid for this column type
		if !a.validateAutoIncrementBounds(ctx, tbl, given, false) {
			return given, nil // Out of bounds, don't update sequence
		}

		// Value is valid, determine next sequence value
		nextVal := given
		if a.validateAutoIncrementBounds(ctx, tbl, given, true) {
			nextVal++
		}
		a.sequences.Store(tbl, nextVal)
		return given, nil
	}

	// |given| < curr
	return given, nil
}

func (a *AutoIncrementTracker) CoerceAutoIncrementValue(ctx *sql.Context, val interface{}) (uint64, error) {
	err := a.waitForInit()
	if err != nil {
		return 0, err
	}
	return CoerceAutoIncrementValue(ctx, val)
}

// CoerceAutoIncrementValue converts |val| into an AUTO_INCREMENT sequence value
func CoerceAutoIncrementValue(ctx *sql.Context, val interface{}) (uint64, error) {
	switch typ := val.(type) {
	case float32:
		val = math.Round(float64(typ))
	case float64:
		val = math.Round(typ)
	}

	var err error
	val, _, err = gmstypes.Uint64.Convert(ctx, val)
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
	err := a.waitForInit()
	if err != nil {
		return nil, err
	}

	tableName = strings.ToLower(tableName)

	release := a.mm.Lock(tableName)
	defer release()

	existing := loadAutoIncValue(a.sequences, tableName)
	if newAutoIncVal > existing && a.validateAutoIncrementBounds(ctx, tableName, newAutoIncVal, true) {
		a.sequences.Store(tableName, newAutoIncVal)
		return table.SetAutoIncrementValue(ctx, newAutoIncVal)
	} else if newAutoIncVal > existing {
		// Value is greater but out of bounds, don't update
		return table, nil
	}
	// Value is not greater than current, do deep check across branches
	return a.deepSet(ctx, tableName, table, ws, newAutoIncVal)
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

	if a.validateAutoIncrementBounds(ctx, tableName, maxAutoInc, true) {
		a.sequences.Store(tableName, maxAutoInc)
	}
	return table, nil
}

func getMaxIndexValue(ctx *sql.Context, indexData durable.Index) (uint64, error) {
	if types.IsFormat_DOLT(indexData.Format()) {
		idx, err := durable.ProllyMapFromIndex(indexData)
		if err != nil {
			return 0, err
		}

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

		maxVal, err := CoerceAutoIncrementValue(ctx, field)
		if err != nil {
			return 0, err
		}

		return maxVal, nil
	}

	// For an LD format table, this operation won't succeed
	return math.MaxUint64, nil
}

// AddNewTable initializes a new table with an auto increment column to the tracker, as necessary
func (a *AutoIncrementTracker) AddNewTable(tableName string) error {
	err := a.waitForInit()
	if err != nil {
		return err
	}

	tableName = strings.ToLower(tableName)
	// only initialize the sequence for this table if no other branch has such a table
	a.sequences.LoadOrStore(tableName, uint64(1))
	return nil
}

// DropTable drops the table with the name given.
// To establish the new auto increment value, callers must also pass all other working sets in scope that may include
// a table with the same name, omitting the working set that just deleted the table named.
func (a *AutoIncrementTracker) DropTable(ctx *sql.Context, tableName string, wses ...*doltdb.WorkingSet) error {
	err := a.waitForInit()
	if err != nil {
		return err
	}

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
	err := a.waitForInit()
	if err != nil {
		return nil, err
	}

	_, i, _ := sql.SystemVariables.GetGlobal("innodb_autoinc_lock_mode")
	lockMode := LockMode(i.(int64))
	if lockMode == LockMode_Interleaved {
		panic("Attempted to acquire AutoInc lock for entire insert operation, but lock mode was set to Interleaved")
	}
	a.lockMode = lockMode
	return a.mm.Lock(tableName), nil
}

func (a *AutoIncrementTracker) waitForInit() error {
	select {
	case <-a.init:
		return a.initErr
	case <-time.After(5 * time.Minute):
		return errors.New("failed to initialize autoincrement tracker")
	}
}

// This method will initialize the AutoIncrementTracker state with all
// data from the tables found in |roots|.  This method closes the
// |a.init| channel when it completes. It is meant to be run in a
// goroutine, as in `go a.initWithRoots(...)`. When running this method,
// a newly allocated |a.init| channel should exist.
//
// It is the caller's responsibility to ensure that whatever |ctx|
// |initWithRoots| is called with appropriately outlives the end of
// the method and that it participates in GC lifecycle callbacks
// appropriately, if that is necessary.
func (a *AutoIncrementTracker) initWithRoots(ctx context.Context, roots ...doltdb.Rootish) {
	defer close(a.init)

	// Cancel the parent context so that the errgroup work will
	// complete with an error if we see cancelInit closed.
	finishedCh := make(chan struct{})
	defer close(finishedCh)
	ctx, cancel := context.WithCancelCause(ctx)
	go func() {
		select {
		case <-a.cancelInit:
			fmt.Printf("canceling it...\n")
			cancel(errors.New("initialization canceled. did not complete successfully."))
		case <-finishedCh:
		}
	}()

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(128)

	for _, root := range roots {
		eg.Go(func() error {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}

			r, err := root.ResolveRootValue(ctx)
			if err != nil {
				return err
			}

			return r.IterTables(ctx, func(tableName doltdb.TableName, table *doltdb.Table, sch schema.Schema) (bool, error) {
				if !schema.HasAutoIncrement(sch) {
					return false, nil
				}

				seq, err := table.GetAutoIncrementValue(ctx)
				if err != nil {
					return true, err
				}

				tableNameStr := tableName.ToLower().Name
				if oldValue, loaded := a.sequences.LoadOrStore(tableNameStr, seq); loaded && seq > oldValue.(uint64) {
					a.sequences.Store(tableNameStr, seq)
				}

				return false, nil
			})
		})
	}

	a.initErr = eg.Wait()
}

// validateAutoIncrementBounds checks if a value (or value+1 if checkIncrement) is valid for the auto-increment column type
func (a *AutoIncrementTracker) validateAutoIncrementBounds(ctx *sql.Context, tbl string, val uint64, checkIncrement bool) bool {
	sess := DSessFromSess(ctx.Session)
	db, ok := sess.Provider().BaseDatabase(ctx, a.dbName)
	if !ok || !db.Versioned() {
		return true // fail-open for infrastructure errors
	}

	ws, err := sess.WorkingSet(ctx, a.dbName)
	if err != nil {
		return true
	}

	table, _, ok, err := doltdb.GetTableInsensitive(ctx, ws.WorkingRoot(), doltdb.TableName{Name: tbl})
	if err != nil || !ok {
		return true
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return true
	}

	aiCol, ok := schema.GetAutoIncrementColumn(sch)
	if !ok {
		return true
	}

	sqlType := aiCol.TypeInfo.ToSqlType()

	testVal := val
	if checkIncrement {
		// Check if incrementing would overflow
		nextVal := val + 1
		if nextVal < val {
			return false // uint64 overflow
		}
		testVal = nextVal
	}

	_, inRange, err := sqlType.Convert(ctx, testVal)
	return err == nil && inRange == sql.InRange
}

func (a *AutoIncrementTracker) InitWithRoots(ctx context.Context, roots ...doltdb.Rootish) error {
	err := a.waitForInit()
	if err != nil {
		return err
	}
	a.init = make(chan struct{})
	go a.initWithRoots(ctx, roots...)
	return a.waitForInit()
}
