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
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/gcctx"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess/mutexmap"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate/sequences"
)

type LockMode int64

var (
	LockMode_Traditional LockMode = 0
	LockMode_Concurrent  LockMode = 1
	LockMode_Interleaved LockMode = 2
)

// A RelationSource maps table names to relations (which may be tables or root objects) at a supplied RootValue
type RelationSource[
	RelationType sequences.SequencedRelation[RelationType, ValueType, StateType],
	StateType sequences.SequenceState[StateType, ValueType],
	ValueType comparable,
] interface {
	GetRelation(ctx context.Context, root doltdb.RootValue, tName doltdb.TableName) (relation RelationType, resolvedName string, found bool, err error)
	GetRelations(ctx context.Context, root doltdb.RootValue, cb func(doltdb.TableName, RelationType) (bool, error)) error
}

type SequenceTracker[
	RelationType sequences.SequencedRelation[RelationType, ValueType, StateType],
	StateType sequences.SequenceState[StateType, ValueType],
	ValueType comparable,
] struct {
	initErr   error
	sequences *SyncMap[string, StateType]
	mm        *mutexmap.MutexMap
	// SequenceTracker is lazily initialized by loading
	// tracker state for every given |root|.  On first access, we
	// block on initialization being completed and we terminally
	// return |initErr| if there was any error initializing.
	init chan struct{}
	// To clean up effectively we need to stop all access to
	// storage. As part of that, we have the possibility to cancel
	// async initialization and block on the process completing.
	cancelInit chan struct{}
	dbName     string
	// lockMode is the effective @@innodb_autoinc_lock_mode at the time of SequenceTracker initialization.
	// This value can only be set by config and cannot be changed in a running server.
	lockMode LockMode
	// relationSource is how the tracker reads objects from a RootValue.
	// It may read tables or RootObjects.
	relationSource RelationSource[RelationType, StateType, ValueType]
}

// currentLockMode returns the effective @@innodb_autoinc_lock_mode stored in global server vars
func currentLockMode() LockMode {
	_, i, _ := sql.SystemVariables.GetGlobal("innodb_autoinc_lock_mode")
	if mode, ok := i.(int64); ok {
		return LockMode(mode)
	}
	return LockMode_Interleaved
}

func (a *SequenceTracker[RelationType, StateType, ValueType]) staticAssertTypes() {
	var _ globalstate.SequenceTracker[RelationType, StateType, ValueType] = a
}

func NewAutoIncrementTrackerI[
	RelationType sequences.SequencedRelation[RelationType, ValueType, StateType],
	StateType sequences.SequenceState[StateType, ValueType],
	ValueType comparable,
](ctx context.Context, dbName string, relationSource RelationSource[RelationType, StateType, ValueType], roots ...doltdb.Rootish) (*SequenceTracker[RelationType, StateType, ValueType], error) {
	ait := SequenceTracker[RelationType, StateType, ValueType]{
		dbName:         dbName,
		sequences:      &SyncMap[string, StateType]{},
		mm:             mutexmap.NewMutexMap(),
		init:           make(chan struct{}),
		cancelInit:     make(chan struct{}),
		relationSource: relationSource,
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

func loadSequenceState[StateType sequences.SequenceState[StateType, ValueType], ValueType comparable](sequences *SyncMap[string, StateType], tableName string) (current StateType, hasCurrent bool) {
	tableName = strings.ToLower(tableName)
	return sequences.Load(tableName)
}

func (a *SequenceTracker[RelationType, StateType, ValueType]) initializeTableAutoIncrement(ctx *sql.Context, tableName string, initialValue interface{}) (state StateType, hasState bool, err error) {
	sess := DSessFromSess(ctx.Session)
	ws, err := sess.WorkingSet(ctx, a.dbName)
	if err != nil {
		return state, false, err
	}

	table, _, ok, err := a.relationSource.GetRelation(ctx, ws.WorkingRoot(), doltdb.TableName{Name: tableName})
	if err != nil || !ok {
		return state, false, err
	}

	hasAutoIncrement, err := table.HasSequenceState(ctx)
	if err != nil {
		return state, false, err
	}

	var seq StateType
	if !hasAutoIncrement {
		// Create a new state based on the provided value
		// TODO: This could cause problems when we need to create the more
		// complicated sequence types for Doltgres
		seq, err = state.WithSQLValue(ctx, initialValue)
		if err != nil {
			return state, false, err
		}
	} else {
		seq, err = table.GetSequenceState(ctx)
		if err != nil {
			return state, false, err
		}
	}

	relation, err := a.deepSet(ctx, tableName, table, ws.Ref(), seq)
	if err != nil {
		return state, false, err
	}

	seq, ok = loadSequenceState(a.sequences, tableName)
	if ok {
		return state, true, nil
	}

	seq, err = relation.GetSequenceState(ctx)
	if err != nil {
		return state, false, err
	}
	a.sequences.Store(strings.ToLower(tableName), seq)
	return state, true, nil
}

func (a *SequenceTracker[RelationType, StateType, ValueType]) Close() {
	close(a.cancelInit)
	<-a.init
}

// Current returns the next value to be generated in the auto increment sequence for |tableName|.
func (a *SequenceTracker[RelationType, StateType, ValueType]) Current(relation string) (current StateType, err error) {
	err = a.waitForInit()
	if err != nil {
		return current, err
	}
	seq, ok := loadSequenceState(a.sequences, relation)
	if !ok {
		return current, nil
	}
	return seq, nil
}

// Next returns the next auto increment value for |tbl| using |insertVal| from an insert. If |insertVal| is
// null or 0, it is generated from the sequence.
func (a *SequenceTracker[RelationType, StateType, ValueType]) Next(ctx *sql.Context, tbl string, insertVal interface{}) (nextValue ValueType, err error) {
	err = a.waitForInit()
	if err != nil {
		return nextValue, err
	}

	tbl = strings.ToLower(tbl)

	// The read-modify-write of the sequence below must be atomic across concurrent inserters. In
	// interleaved lock mode (the default) the engine holds no statement-level lock, so we take a
	// short per-table lock here.
	locked := false
	if a.lockMode == LockMode_Interleaved {
		release := a.mm.Lock(tbl)
		defer release()
		locked = true
	}

	currState, ok := loadSequenceState(a.sequences, tbl)
	if !ok {
		// Missing tracker state after initialization can happen when a running sql-server discovers a database
		// restored after startup, so initialize it here.
		if !locked {
			if a.lockMode == LockMode_Interleaved {
				release := a.mm.Lock(tbl)
				defer release()
				locked = true
			}

			currState, ok = loadSequenceState(a.sequences, tbl)
		}

		if !ok {
			currState, ok, err = a.initializeTableAutoIncrement(ctx, tbl, insertVal)
			if err != nil {
				return nextValue, err
			}
			if !ok {
				return nextValue, fmt.Errorf("autoIncrementTracker: unable to find sequence for table %s", tbl)
			}
		}
	}

	if insertVal == nil {
		// |given| is 0 or NULL
		currentVal, _, nextState, err := currState.Next()
		if err != nil {
			return nextValue, err
		}
		a.sequences.Store(tbl, nextState)
		return currentVal, nil
	}

	givenState, err := currState.WithSQLValue(ctx, insertVal)
	if err != nil {
		return nextValue, err
	}
	given := givenState.CurrentValue()

	if !currState.GreaterThan(givenState) {
		// Check if the given value is valid for this column type
		if !a.validateAutoIncrementBounds(ctx, tbl, givenState, false) {
			return givenState.CurrentValue(), nil // Out of bounds, don't update sequence
		}

		// Value is valid, determine next sequence value
		if a.validateAutoIncrementBounds(ctx, tbl, givenState, true) {
			_, _, givenState, err = givenState.Next()
			if err != nil {
				return nextValue, err
			}
		}
		a.sequences.Store(tbl, givenState)
		return given, nil
	}

	return given, nil
}

// Set sets the auto increment value for the table named, if it's greater than the one already registered for this
// table. Otherwise, the update is silently disregarded. So far this matches the MySQL behavior, but Dolt uses the
// maximum value for this table across all branches.
func (a *SequenceTracker[RelationType, StateType, ValueType]) Set(ctx *sql.Context, tableName string, table RelationType, ws ref.WorkingSetRef, newSequenceState StateType) (newRelation RelationType, err error) {
	err = a.waitForInit()
	if err != nil {
		return newRelation, err
	}

	tableName = strings.ToLower(tableName)

	release := a.mm.Lock(tableName)
	defer release()

	existing, ok := loadSequenceState(a.sequences, tableName)
	if !ok {
		a.sequences.Store(tableName, newSequenceState)
		return table.SetSequenceState(ctx, newSequenceState)
	}
	gt := newSequenceState.GreaterThan(existing)
	if gt && a.validateAutoIncrementBounds(ctx, tableName, newSequenceState, true) {
		a.sequences.Store(tableName, newSequenceState)
		return table.SetSequenceState(ctx, newSequenceState)
	} else if gt {
		// Value is greater but out of bounds, don't update
		return table, nil
	}
	// Value is not greater than current, do deep check across branches
	return a.deepSet(ctx, tableName, table, ws, newSequenceState)
}

// deepSet sets the sequence state for the table named, if it's greater than the one on any branch head for this
// database, ignoring the current in-memory tracker value
func (a *SequenceTracker[RelationType, StateType, ValueType]) deepSet(ctx *sql.Context, tableName string, table RelationType, ws ref.WorkingSetRef, newAutoIncVal StateType) (newRelation RelationType, err error) {
	sess := DSessFromSess(ctx.Session)
	db, ok := sess.Provider().BaseDatabase(ctx, a.dbName)

	// just give up if we can't find this db for any reason, or it's a non-versioned DB
	if !ok || !db.Versioned() {
		return table, nil
	}

	table, success, err := table.TrySetSequenceState(ctx, newAutoIncVal)
	if err != nil {
		return newRelation, err
	}
	if !success {
		return table, nil
	}

	// Now that we have established the current max for this table, reset the global max accordingly
	maxAutoInc := newAutoIncVal
	doltdbs := db.DoltDatabases()
	for _, db := range doltdbs {
		branches, err := db.GetBranches(ctx)
		if err != nil {
			return newRelation, err
		}

		remotes, err := db.GetRemoteRefs(ctx)
		if err != nil {
			return newRelation, err
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
					return newRelation, err
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
						return newRelation, err
					}
					rootish = cm
				} else if err != nil {
					return newRelation, err
				} else {
					rootish = ws
				}
			case ref.RemoteRefType:
				cm, err := db.ResolveCommitRef(ctx, b)
				if err != nil {
					return newRelation, err
				}
				rootish = cm
			}

			root, err := rootish.ResolveRootValue(ctx)
			if err != nil {
				return newRelation, err
			}

			table, _, ok, err := a.relationSource.GetRelation(ctx, root, doltdb.TableName{Name: tableName})
			if err != nil {
				return newRelation, err
			}
			if !ok {
				continue
			}

			hasAutoIncrement, err := table.HasSequenceState(ctx)
			if err != nil {
				return newRelation, err
			}

			if !hasAutoIncrement {
				continue
			}

			tableName = strings.ToLower(tableName)
			seq, err := table.GetSequenceState(ctx)
			if err != nil {
				return newRelation, err
			}

			var mergeOk bool
			maxAutoInc, mergeOk = maxAutoInc.Merge(seq)
			if !mergeOk {
				// TODO: This can't happen with AUTO INCREMENT but needs to be
				// handled for sequences.
			}
		}
	}

	if a.validateAutoIncrementBounds(ctx, tableName, maxAutoInc, true) {
		a.sequences.Store(tableName, maxAutoInc)
	}
	return table, nil
}

// AddNewTable initializes a new table with an auto increment column to the tracker, as necessary
func (a *SequenceTracker[RelationType, StateType, ValueType]) AddNewTable(tableName string, initialState StateType) error {
	err := a.waitForInit()
	if err != nil {
		return err
	}

	tableName = strings.ToLower(tableName)
	// only initialize the sequence for this table if no other branch has such a table
	a.sequences.LoadOrStore(tableName, initialState)
	return nil
}

// DropTable drops the table with the name given.
// To establish the new auto increment value, callers must also pass all other working sets in scope that may include
// a table with the same name, omitting the working set that just deleted the table named.
func (a *SequenceTracker[RelationType, StateType, ValueType]) DropTable(ctx *sql.Context, tableName string, wses ...*doltdb.WorkingSet) error {
	err := a.waitForInit()
	if err != nil {
		return err
	}

	tableName = strings.ToLower(tableName)

	release := a.mm.Lock(tableName)
	defer release()

	var newHighestValue *StateType

	// Get the new highest value from all tables in the working sets given
	for _, ws := range wses {
		table, _, exists, err := a.relationSource.GetRelation(ctx, ws.WorkingRoot(), doltdb.TableName{Name: tableName})
		if err != nil {
			return err
		}

		if !exists {
			continue
		}

		hasAutoIncrement, err := table.HasSequenceState(ctx)
		if err != nil {
			return err
		}
		if hasAutoIncrement {
			seq, err := table.GetSequenceState(ctx)
			if err != nil {
				return err
			}
			if newHighestValue == nil {
				newHighestValue = &seq
			} else {
				var ok bool
				*newHighestValue, ok = (*newHighestValue).Merge(seq)
				if !ok {
					// TODO: This can't happen with AUTO INCREMENT but needs to be
					// handled for sequences.
				}
			}

		}
	}

	if newHighestValue != nil {
		a.sequences.Store(tableName, *newHighestValue)
	} else {
		a.sequences.Delete(tableName)
	}

	return nil
}

func (a *SequenceTracker[RelationType, StateType, ValueType]) AcquireTableLock(ctx *sql.Context, tableName string) (func(), error) {
	err := a.waitForInit()
	if err != nil {
		return nil, err
	}

	if a.lockMode == LockMode_Interleaved {
		// This shouldn't be possible, it's a serious programming error if it happens
		panic("Attempted to acquire AutoInc lock for entire insert operation, but lock mode was set to Interleaved")
	}
	return a.mm.Lock(tableName), nil
}

func (a *SequenceTracker[RelationType, StateType, ValueType]) waitForInit() error {
	select {
	case <-a.init:
		return a.initErr
	case <-time.After(5 * time.Minute):
		return errors.New("failed to initialize autoincrement tracker")
	}
}

// This method will initialize the SequenceTracker state with all
// data from the tables found in |roots|.  This method closes the
// |a.init| channel when it completes. It is meant to be run in a
// goroutine, as in `go a.initWithRoots(...)`. When running this method,
// a newly allocated |a.init| channel should exist.
//
// It is the caller's responsibility to ensure that whatever |ctx|
// |initWithRoots| is called with appropriately outlives the end of
// the method and that it participates in GC lifecycle callbacks
// appropriately, if that is necessary.
func (a *SequenceTracker[RelationType, StateType, ValueType]) initWithRoots(ctx context.Context, roots ...doltdb.Rootish) {
	defer close(a.init)

	// Cancel the parent context so that the errgroup work will
	// complete with an error if we see cancelInit closed.
	finishedCh := make(chan struct{})
	defer close(finishedCh)
	ctx, cancel := context.WithCancelCause(ctx)
	go func() {
		select {
		case <-a.cancelInit:
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

			init := func(tableName doltdb.TableName, relation RelationType) (bool, error) {
				hasSequenceState, err := relation.HasSequenceState(ctx)
				if err != nil {
					return true, err
				}
				if !hasSequenceState {
					return false, nil
				}
				seq, err := relation.GetSequenceState(ctx)
				if err != nil {
					return true, err
				}

				tableNameStr := tableName.ToLower().Name
				if oldValue, loaded := a.sequences.LoadOrStore(tableNameStr, seq); loaded {
					for seq.GreaterThan(oldValue) && !a.sequences.CompareAndSwap(tableNameStr, oldValue, seq) {
						oldValue, _ = a.sequences.Load(tableNameStr)
					}
				}

				return false, nil
			}

			return a.relationSource.GetRelations(ctx, r, init)
		})
	}

	a.lockMode = currentLockMode()
	a.initErr = eg.Wait()
}

// validateAutoIncrementBounds checks if a value (or value+1 if checkIncrement) is valid for the auto-increment column type
func (a *SequenceTracker[RelationType, StateType, ValueType]) validateAutoIncrementBounds(ctx *sql.Context, tbl string, val StateType, checkIncrement bool) bool {
	sess := DSessFromSess(ctx.Session)
	db, ok := sess.Provider().BaseDatabase(ctx, a.dbName)
	if !ok || !db.Versioned() {
		return true // fail-open for infrastructure errors
	}

	ws, err := sess.WorkingSet(ctx, a.dbName)
	if err != nil {
		return true
	}

	table, _, ok, err := a.relationSource.GetRelation(ctx, ws.WorkingRoot(), doltdb.TableName{Name: tbl})
	if err != nil || !ok {
		return true
	}

	hasSequenceState, err := table.HasSequenceState(ctx)
	if !hasSequenceState {
		// fail-open because the table writer could be in the process of adding auto-increment to a column
		return true
	}

	sqlType, ok, err := table.GetSequenceSqlType(ctx)
	if err != nil || !ok {
		return true
	}

	testVal := val
	if checkIncrement {
		// TODO: Remove error parameter?
		_, hasNext, nextVal, _ := val.Next()
		// SequenceState can only error if there is no next value.
		// Consider changing this to a separate |ok| return value.
		if !hasNext {
			return false
		}
		testVal = nextVal
	}

	_, inRange, err := sqlType.Convert(ctx, testVal.CurrentValue())
	return err == nil && inRange == sql.InRange
}

func (a *SequenceTracker[RelationType, StateType, ValueType]) InitWithRoots(ctx context.Context, roots ...doltdb.Rootish) error {
	err := a.waitForInit()
	if err != nil {
		return err
	}
	a.init = make(chan struct{})
	go a.initWithRoots(ctx, roots...)
	return a.waitForInit()
}
