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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate/sequences"
)

// SequenceTrackerBase is the non-generic base interface for SequenceTracker
// It is useful for establishing an upper bound on type parameters in some circumstances.
type SequenceTrackerBase interface {
	// AcquireTableLock acquires the auto increment lock on a relation, and returns a callback function to release the lock.
	// Depending on the value of the `innodb_autoinc_lock_mode` system variable, the engine may need to acquire and hold
	// the lock for the duration of an insert statement.
	AcquireTableLock(ctx *sql.Context, tableName string) (func(), error)
	// DropTable removes a relation from the tracker.
	DropTable(ctx *sql.Context, relation string, wses ...*doltdb.WorkingSet) error
	// InitWithRoots fills the SequenceTracker with values pulled from each root in order.
	InitWithRoots(ctx context.Context, roots ...doltdb.Rootish) error
	Close()
}

// SequenceTracker knows how to get and set the current auto increment value for a relation (a table or a root object).
// It's defined as an interface here because implementations need to reach into session state, requiring a dependency on this package.
type SequenceTracker[
	RelationType sequences.SequencedRelation[RelationType, ValueType, StateType],
	StateType sequences.SequenceState[StateType, ValueType],
	ValueType comparable,
] interface {
	SequenceTrackerBase
	// Current returns the current auto increment state for the given relation.
	Current(relation string) (StateType, error)
	// Next returns the next SQL value produced by the given relation, and advances that relation's state.
	Next(ctx *sql.Context, relation string, insertVal interface{}) (ValueType, error)
	// AddNewTable adds a new table to the tracker, initializing the auto increment value to the provided |initialState|.
	AddNewTable(relation string, initialState StateType) error
	// Set sets the auto increment value for the given relation. This operation may silently do nothing if this value is
	// below the current value for this relation. The relation in the provided working set is assumed to already have the value
	// given, so the new global maximum is computed without regard for its value in that working set.
	Set(ctx *sql.Context, tableName string, table RelationType, ws ref.WorkingSetRef, newSequenceState StateType) (RelationType, error)
}
