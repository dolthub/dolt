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
)

// AutoIncrementTracker knows how to get and set the current auto increment value for a table. It's defined as an
// interface here because implementations need to reach into session state, requiring a dependency on this package.
type AutoIncrementTracker interface {
	// Current returns the current auto increment value for the given table.
	Current(tableName string) (uint64, error)
	// Next returns the next auto increment value for the given table, and increments the current value.
	Next(tbl string, insertVal interface{}) (uint64, error)
	// AddNewTable adds a new table to the tracker, initializing the auto increment value to 1.
	AddNewTable(tableName string) error
	// DropTable removes a table from the tracker.
	DropTable(ctx *sql.Context, tableName string, wses ...*doltdb.WorkingSet) error
	// CoerceAutoIncrementValue coerces the given value to a uint64, returning an error if it can't be done.
	CoerceAutoIncrementValue(val interface{}) (uint64, error)
	// Set sets the auto increment value for the given table. This operation may silently do nothing if this value is
	// below the current value for this table. The table in the provided working set is assumed to already have the value
	// given, so the new global maximum is computed without regard for its value in that working set.
	Set(ctx *sql.Context, tableName string, table *doltdb.Table, ws ref.WorkingSetRef, newAutoIncVal uint64) (*doltdb.Table, error)
	// AcquireTableLock acquires the auto increment lock on a table, and returns a callback function to release the lock.
	// Depending on the value of the `innodb_autoinc_lock_mode` system variable, the engine may need to acquire and hold
	// the lock for the duration of an insert statement.
	AcquireTableLock(ctx *sql.Context, tableName string) (func(), error)
	// InitWithRoots fills the AutoIncrementTracker with values pulled from each root in order.
	InitWithRoots(ctx context.Context, roots ...doltdb.Rootish) error
}
