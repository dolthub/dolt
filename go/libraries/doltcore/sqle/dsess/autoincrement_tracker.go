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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
)

// AutoIncrementTracker knows how to get and set the current auto increment value for a table. It's defined as an
// interface here because implementations need to reach into session state, requiring a dependency on this package.
// Therefore 
type AutoIncrementTracker interface {
	// Current returns the current auto increment value for the given table.
	Current(tableName string) uint64
	// Next returns the next auto increment value for the given table, and increments the current value.
	Next(tbl string, insertVal interface{}) (uint64, error)
	// AddNewTable adds a new table to the tracker, initializing the auto increment value to 1.
	AddNewTable(tableName string)
	// DropTable removes a table from the tracker.
	DropTable(ctx *sql.Context, tableName string, wses ...*doltdb.WorkingSet) error
	// CoerceAutoIncrementValue coerces the given value to a uint64, returning an error if it can't be done.
	CoerceAutoIncrementValue(val interface{}) (uint64, error)
	// Set sets the auto increment value for the given table. This operation may silently do nothing if this value is 
	// below the current value.
	Set(ctx *sql.Context, tableName string, newAutoIncVal uint64) error
}

// GlobalState is just a holding interface for pieces of global state, of which the auto increment tracking info is 
// the only example at the moment.
type GlobalState interface {
	// AutoIncrementTracker returns the auto increment tracker for this global state.
	AutoIncrementTracker(ctx *sql.Context) (AutoIncrementTracker, error)
}

// GlobalStateProvider is an optional interface for databases that provide global state tracking
type GlobalStateProvider interface {
	GetGlobalState() GlobalState
}

