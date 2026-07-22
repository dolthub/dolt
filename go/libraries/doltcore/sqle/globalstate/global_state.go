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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// GlobalState is just a holding interface for pieces of global state, of which the auto increment tracking info is
// the only example at the moment.
type GlobalState interface {
	// GetSequenceTracker returns the auto increment tracker for this global state.
	GetSequenceTracker(ctx *sql.Context, key interface{}) (SequenceTrackerBase, error)
	// AddSequenceTracker adds a new SequenceTracker to the GlobalState, accessible by the provided key.
	AddSequenceTracker(ctx *sql.Context, key interface{}, value SequenceTrackerBase) error
	// InitWithRoots initializes all of the state's SequenceTrackers
	InitWithRoots(ctx *sql.Context, roots ...doltdb.Rootish) error
}

// GlobalStateProvider is an optional interface for databases that provide global state tracking
type GlobalStateProvider interface {
	GetGlobalState() GlobalState
}
