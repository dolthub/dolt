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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
)

type DoltDBRelationSource struct{}

func (s DoltDBRelationSource) GetRelation(ctx context.Context, root doltdb.RootValue, tName doltdb.TableName) (relation *doltdb.Table, resolvedName string, found bool, err error) {
	return doltdb.GetTableInsensitive(ctx, root, tName)
}

func (s DoltDBRelationSource) GetRelations(ctx context.Context, root doltdb.RootValue, cb func(doltdb.TableName, *doltdb.Table) (bool, error)) error {
	return root.IterTables(ctx, func(name doltdb.TableName, table *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		return cb(name, table)
	})
}

var _ RelationSource[*doltdb.Table, doltdb.AutoIncrementState, uint64] = (*DoltDBRelationSource)(nil)

type AutoIncrementTracker = SequenceTracker[*doltdb.Table, doltdb.AutoIncrementState, uint64]

// NewAutoIncrementTracker returns a new autoincrement tracker for the roots given. All roots sets must be
// considered because the auto increment value for a table is tracked globally, across all branches.
// Roots provided should be the working sets when available, or the branches when they are not (e.g. for remote
// branches that don't have a local working set)
func NewAutoIncrementTracker(ctx context.Context, dbName string, roots ...doltdb.Rootish) (*AutoIncrementTracker, error) {
	return NewAutoIncrementTrackerI(ctx, dbName, DoltDBRelationSource{}, roots...)
}

func GetAutoIncrementTracker(ctx *sql.Context, gs globalstate.GlobalState) (*AutoIncrementTracker, error) {
	return GetSequenceTracker(ctx, gs, autoIncrementTrackerKey)
}

// autoIncrementTrackerKey is the key used to store the AutoIncrmenetTracker in the GlobalState's map of SequenceTrackers
var autoIncrementTrackerKey = TrackerKey[*AutoIncrementTracker]{}
