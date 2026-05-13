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

package dsess

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// ConflictsBypassMarker propagates an already-validated decision to admit a
// merge-permission caller into the source-table writer that the conflicts
// table's Updater delegates to. The marker is set on the context only during
// source-table writer construction inside prollyConflictOurTableUpdater.
//
// The source-table Updater factory checks for the marker and, if it matches
// the table being constructed, skips its own Permissions_Write check. The
// match is keyed on (db, branch, table) so the marker cannot be used to
// bypass writes on a different table.
type ConflictsBypassMarker struct {
	DbName  string
	Branch  string
	TblName doltdb.TableName
}

type conflictsBypassKeyT struct{}

var conflictsBypassKey conflictsBypassKeyT

// WithConflictsBypass returns a derived context carrying the given bypass
// marker. Callers should pass this context to the source-table writer
// factory and discard it immediately after — the marker is intentionally
// scoped to the factory call.
func WithConflictsBypass(ctx context.Context, m ConflictsBypassMarker) context.Context {
	return context.WithValue(ctx, conflictsBypassKey, m)
}

// ConflictsBypassFor reports whether the context carries a bypass marker
// matching the given (db, branch, table). Mismatched contexts return false
// so a marker for table t1 cannot be reused to write to t2.
func ConflictsBypassFor(ctx context.Context, dbName, branch string, tblName doltdb.TableName) bool {
	m, ok := ctx.Value(conflictsBypassKey).(ConflictsBypassMarker)
	if !ok {
		return false
	}
	return m.DbName == dbName && m.Branch == branch && m.TblName == tblName
}
