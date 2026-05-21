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

package dtables

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dsess"
	"github.com/dolthub/dolt/go/store/prolly"
)

// checkConflictsTableWriteAccess gates writes that flow through a
// dolt_conflicts_<t> system table. Allowed when:
//
//   - the caller has Permissions_Write on the branch, OR
//   - the caller has Permissions_Merge AND the working set has an active
//     merge AND the artifact map contains data conflicts.
//
// The merge-permission carve-out exists so that PR reviewers with only
// merge access can resolve a merge that produced conflicts.
func checkConflictsTableWriteAccess(ctx *sql.Context, db dsess.SqlDatabase, artM prolly.ArtifactMap) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err == nil {
		return nil
	}
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Merge); err != nil {
		return err
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	dbName, branch := doltdb.SplitRevisionDbName(db.RevisionQualifiedName())
	ws, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return err
	}
	if !ws.MergeActive() {
		return incorrectPermsErr(ctx, branch)
	}
	hasConflicts, err := artM.HasArtifactOfType(ctx, prolly.ArtifactTypeConflict)
	if err != nil {
		return err
	}
	if !hasConflicts {
		return incorrectPermsErr(ctx, branch)
	}
	return nil
}

func incorrectPermsErr(ctx *sql.Context, branch string) error {
	if bas := branch_control.GetBranchAwareSession(ctx); bas != nil {
		return branch_control.ErrIncorrectPermissions.New(bas.GetUser(), bas.GetHost(), branch)
	}
	return branch_control.ErrIncorrectPermissions.New("", "", branch)
}
