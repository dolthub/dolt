// Copyright 2022 Dolthub, Inc.
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

package dprocedures

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var DoltProcedures = []sql.ExternalStoredProcedureDetails{
	{Name: "dolt_add", Schema: int64Schema("status"), Function: doltAdd},
	{Name: "dolt_backup", Schema: int64Schema("status"), Function: doltBackup, ReadOnly: true, AdminOnly: true},
	{Name: "dolt_branch", Schema: int64Schema("status"), Function: doltBranch},
	{Name: "dolt_checkout", Schema: doltCheckoutSchema, Function: doltCheckout, ReadOnly: true},
	{Name: "dolt_cherry_pick", Schema: cherryPickSchema, Function: doltCherryPick},
	{Name: "dolt_clean", Schema: int64Schema("status"), Function: doltClean},
	{Name: "dolt_clone", Schema: int64Schema("status"), Function: doltClone, AdminOnly: true},
	{Name: "dolt_commit", Schema: stringSchema("hash"), Function: doltCommit},
	{Name: "dolt_commit_hash_out", Schema: stringSchema("hash"), Function: doltCommitHashOut},
	{Name: "dolt_conflicts_resolve", Schema: int64Schema("status"), Function: doltConflictsResolve},
	{Name: "dolt_count_commits", Schema: int64Schema("ahead", "behind"), Function: doltCountCommits, ReadOnly: true},
	{Name: "dolt_fetch", Schema: int64Schema("status"), Function: doltFetch, AdminOnly: true},
	{Name: "dolt_undrop", Schema: int64Schema("status"), Function: doltUndrop, AdminOnly: true},
	{Name: "dolt_purge_dropped_databases", Schema: int64Schema("status"), Function: doltPurgeDroppedDatabases, AdminOnly: true},
	{Name: "dolt_rebase", Schema: doltRebaseProcedureSchema, Function: doltRebase},

	// dolt_gc is enabled behind a feature flag for now, see dolt_gc.go
	{Name: "dolt_gc", Schema: int64Schema("status"), Function: doltGC, ReadOnly: true, AdminOnly: true},

	{Name: "dolt_merge", Schema: doltMergeSchema, Function: doltMerge},
	{Name: "dolt_pull", Schema: doltPullSchema, Function: doltPull, AdminOnly: true},
	{Name: "dolt_push", Schema: doltPushSchema, Function: doltPush, AdminOnly: true},
	{Name: "dolt_remote", Schema: int64Schema("status"), Function: doltRemote, AdminOnly: true},
	{Name: "dolt_reset", Schema: int64Schema("status"), Function: doltReset},
	{Name: "dolt_revert", Schema: int64Schema("status"), Function: doltRevert},
	{Name: "dolt_tag", Schema: int64Schema("status"), Function: doltTag},
	{Name: "dolt_verify_constraints", Schema: int64Schema("violations"), Function: doltVerifyConstraints},

	{Name: "dolt_stats_drop", Schema: statsFuncSchema, Function: statsFunc(statsDrop)},
	{Name: "dolt_stats_restart", Schema: statsFuncSchema, Function: statsFunc(statsRestart)},
	{Name: "dolt_stats_stop", Schema: statsFuncSchema, Function: statsFunc(statsStop)},
	{Name: "dolt_stats_status", Schema: statsFuncSchema, Function: statsFunc(statsStatus)},
	{Name: "dolt_stats_prune", Schema: statsFuncSchema, Function: statsFunc(statsPrune)},
	{Name: "dolt_stats_purge", Schema: statsFuncSchema, Function: statsFunc(statsPurge)},
}

// stringSchema returns a non-nullable schema with all columns as LONGTEXT.
func stringSchema(columnNames ...string) sql.Schema {
	sch := make(sql.Schema, len(columnNames))
	for i, colName := range columnNames {
		sch[i] = &sql.Column{
			Name:     colName,
			Type:     types.LongText,
			Nullable: false,
		}
	}
	return sch
}

// int64Schema returns a non-nullable schema with all columns as BIGINT.
func int64Schema(columnNames ...string) sql.Schema {
	sch := make(sql.Schema, len(columnNames))
	for i, colName := range columnNames {
		sch[i] = &sql.Column{
			Name:     colName,
			Type:     types.Int64,
			Nullable: false,
		}
	}
	return sch
}

// rowToIter returns a sql.RowIter with a single row containing the values passed in.
func rowToIter(vals ...interface{}) sql.RowIter {
	row := make(sql.Row, len(vals))
	for i, val := range vals {
		row[i] = val
	}
	return sql.RowsToRowIter(row)
}
