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
	{Name: "dolt_backup", Schema: int64Schema("success"), Function: doltBackup},
	{Name: "dolt_branch", Schema: int64Schema("status"), Function: doltBranch},
	{Name: "dolt_checkout", Schema: int64Schema("status"), Function: doltCheckout},
	{Name: "dolt_cherry_pick", Schema: stringSchema("hash"), Function: doltCherryPick},
	{Name: "dolt_clean", Schema: int64Schema("status"), Function: doltClean},
	{Name: "dolt_clone", Schema: int64Schema("status"), Function: doltClone},
	{Name: "dolt_commit", Schema: stringSchema("hash"), Function: doltCommit},
	{Name: "dolt_commit_hash_out", Schema: stringSchema("hash"), Function: doltCommitHashOut},
	{Name: "dolt_conflicts_resolve", Schema: int64Schema("status"), Function: doltConflictsResolve},
	{Name: "dolt_fetch", Schema: int64Schema("success"), Function: doltFetch},

	// dolt_gc is enabled behind a feature flag for now, see dolt_gc.go
	{Name: "dolt_gc", Schema: int64Schema("success"), Function: doltGC},

	{Name: "dolt_merge", Schema: int64Schema("fast_forward", "conflicts"), Function: doltMerge},
	{Name: "dolt_pull", Schema: int64Schema("fast_forward", "conflicts"), Function: doltPull},
	{Name: "dolt_push", Schema: int64Schema("success"), Function: doltPush},
	{Name: "dolt_remote", Schema: int64Schema("status"), Function: doltRemote},
	{Name: "dolt_reset", Schema: int64Schema("status"), Function: doltReset},
	{Name: "dolt_revert", Schema: int64Schema("status"), Function: doltRevert},
	{Name: "dolt_tag", Schema: int64Schema("status"), Function: doltTag},
	{Name: "dolt_verify_constraints", Schema: int64Schema("violations"), Function: doltVerifyConstraints},

	// Dolt stored procedure aliases
	// TODO: Add new procedure aliases in doltProcedureAliasSet in go-mysql-server/sql/information_schema/routines.go file
	{Name: "dadd", Schema: int64Schema("status"), Function: doltAdd},
	{Name: "dbranch", Schema: int64Schema("status"), Function: doltBranch},
	{Name: "dcheckout", Schema: int64Schema("status"), Function: doltCheckout},
	{Name: "dcherry_pick", Schema: stringSchema("hash"), Function: doltCherryPick},
	{Name: "dclean", Schema: int64Schema("status"), Function: doltClean},
	{Name: "dclone", Schema: int64Schema("status"), Function: doltClone},
	{Name: "dcommit", Schema: stringSchema("hash"), Function: doltCommit},
	{Name: "dfetch", Schema: int64Schema("success"), Function: doltFetch},

	//	{Name: "dgc", Schema: int64Schema("status"), Function: doltGC},

	{Name: "dmerge", Schema: int64Schema("fast_forward", "conflicts"), Function: doltMerge},
	{Name: "dpull", Schema: int64Schema("fast_forward", "conflicts"), Function: doltPull},
	{Name: "dpush", Schema: int64Schema("success"), Function: doltPush},
	{Name: "dremote", Schema: int64Schema("status"), Function: doltRemote},
	{Name: "dreset", Schema: int64Schema("status"), Function: doltReset},
	{Name: "drevert", Schema: int64Schema("status"), Function: doltRevert},
	{Name: "dtag", Schema: int64Schema("status"), Function: doltTag},
	{Name: "dverify_constraints", Schema: int64Schema("violations"), Function: doltVerifyConstraints},
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
