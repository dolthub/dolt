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

import "github.com/dolthub/go-mysql-server/sql"

var DoltProcedures = []sql.ExternalStoredProcedureDetails{
	{Name: "dolt_add", Schema: int64Schema("status"), Function: dolt_add},
	{Name: "dolt_branch", Schema: int64Schema("status"), Function: dolt_branch},
	{Name: "dolt_checkout", Schema: int64Schema("status"), Function: dolt_checkout},
	{Name: "dolt_commit", Schema: stringSchema("hash"), Function: dolt_commit},
	{Name: "dolt_fetch", Schema: int64Schema("success"), Function: dolt_fetch},
	{Name: "dolt_merge", Schema: int64Schema("no_conflicts"), Function: dolt_merge},
	{Name: "dolt_pull", Schema: int64Schema("no_conflicts"), Function: dolt_pull},
	{Name: "dolt_push", Schema: int64Schema("success"), Function: dolt_push},
	{Name: "dolt_reset", Schema: int64Schema("status"), Function: dolt_reset},
	{Name: "dolt_revert", Schema: int64Schema("status"), Function: dolt_revert},
	{Name: "dolt_verify_constraints", Schema: int64Schema("no_violations"), Function: dolt_verify_constraints},
	{Name: "dolt_verify_all_constraints", Schema: int64Schema("no_violations"), Function: dolt_verify_all_constraints},
	{Name: "dadd", Schema: int64Schema("status"), Function: dolt_add},
	{Name: "dbranch", Schema: int64Schema("status"), Function: dolt_branch},
	{Name: "dcheckout", Schema: int64Schema("status"), Function: dolt_checkout},
	{Name: "dcommit", Schema: stringSchema("hash"), Function: dolt_commit},
	{Name: "dfetch", Schema: int64Schema("success"), Function: dolt_fetch},
	{Name: "dmerge", Schema: int64Schema("no_conflicts"), Function: dolt_merge},
	{Name: "dpull", Schema: int64Schema("no_conflicts"), Function: dolt_pull},
	{Name: "dpush", Schema: int64Schema("success"), Function: dolt_push},
	{Name: "dreset", Schema: int64Schema("status"), Function: dolt_reset},
	{Name: "drevert", Schema: int64Schema("status"), Function: dolt_revert},
	{Name: "dverify_constraints", Schema: int64Schema("no_violations"), Function: dolt_verify_constraints},
	{Name: "dverify_all_constraints", Schema: int64Schema("no_violations"), Function: dolt_verify_all_constraints},
}

// stringSchema returns a non-nullable schema with all columns as LONGTEXT.
func stringSchema(columnNames ...string) sql.Schema {
	sch := make(sql.Schema, len(columnNames))
	for i, colName := range columnNames {
		sch[i] = &sql.Column{
			Name:     colName,
			Type:     sql.LongText,
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
			Type:     sql.Int64,
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
