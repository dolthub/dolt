// Copyright 2025 Dolthub, Inc.
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

package sqle

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// DoltProceduresDiffTable creates a dolt_procedures diff table that shows complete history
// like regular dolt_diff_ tables
func DoltProceduresDiffTable(ctx *sql.Context, ddb *doltdb.DoltDB, head *doltdb.Commit, workingRoot doltdb.RootValue, db Database) sql.Table {
	return &doltProceduresDiffTable{
		name:        doltdb.DoltDiffTablePrefix + doltdb.ProceduresTableName,
		ddb:         ddb,
		head:        head,
		workingRoot: workingRoot,
		db:          db,
	}
}

// doltProceduresDiffTable implements the dolt_diff_dolt_procedures system table with complete history
// It follows the same pattern as regular dolt_diff_ tables
type doltProceduresDiffTable struct {
	name        string
	ddb         *doltdb.DoltDB
	head        *doltdb.Commit
	workingRoot doltdb.RootValue
	db          Database
}

var _ sql.Table = (*doltProceduresDiffTable)(nil)
var _ sql.PrimaryKeyTable = (*doltProceduresDiffTable)(nil)

// Name implements sql.Table
func (dpdt *doltProceduresDiffTable) Name() string {
	return dpdt.name
}

// String implements sql.Table
func (dpdt *doltProceduresDiffTable) String() string {
	return dpdt.name
}

// Schema implements sql.Table
func (dpdt *doltProceduresDiffTable) Schema() sql.Schema {
	// Same schema as the regular diff table
	return sql.Schema{
		// TO columns
		&sql.Column{Name: doltdb.DiffToPrefix + doltdb.ProceduresTableNameCol, Type: gmstypes.MustCreateString(sqltypes.VarChar, 64, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dpdt.name},
		&sql.Column{Name: doltdb.DiffToPrefix + doltdb.ProceduresTableCreateStmtCol, Type: gmstypes.MustCreateString(sqltypes.VarChar, 4096, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dpdt.name},
		&sql.Column{Name: doltdb.DiffToPrefix + doltdb.ProceduresTableCreatedAtCol, Type: gmstypes.Timestamp, Nullable: true, Source: dpdt.name},
		&sql.Column{Name: doltdb.DiffToPrefix + doltdb.ProceduresTableModifiedAtCol, Type: gmstypes.Timestamp, Nullable: true, Source: dpdt.name},
		&sql.Column{Name: doltdb.DiffToPrefix + doltdb.ProceduresTableSqlModeCol, Type: gmstypes.MustCreateString(sqltypes.VarChar, 256, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dpdt.name},
		&sql.Column{Name: doltdb.ToCommitCol, Type: gmstypes.MustCreateString(sqltypes.VarChar, 1023, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dpdt.name},
		&sql.Column{Name: doltdb.ToCommitDateCol, Type: gmstypes.DatetimeMaxPrecision, Nullable: true, Source: dpdt.name},

		// FROM columns
		&sql.Column{Name: doltdb.DiffFromPrefix + doltdb.ProceduresTableNameCol, Type: gmstypes.MustCreateString(sqltypes.VarChar, 64, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dpdt.name},
		&sql.Column{Name: doltdb.DiffFromPrefix + doltdb.ProceduresTableCreateStmtCol, Type: gmstypes.MustCreateString(sqltypes.VarChar, 4096, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dpdt.name},
		&sql.Column{Name: doltdb.DiffFromPrefix + doltdb.ProceduresTableCreatedAtCol, Type: gmstypes.Timestamp, Nullable: true, Source: dpdt.name},
		&sql.Column{Name: doltdb.DiffFromPrefix + doltdb.ProceduresTableModifiedAtCol, Type: gmstypes.Timestamp, Nullable: true, Source: dpdt.name},
		&sql.Column{Name: doltdb.DiffFromPrefix + doltdb.ProceduresTableSqlModeCol, Type: gmstypes.MustCreateString(sqltypes.VarChar, 256, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dpdt.name},
		&sql.Column{Name: doltdb.FromCommitCol, Type: gmstypes.MustCreateString(sqltypes.VarChar, 1023, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dpdt.name},
		&sql.Column{Name: doltdb.FromCommitDateCol, Type: gmstypes.DatetimeMaxPrecision, Nullable: true, Source: dpdt.name},

		// Diff type column
		&sql.Column{Name: doltdb.DiffTypeCol, Type: gmstypes.MustCreateString(sqltypes.VarChar, 1023, sql.Collation_utf8mb4_0900_ai_ci), Nullable: false, Source: dpdt.name},
	}
}

// Collation implements sql.Table
func (dpdt *doltProceduresDiffTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions implements sql.Table - follows the pattern of regular diff tables
func (dpdt *doltProceduresDiffTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	// Create commit iterator for the entire history
	cmItr := doltdb.CommitItrForRoots[*sql.Context](dpdt.ddb, dpdt.head)

	// Set up commit iterator like regular diff tables
	err := cmItr.Reset(ctx)
	if err != nil {
		return nil, err
	}

	return &DoltProceduresDiffPartitionItr{
		cmItr:                cmItr,
		db:                   dpdt.db,
		head:                 dpdt.head,
		workingRoot:          dpdt.workingRoot,
		workingPartitionDone: false,
	}, nil
}

// PartitionRows implements sql.Table
func (dpdt *doltProceduresDiffTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	p := partition.(*DoltProceduresDiffPartition)
	return p.GetRowIter(ctx)
}

// PrimaryKeySchema implements sql.PrimaryKeyTable
func (dpdt *doltProceduresDiffTable) PrimaryKeySchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     dpdt.Schema(),
		PkOrdinals: []int{0}, // to_name
	}
}

// DoltProceduresDiffPartitionItr iterates through commit history for procedure diffs
type DoltProceduresDiffPartitionItr struct {
	cmItr                doltdb.CommitItr[*sql.Context]
	db                   Database
	head                 *doltdb.Commit
	workingRoot          doltdb.RootValue
	workingPartitionDone bool
}

var _ sql.PartitionIter = (*DoltProceduresDiffPartitionItr)(nil)

// Next implements sql.PartitionIter
func (dpdp *DoltProceduresDiffPartitionItr) Next(ctx *sql.Context) (sql.Partition, error) {
	// First iterate through commit history, then add working partition as the final step
	for {
		cmHash, optCmt, err := dpdp.cmItr.Next(ctx)
		if err == io.EOF {
			// Finished with commit history, now add working partition if not done
			if !dpdp.workingPartitionDone {
				dpdp.workingPartitionDone = true
				partition, err := dpdp.createWorkingPartition(ctx)
				if err != nil {
					return nil, fmt.Errorf("failed to create working partition: %w", err)
				}
				return partition, nil
			}
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}

		cm, ok := optCmt.ToCommit()
		if !ok {
			return nil, doltdb.ErrGhostCommitRuntimeFailure
		}

		// Get commit info for this commit
		cmRoot, err := cm.GetRootValue(ctx)
		if err != nil {
			return nil, err
		}

		// Check if this commit has a procedures table
		cmProceduresTable, cmExists, err := cmRoot.GetTable(ctx, doltdb.TableName{Name: doltdb.ProceduresTableName})
		if err != nil {
			return nil, err
		}

		// Get parent commit for comparison
		parentHashes, err := cm.ParentHashes(ctx)
		if err != nil {
			return nil, err
		}

		// For simplicity, only use the first parent (TODO: handle merge commits)
		if len(parentHashes) == 0 {
			// This is the initial commit, compare with empty state
			if cmExists {
				// Create partition comparing empty state to this commit
				cmMeta, err := cm.GetCommitMeta(ctx)
				if err != nil {
					return nil, err
				}
				cmCommitDate := types.Timestamp(cmMeta.Time())

				return &DoltProceduresDiffPartition{
					toTable:   cmProceduresTable,
					fromTable: nil, // Empty state
					toName:    cmHash.String(),
					fromName:  doltdb.EmptyCommitRef,
					toDate:    &cmCommitDate,
					fromDate:  nil,
					toRoot:    cmRoot,
					fromRoot:  nil, // No root for empty state
					db:        dpdp.db,
				}, nil
			}
			continue // Skip if no procedures table in initial commit
		}

		// Get parent commit
		parentOptCmt, err := cm.GetParent(ctx, 0)
		if err != nil {
			return nil, err
		}
		parentCm, ok := parentOptCmt.ToCommit()
		if !ok {
			return nil, doltdb.ErrGhostCommitEncountered
		}

		parentRoot, err := parentCm.GetRootValue(ctx)
		if err != nil {
			return nil, err
		}

		parentProceduresTable, parentExists, err := parentRoot.GetTable(ctx, doltdb.TableName{Name: doltdb.ProceduresTableName})
		if err != nil {
			return nil, err
		}

		// Check if procedures table changed between parent and this commit
		var cmTblHash, parentTblHash hash.Hash
		if cmExists {
			cmTblHash, _, err = cmRoot.GetTableHash(ctx, doltdb.TableName{Name: doltdb.ProceduresTableName})
			if err != nil {
				return nil, err
			}
		}
		if parentExists {
			parentTblHash, _, err = parentRoot.GetTableHash(ctx, doltdb.TableName{Name: doltdb.ProceduresTableName})
			if err != nil {
				return nil, err
			}
		}

		// If table hashes are different or existence changed, create a diff partition
		if cmTblHash != parentTblHash || cmExists != parentExists {
			cmMeta, err := cm.GetCommitMeta(ctx)
			if err != nil {
				return nil, err
			}
			cmCommitDate := types.Timestamp(cmMeta.Time())

			parentMeta, err := parentCm.GetCommitMeta(ctx)
			if err != nil {
				return nil, err
			}
			parentCommitDate := types.Timestamp(parentMeta.Time())

			parentHash, err := parentCm.HashOf()
			if err != nil {
				return nil, err
			}

			return &DoltProceduresDiffPartition{
				toTable:   cmProceduresTable,
				fromTable: parentProceduresTable,
				toName:    cmHash.String(),
				fromName:  parentHash.String(),
				toDate:    &cmCommitDate,
				fromDate:  &parentCommitDate,
				toRoot:    cmRoot,
				fromRoot:  parentRoot,
				db:        dpdp.db,
			}, nil
		}
	}
}

func (dpdp *DoltProceduresDiffPartitionItr) createWorkingPartition(ctx *sql.Context) (sql.Partition, error) {
	// Get HEAD commit details
	headRoot, err := dpdp.head.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	headCommitHash, err := dpdp.head.HashOf()
	if err != nil {
		return nil, err
	}

	headMeta, err := dpdp.head.GetCommitMeta(ctx)
	if err != nil {
		return nil, err
	}
	headCommitDate := types.Timestamp(headMeta.Time())

	headProceduresTable, _, err := headRoot.GetTable(ctx, doltdb.TableName{Name: doltdb.ProceduresTableName})
	if err != nil {
		return nil, err
	}

	workingProceduresTable, _, err := dpdp.workingRoot.GetTable(ctx, doltdb.TableName{Name: doltdb.ProceduresTableName})
	if err != nil {
		return nil, err
	}

	return &DoltProceduresDiffPartition{
		toTable:   workingProceduresTable,
		fromTable: headProceduresTable,
		toName:    doltdb.WorkingCommitRef,
		fromName:  headCommitHash.String(),
		toDate:    nil,
		fromDate:  &headCommitDate,
		toRoot:    dpdp.workingRoot,
		fromRoot:  headRoot,
		db:        dpdp.db,
	}, nil
}

// Close implements sql.PartitionIter
func (dpdp *DoltProceduresDiffPartitionItr) Close(ctx *sql.Context) error {
	return nil
}

// DoltProceduresDiffPartition represents a single diff between two commit states
type DoltProceduresDiffPartition struct {
	toTable   *doltdb.Table
	fromTable *doltdb.Table
	toName    string
	fromName  string
	toDate    *types.Timestamp
	fromDate  *types.Timestamp
	toRoot    doltdb.RootValue
	fromRoot  doltdb.RootValue
	db        Database
}

var _ sql.Partition = (*DoltProceduresDiffPartition)(nil)

// Key implements sql.Partition
func (dpdp *DoltProceduresDiffPartition) Key() []byte {
	return []byte(dpdp.toName + dpdp.fromName)
}

// GetRowIter implements sql.Partition
func (dpdp *DoltProceduresDiffPartition) GetRowIter(ctx *sql.Context) (sql.RowIter, error) {
	// Create a special diff iterator just for this partition
	return &doltProceduresDiffPartitionRowIter{
		ctx:       ctx,
		toTable:   dpdp.toTable,
		fromTable: dpdp.fromTable,
		toName:    dpdp.toName,
		fromName:  dpdp.fromName,
		toDate:    dpdp.toDate,
		fromDate:  dpdp.fromDate,
		toRoot:    dpdp.toRoot,
		fromRoot:  dpdp.fromRoot,
		db:        dpdp.db,
		done:      false,
	}, nil
}

// doltProceduresDiffPartitionRowIter implements a row iterator for a single diff partition
type doltProceduresDiffPartitionRowIter struct {
	ctx       *sql.Context
	toTable   *doltdb.Table
	fromTable *doltdb.Table
	toName    string
	fromName  string
	toDate    *types.Timestamp
	fromDate  *types.Timestamp
	toRoot    doltdb.RootValue
	fromRoot  doltdb.RootValue
	db        Database
	rows      []sql.Row
	idx       int
	done      bool
}

// Next implements sql.RowIter
func (dppri *doltProceduresDiffPartitionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if dppri.rows == nil && !dppri.done {
		// Initialize diff rows for this specific commit pair
		err := dppri.loadDiffRowsForCommitPair()
		if err != nil {
			return nil, err
		}
	}

	if dppri.idx >= len(dppri.rows) {
		return nil, io.EOF
	}

	row := dppri.rows[dppri.idx]
	dppri.idx++
	return row, nil
}

func (dppri *doltProceduresDiffPartitionRowIter) loadDiffRowsForCommitPair() error {
	// Build maps of procedure rows for comparison
	fromRows := make(map[string]sql.Row)
	toRows := make(map[string]sql.Row)

	// Read from table if it exists
	if dppri.fromTable != nil && dppri.fromRoot != nil {
		if err := dppri.readDoltProceduresRowsFromRoot(dppri.fromTable, dppri.fromRoot, fromRows); err != nil {
			return err
		}
	}

	// Read to table if it exists
	if dppri.toTable != nil {
		if err := dppri.readDoltProceduresRowsFromRoot(dppri.toTable, dppri.toRoot, toRows); err != nil {
			return err
		}
	}

	// Generate diff rows
	rows := make([]sql.Row, 0)

	// Find added and modified rows
	for key, toRow := range toRows {
		if fromRow, exists := fromRows[key]; exists {
			// Compare rows to see if modified
			if !procedureRowsEqual(fromRow, toRow) {
				// Modified row: to_* columns from toRow, from_* columns from fromRow
				diffRow := dppri.createDiffRow(toRow, fromRow, doltdb.DiffTypeModified)
				rows = append(rows, diffRow)
			}
		} else {
			// Added row: to_* columns from toRow, from_* columns are null
			diffRow := dppri.createDiffRow(toRow, nil, doltdb.DiffTypeAdded)
			rows = append(rows, diffRow)
		}
	}

	// Find removed rows
	for key, fromRow := range fromRows {
		if _, exists := toRows[key]; !exists {
			// Removed row: to_* columns are null, from_* columns from fromRow
			diffRow := dppri.createDiffRow(nil, fromRow, doltdb.DiffTypeRemoved)
			rows = append(rows, diffRow)
		}
	}

	dppri.rows = rows
	dppri.done = true
	return nil
}

func (dppri *doltProceduresDiffPartitionRowIter) readDoltProceduresRowsFromRoot(tbl *doltdb.Table, root doltdb.RootValue, rowMap map[string]sql.Row) error {
	if tbl == nil {
		return nil // Empty table, no rows to read
	}

	// Get the schema from the table
	sch, err := tbl.GetSchema(dppri.ctx)
	if err != nil {
		return err
	}

	// Create a DoltTable using the database reference we have
	doltTable, err := NewDoltTable(doltdb.ProceduresTableName, sch, tbl, dppri.db, editor.Options{})
	if err != nil {
		return err
	}

	// Lock the table to the specific root
	lockedTable, err := doltTable.LockedToRoot(dppri.ctx, root)
	if err != nil {
		return err
	}

	// Get partitions and read rows
	partitions, err := lockedTable.Partitions(dppri.ctx)
	if err != nil {
		return err
	}

	var baseRows []sql.Row
	for {
		partition, err := partitions.Next(dppri.ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		rowIter, err := lockedTable.PartitionRows(dppri.ctx, partition)
		if err != nil {
			return err
		}

		for {
			row, err := rowIter.Next(dppri.ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			baseRows = append(baseRows, row)
		}

		err = rowIter.Close(dppri.ctx)
		if err != nil {
			return err
		}
	}

	err = partitions.Close(dppri.ctx)
	if err != nil {
		return err
	}

	// Process each row and add to map
	for _, row := range baseRows {
		// Create key from name column (procedures table has name as primary key)
		if len(row) >= 1 && row[0] != nil {
			key := strings.ToLower(row[0].(string))
			rowMap[key] = row
		}
	}

	return nil
}

// createDiffRow creates a diff row with proper to_* and from_* column layout
func (dppri *doltProceduresDiffPartitionRowIter) createDiffRow(toRow, fromRow sql.Row, diffType string) sql.Row {
	// Expected schema: 7 to_* columns + 7 from_* columns + 1 diff_type = 15 columns
	row := make(sql.Row, 15)

	// TO columns (indices 0-6)
	if toRow != nil && len(toRow) >= 5 {
		copy(row[0:5], toRow[0:5]) // to_name, to_create_stmt, to_created_at, to_modified_at, to_sql_mode
		row[5] = dppri.toName      // to_commit
		if dppri.toDate != nil {
			row[6] = time.Time(*dppri.toDate) // to_commit_date converted to time.Time
		} else {
			row[6] = nil // to_commit_date
		}
	} else {
		// to_* procedure columns are null for removed rows, but commit info should be populated
		for i := 0; i < 5; i++ {
			row[i] = nil
		}
		row[5] = dppri.toName // to_commit should always be populated
		if dppri.toDate != nil {
			row[6] = time.Time(*dppri.toDate) // to_commit_date converted to time.Time
		} else {
			row[6] = nil // to_commit_date
		}
	}

	// FROM columns (indices 7-13)
	if fromRow != nil && len(fromRow) >= 5 {
		copy(row[7:12], fromRow[0:5]) // from_name, from_create_stmt, from_created_at, from_modified_at, from_sql_mode
		row[12] = dppri.fromName      // from_commit
		if dppri.fromDate != nil {
			row[13] = time.Time(*dppri.fromDate) // from_commit_date converted to time.Time
		} else {
			row[13] = nil // from_commit_date
		}
	} else {
		// from_* procedure columns are null for added rows, but commit info should be populated
		for i := 7; i < 12; i++ {
			row[i] = nil
		}
		row[12] = dppri.fromName // from_commit should always be populated
		if dppri.fromDate != nil {
			row[13] = time.Time(*dppri.fromDate) // from_commit_date converted to time.Time
		} else {
			row[13] = nil // from_commit_date
		}
	}

	// Diff type column (index 14)
	row[14] = diffType

	return row
}

// Close implements sql.RowIter
func (dppri *doltProceduresDiffPartitionRowIter) Close(ctx *sql.Context) error {
	return nil
}

// procedureRowsEqual compares two SQL rows for equality (procedures-specific version)
func procedureRowsEqual(row1, row2 sql.Row) bool {
	if len(row1) != len(row2) {
		return false
	}

	for i, val1 := range row1 {
		val2 := row2[i]
		if val1 == nil && val2 == nil {
			continue
		}
		if val1 == nil || val2 == nil {
			return false
		}

		// Handle different types by converting to string for comparison
		str1 := fmt.Sprintf("%v", val1)
		str2 := fmt.Sprintf("%v", val2)
		if str1 != str2 {
			return false
		}
	}

	return true
}