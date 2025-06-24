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

const (
	// DiffTypeCol is the column name for the type of change (added, modified, removed)
	DiffTypeCol = "diff_type"
)

// DoltSchemasDiffTable creates a dolt_schemas diff table that shows complete history
// like regular dolt_diff_ tables
func DoltSchemasDiffTable(ctx *sql.Context, ddb *doltdb.DoltDB, head *doltdb.Commit, workingRoot doltdb.RootValue, db Database) sql.Table {
	return &doltSchemasDiffTable{
		name:        doltdb.DoltDiffTablePrefix + doltdb.SchemasTableName,
		ddb:         ddb,
		head:        head,
		workingRoot: workingRoot,
		db:          db,
	}
}

// doltSchemasDiffTable implements the dolt_diff_dolt_schemas system table with complete history
// It follows the same pattern as regular dolt_diff_ tables
type doltSchemasDiffTable struct {
	name        string
	ddb         *doltdb.DoltDB
	head        *doltdb.Commit
	workingRoot doltdb.RootValue
	db          Database
}

var _ sql.Table = (*doltSchemasDiffTable)(nil)
var _ sql.PrimaryKeyTable = (*doltSchemasDiffTable)(nil)

// Name implements sql.Table
func (dsdt *doltSchemasDiffTable) Name() string {
	return dsdt.name
}

// String implements sql.Table
func (dsdt *doltSchemasDiffTable) String() string {
	return dsdt.name
}

// Schema implements sql.Table
func (dsdt *doltSchemasDiffTable) Schema() sql.Schema {
	// Same schema as the regular diff table
	return sql.Schema{
		// TO columns
		&sql.Column{Name: "to_" + doltdb.SchemasTablesTypeCol, Type: gmstypes.MustCreateString(sqltypes.VarChar, 64, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "to_" + doltdb.SchemasTablesNameCol, Type: gmstypes.MustCreateString(sqltypes.VarChar, 64, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "to_" + doltdb.SchemasTablesFragmentCol, Type: gmstypes.LongText, Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "to_" + doltdb.SchemasTablesExtraCol, Type: gmstypes.JSON, Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "to_" + doltdb.SchemasTablesSqlModeCol, Type: gmstypes.MustCreateString(sqltypes.VarChar, 256, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "to_commit", Type: gmstypes.MustCreateString(sqltypes.VarChar, 1023, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "to_commit_date", Type: gmstypes.DatetimeMaxPrecision, Nullable: true, Source: dsdt.name},

		// FROM columns
		&sql.Column{Name: "from_" + doltdb.SchemasTablesTypeCol, Type: gmstypes.MustCreateString(sqltypes.VarChar, 64, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "from_" + doltdb.SchemasTablesNameCol, Type: gmstypes.MustCreateString(sqltypes.VarChar, 64, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "from_" + doltdb.SchemasTablesFragmentCol, Type: gmstypes.LongText, Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "from_" + doltdb.SchemasTablesExtraCol, Type: gmstypes.JSON, Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "from_" + doltdb.SchemasTablesSqlModeCol, Type: gmstypes.MustCreateString(sqltypes.VarChar, 256, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "from_commit", Type: gmstypes.MustCreateString(sqltypes.VarChar, 1023, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "from_commit_date", Type: gmstypes.DatetimeMaxPrecision, Nullable: true, Source: dsdt.name},

		// Diff type column
		&sql.Column{Name: DiffTypeCol, Type: gmstypes.MustCreateString(sqltypes.VarChar, 1023, sql.Collation_utf8mb4_0900_ai_ci), Nullable: false, Source: dsdt.name},
	}
}

// Collation implements sql.Table
func (dsdt *doltSchemasDiffTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions implements sql.Table - follows the pattern of regular diff tables
func (dsdt *doltSchemasDiffTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	// Create commit iterator for the entire history
	cmItr := doltdb.CommitItrForRoots[*sql.Context](dsdt.ddb, dsdt.head)

	// Set up commit iterator like regular diff tables
	err := cmItr.Reset(ctx)
	if err != nil {
		return nil, err
	}

	return &DoltSchemasDiffPartitionItr{
		cmItr:                cmItr,
		db:                   dsdt.db,
		head:                 dsdt.head,
		workingRoot:          dsdt.workingRoot,
		workingPartitionDone: false,
	}, nil
}

// PartitionRows implements sql.Table
func (dsdt *doltSchemasDiffTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	p := partition.(*DoltSchemasDiffPartition)
	return p.GetRowIter(ctx)
}

// PrimaryKeySchema implements sql.PrimaryKeyTable
func (dsdt *doltSchemasDiffTable) PrimaryKeySchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     dsdt.Schema(),
		PkOrdinals: []int{0, 1}, // to_type, to_name
	}
}


// DoltSchemasDiffPartitionItr iterates through commit history for schema diffs
type DoltSchemasDiffPartitionItr struct {
	cmItr                doltdb.CommitItr[*sql.Context]
	db                   Database
	head                 *doltdb.Commit
	workingRoot          doltdb.RootValue
	workingPartitionDone bool
}

var _ sql.PartitionIter = (*DoltSchemasDiffPartitionItr)(nil)

// Next implements sql.PartitionIter
func (dsdp *DoltSchemasDiffPartitionItr) Next(ctx *sql.Context) (sql.Partition, error) {
	// First iterate through commit history, then add working partition as the final step
	for {
		cmHash, optCmt, err := dsdp.cmItr.Next(ctx)
		if err == io.EOF {
			// Finished with commit history, now add working partition if not done
			if !dsdp.workingPartitionDone {
				dsdp.workingPartitionDone = true
				partition, err := dsdp.createWorkingPartition(ctx)
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

		// Check if this commit has a schemas table
		cmSchemasTable, cmExists, err := cmRoot.GetTable(ctx, doltdb.TableName{Name: doltdb.SchemasTableName})
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

				return &DoltSchemasDiffPartition{
					toTable:   cmSchemasTable,
					fromTable: nil, // Empty state
					toName:    cmHash.String(),
					fromName:  "EMPTY",
					toDate:    &cmCommitDate,
					fromDate:  nil,
					toRoot:    cmRoot,
					fromRoot:  nil, // No root for empty state
					db:        dsdp.db,
				}, nil
			}
			continue // Skip if no schemas table in initial commit
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

		parentSchemasTable, parentExists, err := parentRoot.GetTable(ctx, doltdb.TableName{Name: doltdb.SchemasTableName})
		if err != nil {
			return nil, err
		}

		// Check if schemas table changed between parent and this commit
		var cmTblHash, parentTblHash hash.Hash
		if cmExists {
			cmTblHash, _, err = cmRoot.GetTableHash(ctx, doltdb.TableName{Name: doltdb.SchemasTableName})
			if err != nil {
				return nil, err
			}
		}
		if parentExists {
			parentTblHash, _, err = parentRoot.GetTableHash(ctx, doltdb.TableName{Name: doltdb.SchemasTableName})
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

			return &DoltSchemasDiffPartition{
				toTable:   cmSchemasTable,
				fromTable: parentSchemasTable,
				toName:    cmHash.String(),
				fromName:  parentHash.String(),
				toDate:    &cmCommitDate,
				fromDate:  &parentCommitDate,
				toRoot:    cmRoot,
				fromRoot:  parentRoot,
				db:        dsdp.db,
			}, nil
		}
	}
}

func (dsdp *DoltSchemasDiffPartitionItr) createWorkingPartition(ctx *sql.Context) (sql.Partition, error) {
	// Get HEAD commit details
	headRoot, err := dsdp.head.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	headCommitHash, err := dsdp.head.HashOf()
	if err != nil {
		return nil, err
	}

	headMeta, err := dsdp.head.GetCommitMeta(ctx)
	if err != nil {
		return nil, err
	}
	headCommitDate := types.Timestamp(headMeta.Time())

	headSchemasTable, _, err := headRoot.GetTable(ctx, doltdb.TableName{Name: doltdb.SchemasTableName})
	if err != nil {
		return nil, err
	}

	workingSchemasTable, _, err := dsdp.workingRoot.GetTable(ctx, doltdb.TableName{Name: doltdb.SchemasTableName})
	if err != nil {
		return nil, err
	}

	return &DoltSchemasDiffPartition{
		toTable:   workingSchemasTable,
		fromTable: headSchemasTable,
		toName:    "WORKING",
		fromName:  headCommitHash.String(),
		toDate:    nil,
		fromDate:  &headCommitDate,
		toRoot:    dsdp.workingRoot,
		fromRoot:  headRoot,
		db:        dsdp.db,
	}, nil
}

// Close implements sql.PartitionIter
func (dsdp *DoltSchemasDiffPartitionItr) Close(ctx *sql.Context) error {
	return nil
}

// DoltSchemasDiffPartition represents a single diff between two commit states
type DoltSchemasDiffPartition struct {
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

var _ sql.Partition = (*DoltSchemasDiffPartition)(nil)

// Key implements sql.Partition
func (dsdp *DoltSchemasDiffPartition) Key() []byte {
	return []byte(dsdp.toName + dsdp.fromName)
}

// GetRowIter implements sql.Partition
func (dsdp *DoltSchemasDiffPartition) GetRowIter(ctx *sql.Context) (sql.RowIter, error) {
	// Create a special diff iterator just for this partition
	return &doltSchemasDiffPartitionRowIter{
		ctx:       ctx,
		toTable:   dsdp.toTable,
		fromTable: dsdp.fromTable,
		toName:    dsdp.toName,
		fromName:  dsdp.fromName,
		toDate:    dsdp.toDate,
		fromDate:  dsdp.fromDate,
		toRoot:    dsdp.toRoot,
		fromRoot:  dsdp.fromRoot,
		db:        dsdp.db,
		done:      false,
	}, nil
}

// doltSchemasDiffPartitionRowIter implements a row iterator for a single diff partition
type doltSchemasDiffPartitionRowIter struct {
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
func (dspri *doltSchemasDiffPartitionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if dspri.rows == nil && !dspri.done {
		// Initialize diff rows for this specific commit pair
		err := dspri.loadDiffRowsForCommitPair()
		if err != nil {
			return nil, err
		}
	}

	if dspri.idx >= len(dspri.rows) {
		return nil, io.EOF
	}

	row := dspri.rows[dspri.idx]
	dspri.idx++
	return row, nil
}

func (dspri *doltSchemasDiffPartitionRowIter) loadDiffRowsForCommitPair() error {
	// Build maps of schema rows for comparison
	fromRows := make(map[string]sql.Row)
	toRows := make(map[string]sql.Row)

	// Read from table if it exists
	if dspri.fromTable != nil && dspri.fromRoot != nil {
		if err := dspri.readDoltSchemasRowsFromRoot(dspri.fromTable, dspri.fromRoot, fromRows); err != nil {
			return err
		}
	}

	// Read to table if it exists
	if dspri.toTable != nil {
		if err := dspri.readDoltSchemasRowsFromRoot(dspri.toTable, dspri.toRoot, toRows); err != nil {
			return err
		}
	}

	// Generate diff rows
	rows := make([]sql.Row, 0)

	// Find added and modified rows
	for key, toRow := range toRows {
		if fromRow, exists := fromRows[key]; exists {
			// Compare rows to see if modified
			if !rowsEqual(fromRow, toRow) {
				// Modified row: to_* columns from toRow, from_* columns from fromRow
				diffRow := dspri.createDiffRow(toRow, fromRow, "modified")
				rows = append(rows, diffRow)
			}
		} else {
			// Added row: to_* columns from toRow, from_* columns are null
			diffRow := dspri.createDiffRow(toRow, nil, "added")
			rows = append(rows, diffRow)
		}
	}

	// Find removed rows
	for key, fromRow := range fromRows {
		if _, exists := toRows[key]; !exists {
			// Removed row: to_* columns are null, from_* columns from fromRow
			diffRow := dspri.createDiffRow(nil, fromRow, "removed")
			rows = append(rows, diffRow)
		}
	}

	dspri.rows = rows
	dspri.done = true
	return nil
}

func (dspri *doltSchemasDiffPartitionRowIter) readDoltSchemasRowsFromRoot(tbl *doltdb.Table, root doltdb.RootValue, rowMap map[string]sql.Row) error {
	if tbl == nil {
		return nil // Empty table, no rows to read
	}

	// Get the schema from the table
	sch, err := tbl.GetSchema(dspri.ctx)
	if err != nil {
		return err
	}

	// Create a DoltTable using the database reference we have
	doltTable, err := NewDoltTable(doltdb.SchemasTableName, sch, tbl, dspri.db, editor.Options{})
	if err != nil {
		return err
	}

	// Lock the table to the specific root
	lockedTable, err := doltTable.LockedToRoot(dspri.ctx, root)
	if err != nil {
		return err
	}

	// Get partitions and read rows
	partitions, err := lockedTable.Partitions(dspri.ctx)
	if err != nil {
		return err
	}

	var baseRows []sql.Row
	for {
		partition, err := partitions.Next(dspri.ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		rowIter, err := lockedTable.PartitionRows(dspri.ctx, partition)
		if err != nil {
			return err
		}

		for {
			row, err := rowIter.Next(dspri.ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			baseRows = append(baseRows, row)
		}

		err = rowIter.Close(dspri.ctx)
		if err != nil {
			return err
		}
	}

	err = partitions.Close(dspri.ctx)
	if err != nil {
		return err
	}

	// Process each row and add to map
	for _, row := range baseRows {
		// Create key from type and name columns
		if len(row) >= 2 && row[0] != nil && row[1] != nil {
			key := strings.ToLower(row[0].(string)) + ":" + strings.ToLower(row[1].(string))
			rowMap[key] = row
		}
	}

	return nil
}

// createDiffRow creates a diff row with proper to_* and from_* column layout
func (dspri *doltSchemasDiffPartitionRowIter) createDiffRow(toRow, fromRow sql.Row, diffType string) sql.Row {
	// Expected schema: 7 to_* columns + 7 from_* columns + 1 diff_type = 15 columns
	row := make(sql.Row, 15)

	// TO columns (indices 0-6)
	if toRow != nil && len(toRow) >= 5 {
		copy(row[0:5], toRow[0:5]) // to_type, to_name, to_fragment, to_extra, to_sql_mode
		row[5] = dspri.toName      // to_commit
		if dspri.toDate != nil {
			row[6] = time.Time(*dspri.toDate) // to_commit_date converted to time.Time
		} else {
			row[6] = nil // to_commit_date
		}
	} else {
		// to_* schema columns are null for removed rows, but commit info should be populated
		for i := 0; i < 5; i++ {
			row[i] = nil
		}
		row[5] = dspri.toName // to_commit should always be populated
		if dspri.toDate != nil {
			row[6] = time.Time(*dspri.toDate) // to_commit_date converted to time.Time
		} else {
			row[6] = nil // to_commit_date
		}
	}

	// FROM columns (indices 7-13)
	if fromRow != nil && len(fromRow) >= 5 {
		copy(row[7:12], fromRow[0:5]) // from_type, from_name, from_fragment, from_extra, from_sql_mode
		row[12] = dspri.fromName      // from_commit
		if dspri.fromDate != nil {
			row[13] = time.Time(*dspri.fromDate) // from_commit_date converted to time.Time
		} else {
			row[13] = nil // from_commit_date
		}
	} else {
		// from_* schema columns are null for added rows, but commit info should be populated
		for i := 7; i < 12; i++ {
			row[i] = nil
		}
		row[12] = dspri.fromName // from_commit should always be populated
		if dspri.fromDate != nil {
			row[13] = time.Time(*dspri.fromDate) // from_commit_date converted to time.Time
		} else {
			row[13] = nil // from_commit_date
		}
	}

	// Diff type column (index 14)
	row[14] = diffType

	return row
}

// Close implements sql.RowIter
func (dspri *doltSchemasDiffPartitionRowIter) Close(ctx *sql.Context) error {
	return nil
}

// rowsEqual compares two SQL rows for equality
func rowsEqual(row1, row2 sql.Row) bool {
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

		// Handle JSON types specially - convert to string for comparison
		// JSON values might have different internal representations but same content
		str1 := fmt.Sprintf("%v", val1)
		str2 := fmt.Sprintf("%v", val2)
		if str1 != str2 {
			return false
		}
	}

	return true
}
