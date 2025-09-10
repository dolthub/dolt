// Copyright 2020 Dolthub, Inc.
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
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/fk"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/hash"
	storetypes "github.com/dolthub/dolt/go/store/types"
)

const branchesDefaultRowCount = 10

// doltBranchesIndexName is the name of the index on the dolt_branches system table that covers
// the "name" field.
const doltBranchesIndexName = "dolt_branches_name_idx"

var _ sql.Table = (*BranchesTable)(nil)
var _ sql.StatisticsTable = (*BranchesTable)(nil)
var _ sql.UpdatableTable = (*BranchesTable)(nil)
var _ sql.DeletableTable = (*BranchesTable)(nil)
var _ sql.InsertableTable = (*BranchesTable)(nil)
var _ sql.ReplaceableTable = (*BranchesTable)(nil)
var _ sql.ForeignKeyTable = (*BranchesTable)(nil)
var _ sql.IndexedTable = (*BranchesTable)(nil)

// BranchesTable is the system table that accesses branches
type BranchesTable struct {
	db        dsess.SqlDatabase
	tableName string
	remote    bool
}

// LookupPartitions implements sql.IndexedTable
func (bt *BranchesTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if lookup.Index.ID() == doltBranchesIndexName {
		mysqlRanges, ok := lookup.Ranges.(sql.MySQLRangeCollection)
		if !ok {
			return nil, fmt.Errorf("unsupported range cut type: %T", lookup.Ranges)
		}

		var partitions []sql.Partition
		for i := range mysqlRanges.ToRanges() {
			mysqlRange := mysqlRanges.ToRanges()[i].(sql.MySQLRange)
			rangeExpr := mysqlRange[0]

			lowerBoundInclusive := false
			noLowerBoundResults := false
			var lowerBoundValue any
			switch x := rangeExpr.LowerBound.(type) {
			case sql.Above:
				lowerBoundValue = x.Key
			case sql.Below:
				lowerBoundValue = x.Key
				lowerBoundInclusive = true
			case sql.BelowNull, sql.AboveNull:
				// BelowNull and AboveNull for a lower bound means no lower bound
				// They evaluate the same, since name is a PK and will never be NULL.
				lowerBoundValue = ""
			case sql.AboveAll:
				noLowerBoundResults = true
				lowerBoundValue = ""
			default:
				return nil, fmt.Errorf("unknown range cut type: %T", rangeExpr.LowerBound)
			}

			upperBoundInclusive := false
			noUpperBoundResults := false
			var upperBoundValue any
			switch x := rangeExpr.UpperBound.(type) {
			case sql.Above:
				upperBoundValue = x.Key
				upperBoundInclusive = true
			case sql.Below:
				upperBoundValue = x.Key
			case sql.AboveAll:
				noUpperBoundResults = true
				upperBoundValue = ""
			case sql.BelowNull, sql.AboveNull:
				upperBoundValue = ""
			default:
				return nil, fmt.Errorf("unknown range cut type: %T", rangeExpr.UpperBound)
			}

			if noUpperBoundResults && noLowerBoundResults {
				continue
			}

			partitions = append(partitions, &filteredPartition{
				lowerBound:          lowerBoundValue.(string),
				lowerBoundInclusive: lowerBoundInclusive,
				upperBound:          upperBoundValue.(string),
				upperBoundInclusive: upperBoundInclusive,
			})
		}
		return NewSliceOfPartitionsItr(partitions), nil
	}

	return nil, fmt.Errorf("unsupported index: %s", lookup.Index.ID())
}

// IndexedAccess implements sql.IndexAddressable
func (bt *BranchesTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	return bt
}

// GetIndexes implements sql.IndexAddressable
func (bt *BranchesTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return []sql.Index{
		index.NewBranchNameIndex(
			index.MockIndex(doltBranchesIndexName,
				bt.db.Name(), bt.Name(), "name", storetypes.StringKind, true)),
	}, nil
}

// PreciseMatch implements sql.IndexAddressable
func (bt *BranchesTable) PreciseMatch() bool {
	return false
}

// CreateIndexForForeignKey implements sql.ForeignKeyTable
func (bt *BranchesTable) CreateIndexForForeignKey(ctx *sql.Context, indexDef sql.IndexDef) error {
	return fmt.Errorf("creating indexes on dolt system tables is not supported")
}

// GetDeclaredForeignKeys implements sql.ForeignKeyTable
func (bt *BranchesTable) GetDeclaredForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	// The dolt_branches system table does not declare any FKs
	return nil, nil
}

// GetReferencedForeignKeys implements sql.ForeignKeyTable
func (bt *BranchesTable) GetReferencedForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	databaseName := bt.db.Name()
	doltSession := dsess.DSessFromSess(ctx.Session)
	roots, ok := doltSession.GetRoots(ctx, databaseName)
	if !ok {
		return nil, fmt.Errorf("unable to get roots for database '%s'", databaseName)
	}

	pkSch := sql.NewPrimaryKeySchema(bt.Schema(), 0)
	tableName := doltdb.TableName{Name: "dolt_branches"}
	doltSchema, err := sqlutil.ToDoltSchema(ctx, roots.Working, tableName, pkSch, roots.Head, bt.Collation())
	if err != nil {
		return nil, err
	}

	return fk.GetReferencedForeignKeys(ctx, roots.Working, bt.db.Name(), tableName, doltSchema)
}

// AddForeignKey implements sql.ForeignKeyTable
func (bt *BranchesTable) AddForeignKey(ctx *sql.Context, fk sql.ForeignKeyConstraint) error {
	return fmt.Errorf("dolt system tables do not support adding foreign keys")
}

// DropForeignKey implements sql.ForeignKeyTable
func (bt *BranchesTable) DropForeignKey(ctx *sql.Context, fkName string) error {
	return fmt.Errorf("dolt system tables do not support dropping foreign keys")
}

// UpdateForeignKey implements sql.ForeignKeyTable
func (bt *BranchesTable) UpdateForeignKey(ctx *sql.Context, fkName string, fk sql.ForeignKeyConstraint) error {
	return fmt.Errorf("dolt_branches system table does not support updating foreign keys")
}

// GetForeignKeyEditor implements sql.ForeignKeyTable
func (bt *BranchesTable) GetForeignKeyEditor(ctx *sql.Context) sql.ForeignKeyEditor {
	return &systemTableForeignKeyEditor{
		indexedTable: bt,
	}
}

// NewBranchesTable creates a BranchesTable
func NewBranchesTable(_ *sql.Context, db dsess.SqlDatabase, tableName string) sql.Table {
	return &BranchesTable{db: db, tableName: tableName}
}

// NewRemoteBranchesTable creates a BranchesTable with only remote refs
func NewRemoteBranchesTable(_ *sql.Context, ddb dsess.SqlDatabase, tableName string) sql.Table {
	return &BranchesTable{db: ddb, remote: true, tableName: tableName}
}

func (bt *BranchesTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(bt.Schema())
	numRows, _, err := bt.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (bt *BranchesTable) RowCount(_ *sql.Context) (uint64, bool, error) {
	return branchesDefaultRowCount, false, nil
}

// Name is a sql.Table interface function which returns the name of the table
func (bt *BranchesTable) Name() string {
	return bt.tableName
}

// String is a sql.Table interface function which returns the name of the table
func (bt *BranchesTable) String() string {
	return bt.tableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the branches system table
func (bt *BranchesTable) Schema() sql.Schema {
	columns := []*sql.Column{
		{Name: "name", Type: types.Text, Source: bt.tableName, PrimaryKey: true, Nullable: false, DatabaseSource: bt.db.Name()},
		{Name: "hash", Type: types.Text, Source: bt.tableName, PrimaryKey: false, Nullable: false, DatabaseSource: bt.db.Name()},
		{Name: "latest_committer", Type: types.Text, Source: bt.tableName, PrimaryKey: false, Nullable: true, DatabaseSource: bt.db.Name()},
		{Name: "latest_committer_email", Type: types.Text, Source: bt.tableName, PrimaryKey: false, Nullable: true, DatabaseSource: bt.db.Name()},
		{Name: "latest_commit_date", Type: types.Datetime, Source: bt.tableName, PrimaryKey: false, Nullable: true, DatabaseSource: bt.db.Name()},
		{Name: "latest_commit_message", Type: types.Text, Source: bt.tableName, PrimaryKey: false, Nullable: true, DatabaseSource: bt.db.Name()},
	}
	if !bt.remote {
		columns = append(columns, &sql.Column{Name: "remote", Type: types.Text, Source: bt.tableName, PrimaryKey: false, Nullable: true})
		columns = append(columns, &sql.Column{Name: "branch", Type: types.Text, Source: bt.tableName, PrimaryKey: false, Nullable: true})
		columns = append(columns, &sql.Column{Name: "dirty", Type: types.Boolean, Source: bt.tableName, PrimaryKey: false, Nullable: true})
	}
	return columns
}

// Collation implements the sql.Table interface.
func (bt *BranchesTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data.  Currently the data is unpartitioned.
func (bt *BranchesTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (bt *BranchesTable) PartitionRows(sqlCtx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	if filteredPartition, ok := part.(*filteredPartition); ok {
		return NewFilteredBranchItr(sqlCtx, bt,
			filteredPartition.lowerBound, filteredPartition.lowerBoundInclusive,
			filteredPartition.upperBound, filteredPartition.upperBoundInclusive)
	}

	return NewBranchItr(sqlCtx, bt)
}

// BranchItr is a sql.RowItr implementation which iterates over each commit as if it's a row in the table.
type BranchItr struct {
	table    *BranchesTable
	branches []string
	commits  []*doltdb.Commit
	dirty    []bool
	idx      int

	// lowerBound is an optional filter to control the lowest (alphabetically) branch name
	// returned by this iterator
	lowerBound string
	// lowerBoundInclusive controls whether |lowerBound| is inclusive or not.
	lowerBoundInclusive bool
	// upperBound is an optional filter to control the highest (alphabetically) branch name
	// returned by this iterator
	upperBound string
	// upperBoundInclusive controls whether |upperBound| is inclusive or not.
	upperBoundInclusive bool
}

// NewFilteredBranchItr creates a BranchItr that filters out branch names lower that
// |lowerBound| and higher than |upperBound|.
func NewFilteredBranchItr(ctx *sql.Context, table *BranchesTable, lowerBound string, lowerBoundInclusive bool, upperBound string, upperBoundInclusive bool) (*BranchItr, error) {
	itr, err := NewBranchItr(ctx, table)
	if err != nil {
		return nil, err
	}

	itr.lowerBound = lowerBound
	itr.lowerBoundInclusive = lowerBoundInclusive
	itr.upperBound = upperBound
	itr.upperBoundInclusive = upperBoundInclusive
	return itr, nil
}

// NewBranchItr creates a BranchItr from the current environment.
func NewBranchItr(ctx *sql.Context, table *BranchesTable) (*BranchItr, error) {
	var branchRefs []ref.DoltRef
	var err error
	db := table.db
	remote := table.remote

	txRoot, err := dsess.TransactionRoot(ctx, db)
	if err != nil {
		return nil, err
	}

	ddb := db.DbData().Ddb

	if remote {
		branchRefs, err = ddb.GetRefsOfTypeByNomsRoot(ctx, map[ref.RefType]struct{}{ref.RemoteRefType: {}}, txRoot)
		if err != nil {
			return nil, err
		}
	} else {
		branchRefs, err = ddb.GetBranchesByNomsRoot(ctx, txRoot)
		if err != nil {
			return nil, err
		}
	}

	branchNames := make([]string, len(branchRefs))
	commits := make([]*doltdb.Commit, len(branchRefs))
	dirtyBits := make([]bool, len(branchRefs))
	for i, branch := range branchRefs {
		commit, err := ddb.ResolveCommitRefAtRoot(ctx, branch, txRoot)
		if err != nil {
			return nil, err
		}

		var dirty bool
		if !remote {
			dirty, err = isDirty(ctx, ddb, commit, branch, txRoot)
			if err != nil {
				return nil, err
			}
		}

		if branch.GetType() == ref.RemoteRefType {
			branchNames[i] = "remotes/" + branch.GetPath()
		} else {
			branchNames[i] = branch.GetPath()
		}

		dirtyBits[i] = dirty
		commits[i] = commit
	}

	return &BranchItr{
		table:    table,
		branches: branchNames,
		commits:  commits,
		dirty:    dirtyBits,
		idx:      0,
	}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed. If an upper
// or lower bound has been configured, this function will filter out branch
// names outside those bounds.
func (itr *BranchItr) Next(ctx *sql.Context) (sql.Row, error) {
	defer func() {
		itr.idx++
	}()

	for {
		if itr.idx >= len(itr.commits) {
			return nil, io.EOF
		}

		name := itr.branches[itr.idx]
		if itr.outOfLowerBound(name) || itr.outOfUpperBound(name) {
			itr.idx++
			continue
		}

		cm := itr.commits[itr.idx]
		dirty := itr.dirty[itr.idx]
		meta, err := cm.GetCommitMeta(ctx)
		if err != nil {
			return nil, err
		}

		h, err := cm.HashOf()
		if err != nil {
			return nil, err
		}

		remoteBranches := itr.table.remote
		if remoteBranches {
			return sql.NewRow(name, h.String(), meta.Name, meta.Email, meta.Time(), meta.Description), nil
		} else {
			branches, err := itr.table.db.DbData().Rsr.GetBranches()
			if err != nil {
				return nil, err
			}

			remoteName := ""
			branchName := ""
			branch, ok := branches.Get(name)
			if ok {
				remoteName = branch.Remote
				branchName = branch.Merge.Ref.GetPath()
			}
			return sql.NewRow(name, h.String(), meta.Name, meta.Email, meta.Time(), meta.Description, remoteName, branchName, dirty), nil
		}
	}
}

// outOfLowerBound returns true if |branchName| is below the lower bound configured in |itr|,
// indicating that it should be filtered out and not returned.
func (itr *BranchItr) outOfLowerBound(branchName string) bool {
	if itr.lowerBound == "" {
		return false
	}

	threshold := 1
	if itr.lowerBoundInclusive {
		threshold = 0
	}
	return strings.Compare(branchName, itr.lowerBound) < threshold
}

// outOfUpperBound returns true if |branchName| is above the upper bound configured in |itr|,
// indicating that it should be filtered out and not returned.
func (itr *BranchItr) outOfUpperBound(branchName string) bool {
	if itr.upperBound == "" {
		return false
	}

	threshold := -1
	if itr.upperBoundInclusive {
		threshold = 0
	}
	return strings.Compare(branchName, itr.upperBound) > threshold
}

// isDirty returns true if the working ref points to a dirty branch.
func isDirty(ctx *sql.Context, ddb *doltdb.DoltDB, commit *doltdb.Commit, branch ref.DoltRef, txRoot hash.Hash) (bool, error) {
	wsRef, err := ref.WorkingSetRefForHead(branch)
	if err != nil {
		return false, err
	}
	ws, err := ddb.ResolveWorkingSetAtRoot(ctx, wsRef, txRoot)
	if err != nil {
		if errors.Is(err, doltdb.ErrWorkingSetNotFound) {
			// If there is no working set for this branch, then it is never dirty. This happens on servers commonly.
			return false, nil
		}
		return false, err
	}

	workingRoot := ws.WorkingRoot()
	workingRootHash, err := workingRoot.HashOf()
	if err != nil {
		return false, err
	}
	stagedRoot := ws.StagedRoot()
	stagedRootHash, err := stagedRoot.HashOf()
	if err != nil {
		return false, err
	}

	dirty := false
	if workingRootHash != stagedRootHash {
		dirty = true
	} else {
		cmRt, err := commit.GetRootValue(ctx)
		if err != nil {
			return false, err
		}
		cmRtHash, err := cmRt.HashOf()
		if err != nil {
			return false, err
		}
		if cmRtHash != workingRootHash {
			dirty = true
		}
	}
	return dirty, nil
}

// Close closes the iterator.
func (itr *BranchItr) Close(*sql.Context) error {
	return nil
}

// Replacer returns a RowReplacer for this table. The RowReplacer will have Insert and optionally Delete called once
// for each row, followed by a call to Close() when all rows have been processed.
func (bt *BranchesTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return branchWriter{bt}
}

// Updater returns a RowUpdater for this table. The RowUpdater will have Update called once for each row to be
// updated, followed by a call to Close() when all rows have been processed.
func (bt *BranchesTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return branchWriter{bt}
}

// Inserter returns an Inserter for this table. The Inserter will get one call to Insert() for each row to be
// inserted, and will end with a call to Close() to finalize the insert operation.
func (bt *BranchesTable) Inserter(*sql.Context) sql.RowInserter {
	return branchWriter{bt}
}

// Deleter returns a RowDeleter for this table. The RowDeleter will get one call to Delete for each row to be deleted,
// and will end with a call to Close() to finalize the delete operation.
func (bt *BranchesTable) Deleter(*sql.Context) sql.RowDeleter {
	return branchWriter{bt}
}

var _ sql.RowReplacer = branchWriter{nil}
var _ sql.RowUpdater = branchWriter{nil}
var _ sql.RowInserter = branchWriter{nil}
var _ sql.RowDeleter = branchWriter{nil}

type branchWriter struct {
	bt *BranchesTable
}

// Insert inserts the row given, returning an error if it cannot. Insert will be called once for each row to process
// for the insert operation, which may involve many rows. After all rows in an operation have been processed, Close
// is called.
func (bWr branchWriter) Insert(ctx *sql.Context, r sql.Row) error {
	return fmt.Errorf("the dolt_branches table is read-only; use the dolt_branch stored procedure to edit remotes")
}

// Update the given row. Provides both the old and new rows.
func (bWr branchWriter) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	return fmt.Errorf("the dolt_branches table is read-only; use the dolt_branch stored procedure to edit remotes")
}

// Delete deletes the given row. Returns ErrDeleteRowNotFound if the row was not found. Delete will be called once for
// each row to process for the delete operation, which may involve many rows. After all rows have been processed,
// Close is called.
func (bWr branchWriter) Delete(ctx *sql.Context, r sql.Row) error {
	return fmt.Errorf("the dolt_branches table is read-only; use the dolt_branch stored procedure to edit remotes")
}

// StatementBegin implements the interface sql.TableEditor. Currently a no-op.
func (bWr branchWriter) StatementBegin(ctx *sql.Context) {}

// DiscardChanges implements the interface sql.TableEditor. Currently a no-op.
func (bWr branchWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return nil
}

// StatementComplete implements the interface sql.TableEditor. Currently a no-op.
func (bWr branchWriter) StatementComplete(ctx *sql.Context) error {
	return nil
}

// Close finalizes the delete operation, persisting the result.
func (bWr branchWriter) Close(*sql.Context) error {
	return nil
}

// filteredPartition represents a partition of branch names that is filtered by a
// lower bound and upper bound.
type filteredPartition struct {
	lowerBound          string
	lowerBoundInclusive bool
	upperBound          string
	upperBoundInclusive bool
}

var _ sql.Partition = (*filteredPartition)(nil)

// Key implements sql.Partition
func (f filteredPartition) Key() []byte {
	// Key is not used to identify the partition, so we return nil
	return nil
}
