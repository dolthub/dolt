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

package dtables

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	dtypes "github.com/dolthub/dolt/go/store/types"
)

var _ sql.FilteredTable = (*ColumnDiffTable)(nil)

// ColumnDiffTable is a sql.Table implementation of a system table that shows which tables and columns have
// changed in each commit, across all branches.
type ColumnDiffTable struct {
	dbName           string
	ddb              *doltdb.DoltDB
	head             *doltdb.Commit
	partitionFilters []sql.Expression
	commitCheck      doltdb.CommitFilter
}

// NewColumnDiffTable creates an ColumnDiffTable
func NewColumnDiffTable(_ *sql.Context, dbName string, ddb *doltdb.DoltDB, head *doltdb.Commit) sql.Table {
	return &ColumnDiffTable{dbName: dbName, ddb: ddb, head: head}
}

// Name is a sql.Table interface function which returns the name of the table which is defined by the constant
// ColumnDiffTableName
func (dt *ColumnDiffTable) Name() string {
	return doltdb.ColumnDiffTableName
}

// String is a sql.Table interface function which returns the name of the table which is defined by the constant
// ColumnDiffTableName
func (dt *ColumnDiffTable) String() string {
	return doltdb.ColumnDiffTableName
}

// Schema is a sql.Table interface function that returns the sql.Schema for this system table.
func (dt *ColumnDiffTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "commit_hash", Type: types.Text, Source: doltdb.ColumnDiffTableName, PrimaryKey: true},
		{Name: "table_name", Type: types.Text, Source: doltdb.ColumnDiffTableName, PrimaryKey: true},
		{Name: "column_name", Type: types.Text, Source: doltdb.ColumnDiffTableName, PrimaryKey: true},
		{Name: "committer", Type: types.Text, Source: doltdb.ColumnDiffTableName, PrimaryKey: false},
		{Name: "email", Type: types.Text, Source: doltdb.ColumnDiffTableName, PrimaryKey: false},
		{Name: "date", Type: types.Datetime, Source: doltdb.ColumnDiffTableName, PrimaryKey: false},
		{Name: "message", Type: types.Text, Source: doltdb.ColumnDiffTableName, PrimaryKey: false},
		{Name: "diff_type", Type: types.Text, Source: doltdb.ColumnDiffTableName, PrimaryKey: false},
	}
}

// Filters returns the list of filters that are applied to this table.
func (dt *ColumnDiffTable) Filters() []sql.Expression {
	return dt.partitionFilters
}

// HandledFilters returns the list of filters that will be handled by the table itself
func (dt *ColumnDiffTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	filters = append(filters, dt.partitionFilters...)
	dt.partitionFilters = FilterFilters(filters, ColumnPredicate(filterColumnNameSet))
	return dt.partitionFilters
}

// WithFilters returns a new sql.Table instance with the filters applied
func (dt *ColumnDiffTable) WithFilters(_ *sql.Context, filters []sql.Expression) sql.Table {
	dt.partitionFilters = FilterFilters(filters, ColumnPredicate(filterColumnNameSet))
	commitCheck, err := commitFilterForDiffTableFilterExprs(dt.partitionFilters)
	if err != nil {
		return nil
	}
	dt.commitCheck = commitCheck
	return dt
}

// Partitions is a sql.Table interface function that returns a partition of the data. Returns one
// partition for working set changes and one partition for all commit history.
func (dt *ColumnDiffTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return NewSliceOfPartitionsItr([]sql.Partition{
		newDoltDiffPartition(workingSetPartitionKey),
		newDoltDiffPartition(commitHistoryPartitionKey),
	}), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition.
func (dt *ColumnDiffTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	switch p := partition.(type) {
	case *doltdb.CommitPart:
		return dt.newCommitHistoryRowItrFromCommits(ctx, []*doltdb.Commit{p.Commit()})
	default:
		if bytes.Equal(partition.Key(), workingSetPartitionKey) {
			return dt.newWorkingSetRowItr(ctx)
		} else if bytes.Equal(partition.Key(), commitHistoryPartitionKey) {
			cms, hasCommitHashEquality := getCommitsFromCommitHashEquality(ctx, dt.ddb, dt.partitionFilters)
			if hasCommitHashEquality {
				return dt.newCommitHistoryRowItrFromCommits(ctx, cms)
			}
			iter := doltdb.CommitItrForRoots(dt.ddb, dt.head)
			if dt.commitCheck != nil {
				iter = doltdb.NewFilteringCommitItr(iter, dt.commitCheck)
			}
			return dt.newCommitHistoryRowItrFromItr(ctx, iter)
		} else {
			return nil, fmt.Errorf("unexpected partition: %v", partition)
		}
	}
}

// GetIndexes implements sql.IndexAddressable
func (dt *ColumnDiffTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return index.DoltCommitIndexes(dt.Name(), dt.ddb, true)
}

// IndexedAccess implements sql.IndexAddressable
func (dt *ColumnDiffTable) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	nt := *dt
	return &nt
}

// Collation implements the sql.Table interface.
func (dt *ColumnDiffTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (dt *ColumnDiffTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if lookup.Index.ID() == index.CommitHashIndexId {
		hs, ok := index.LookupToPointSelectStr(lookup)
		if !ok {
			return nil, fmt.Errorf("failed to parse commit lookup ranges: %s", sql.DebugString(lookup.Ranges))
		}
		hashes, commits, metas := index.HashesToCommits(ctx, dt.ddb, hs, dt.head, false)
		if len(hashes) == 0 {
			return sql.PartitionsToPartitionIter(), nil
		}

		headHash, err := dt.head.HashOf()
		if err != nil {
			return nil, err
		}
		var partitions []sql.Partition
		for i, h := range hashes {
			if h == headHash && commits[i] == nil {
				partitions = append(partitions, newDoltDiffPartition(workingSetPartitionKey))
			} else {
				partitions = append(partitions, doltdb.NewCommitPart(h, commits[i], metas[i]))
			}
		}
		return sql.PartitionsToPartitionIter(partitions...), nil
	}

	return dt.Partitions(ctx)
}

type doltColDiffWorkingSetRowItr struct {
	stagedIndex         int
	unstagedIndex       int
	colIndex            int
	stagedTableDeltas   []diff.TableDelta
	unstagedTableDeltas []diff.TableDelta
	colNames            []string
	diffTypes           []string
}

func (dt *ColumnDiffTable) newWorkingSetRowItr(ctx *sql.Context) (sql.RowIter, error) {
	sess := dsess.DSessFromSess(ctx.Session)
	roots, ok := sess.GetRoots(ctx, dt.dbName)
	if !ok {
		return nil, fmt.Errorf("unable to lookup roots for database %s", dt.dbName)
	}

	staged, unstaged, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		return nil, err
	}

	var ri sql.RowIter
	ri = &doltColDiffWorkingSetRowItr{
		stagedTableDeltas:   staged,
		unstagedTableDeltas: unstaged,
	}

	for _, filter := range dt.partitionFilters {
		ri = plan.NewFilterIter(filter, ri)
	}

	return ri, nil
}

// incrementColIndex increments the column index and table changes index.  When the end of the column names array is
// reached, moves to the next table changes delta.
func (d *doltColDiffWorkingSetRowItr) incrementColIndex(staged bool) {
	d.colIndex++

	// move to next table once all modified columns are iterated through
	if d.colIndex >= len(d.colNames) {
		d.colIndex = 0
		if staged {
			d.stagedIndex++
		} else {
			d.unstagedIndex++
		}
	}
}

func (d *doltColDiffWorkingSetRowItr) Next(ctx *sql.Context) (sql.Row, error) {
	var changeSet string
	var tableDelta diff.TableDelta
	// staged keeps track of whether we are looking at staged changes or working set changes
	staged := false
	if d.stagedIndex < len(d.stagedTableDeltas) {
		changeSet = "STAGED"
		tableDelta = d.stagedTableDeltas[d.stagedIndex]
		staged = true
		// only need to load new changes when we're finished iterating through the previous tableDelta
		if d.colIndex == 0 {
			d.loadColumnChanges(tableDelta)
		}
	} else if d.unstagedIndex < len(d.unstagedTableDeltas) {
		changeSet = "WORKING"
		tableDelta = d.unstagedTableDeltas[d.unstagedIndex]
		// only need to load new changes when we're finished iterating through the previous tableDelta
		if d.colIndex == 0 {
			d.loadColumnChanges(tableDelta)
		}
	} else {
		return nil, io.EOF
	}

	defer d.incrementColIndex(staged)

	change, err := tableDelta.GetSummary(ctx)
	if err != nil {
		return nil, err
	}

	sqlRow := sql.NewRow(
		changeSet,
		change.TableName,
		d.colNames[d.colIndex],
		nil, // committer
		nil, // email
		nil, // date
		nil, // message
		d.diffTypes[d.colIndex],
	)

	return sqlRow, nil
}

// loadColumnChanges loads the list of column names for columns that have changed and the corresponding diff_type for
// those column changes
func (d *doltColDiffWorkingSetRowItr) loadColumnChanges(tableDelta diff.TableDelta) {
	var toCols, fromCols *schema.ColCollection
	if tableDelta.ToSch != nil {
		toCols = tableDelta.ToSch.GetAllCols()
	}
	if tableDelta.FromSch != nil {
		fromCols = tableDelta.FromSch.GetAllCols()
	}
	_, _, _, colNames, diffTypes := calculateColSchemaDiff(toCols, fromCols)
	// TODO (steph): if a row has been modified, all columns will be marked as modified instead of just the columns which contain the changes

	d.colNames = colNames
	d.diffTypes = diffTypes
}

func (d *doltColDiffWorkingSetRowItr) Close(c *sql.Context) error {
	return nil
}

// doltColDiffCommitHistoryRowItr is a sql.RowItr implementation which iterates over each commit as if it's a row in the table.
type doltColDiffCommitHistoryRowItr struct {
	ctx             *sql.Context
	ddb             *doltdb.DoltDB
	child           doltdb.CommitItr
	commits         []*doltdb.Commit
	meta            *datas.CommitMeta
	hash            hash.Hash
	tableChanges    []tableColChange
	tableChangesIdx int
	colIdx          int
}

// newCommitHistoryRowItr creates a doltDiffCommitHistoryRowItr from a CommitItr.
func (dt *ColumnDiffTable) newCommitHistoryRowItrFromItr(ctx *sql.Context, iter doltdb.CommitItr) (*doltColDiffCommitHistoryRowItr, error) {
	dchItr := &doltColDiffCommitHistoryRowItr{
		ctx:             ctx,
		ddb:             dt.ddb,
		tableChangesIdx: -1,
		child:           iter,
	}
	return dchItr, nil
}

// newCommitHistoryRowItr creates a doltDiffCommitHistoryRowItr from a list of commits.
func (dt *ColumnDiffTable) newCommitHistoryRowItrFromCommits(ctx *sql.Context, commits []*doltdb.Commit) (*doltColDiffCommitHistoryRowItr, error) {
	dchItr := &doltColDiffCommitHistoryRowItr{
		ctx:             ctx,
		ddb:             dt.ddb,
		tableChangesIdx: -1,
		commits:         commits,
	}
	return dchItr, nil
}

// incrementIndexes increments the column index and table changes index. When the end of the column names array is
// reached, moves to the next table. When the end of the table changes array is reached, moves to the next commit,
// and resets the table changes index so that it can be populated when Next() is called.
func (itr *doltColDiffCommitHistoryRowItr) incrementIndexes(tableChange tableColChange) {
	itr.colIdx++
	if itr.colIdx >= len(tableChange.colNames) {
		itr.tableChangesIdx++
		itr.colIdx = 0
		if itr.tableChangesIdx >= len(itr.tableChanges) {
			itr.tableChangesIdx = -1
			itr.tableChanges = nil
		}
	}
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *doltColDiffCommitHistoryRowItr) Next(ctx *sql.Context) (sql.Row, error) {
	for itr.tableChanges == nil {
		if itr.commits != nil {
			for _, commit := range itr.commits {
				err := itr.loadTableChanges(ctx, commit)
				if err != nil {
					return nil, err
				}
			}
			itr.commits = nil
		} else if itr.child != nil {
			_, commit, err := itr.child.Next(ctx)
			if err != nil {
				return nil, err
			}
			err = itr.loadTableChanges(ctx, commit)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, io.EOF
		}
	}

	tableChange := itr.tableChanges[itr.tableChangesIdx]
	defer itr.incrementIndexes(tableChange)

	meta := itr.meta
	h := itr.hash
	col := tableChange.colNames[itr.colIdx]
	diffType := tableChange.diffTypes[itr.colIdx]

	return sql.NewRow(
		h.String(),
		tableChange.tableName,
		col,
		meta.Name,
		meta.Email,
		meta.Time(),
		meta.Description,
		diffType,
	), nil
}

// loadTableChanges loads the current commit's table changes and metadata into the iterator.
func (itr *doltColDiffCommitHistoryRowItr) loadTableChanges(ctx context.Context, commit *doltdb.Commit) error {
	tableChanges, err := itr.calculateTableChanges(ctx, commit)
	if err != nil {
		return err
	}

	itr.tableChanges = tableChanges
	itr.tableChangesIdx = 0
	if len(tableChanges) == 0 {
		return nil
	}

	meta, err := commit.GetCommitMeta(ctx)
	if err != nil {
		return err
	}
	itr.meta = meta

	cmHash, err := commit.HashOf()
	if err != nil {
		return err
	}
	itr.hash = cmHash

	return nil
}

// calculateTableChanges calculates the tables that changed in the specified commit, by comparing that
// commit with its immediate ancestor commit.
func (itr *doltColDiffCommitHistoryRowItr) calculateTableChanges(ctx context.Context, commit *doltdb.Commit) ([]tableColChange, error) {
	if len(commit.DatasParents()) == 0 {
		return nil, nil
	}

	toRootValue, err := commit.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	parent, err := itr.ddb.ResolveParent(ctx, commit, 0)
	if err != nil {
		return nil, err
	}

	fromRootValue, err := parent.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	deltas, err := diff.GetTableDeltas(ctx, fromRootValue, toRootValue)
	if err != nil {
		return nil, err
	}

	tableChanges := make([]tableColChange, len(deltas))
	tableChangeIdx := 0
	for i := 0; i < len(deltas); i++ {
		change, err := processTableColDelta(itr.ctx, itr.ddb, deltas[i])
		if err != nil {
			return nil, err
		}

		// filter out changes that have no modified columns
		if len(change.colNames) != 0 {
			tableChanges[tableChangeIdx] = *change
			tableChangeIdx++
		} else {
			// remove table changes that don't have any modified columns
			if tableChangeIdx == len(tableChanges)-1 {
				tableChanges = tableChanges[:tableChangeIdx]
			} else {
				tableChanges = append(tableChanges[:tableChangeIdx], tableChanges[tableChangeIdx+1:]...)
			}
		}
	}

	// Not all commits mutate tables (e.g. empty commits)
	if len(tableChanges) == 0 {
		return nil, nil
	}

	return tableChanges, nil
}

// Close closes the iterator.
func (itr *doltColDiffCommitHistoryRowItr) Close(*sql.Context) error {
	return nil
}

// tableColChange is an internal data structure used to hold the results of processing
// a diff.TableDelta structure into the output data for this system table.
type tableColChange struct {
	tableName string
	colNames  []string
	diffTypes []string
}

// processTableColDelta processes the specified TableDelta to determine what kind of change it was (i.e. table drop,
// table rename, table create, or data update) and returns a tableChange struct representing the change.
func processTableColDelta(ctx *sql.Context, ddb *doltdb.DoltDB, delta diff.TableDelta) (*tableColChange, error) {
	// Dropping a table is always a schema change, and also a data change if the table contained data
	if delta.IsDrop() {
		diffTypes := make([]string, delta.FromSch.GetAllCols().Size())
		for i := range diffTypes {
			diffTypes[i] = diffTypeRemoved
		}

		return &tableColChange{
			tableName: delta.FromName,
			colNames:  delta.FromSch.GetAllCols().GetColumnNames(),
			diffTypes: diffTypes,
		}, nil
	}

	// Renaming a table does not affect columns necessarily, if table data was changed it will be checked below

	// Creating a table is always a schema change, and also a data change if data was inserted
	if delta.IsAdd() {
		diffTypes := make([]string, delta.ToSch.GetAllCols().Size())
		for i := range diffTypes {
			diffTypes[i] = diffTypeAdded
		}

		return &tableColChange{
			tableName: delta.ToName,
			colNames:  delta.ToSch.GetAllCols().GetColumnNames(),
			diffTypes: diffTypes,
		}, nil
	}

	// calculate which columns have been modified
	diffTableSchema, j, err := GetDiffTableSchemaAndJoiner(delta.ToTable.Format(), delta.FromSch, delta.ToSch)
	if err != nil {
		return nil, err
	}

	// accurate commit time returned elsewhere
	now := time.Now()
	dp := NewDiffPartition(delta.ToTable, delta.FromTable, delta.ToName, delta.FromName, (*dtypes.Timestamp)(&now), (*dtypes.Timestamp)(&now), delta.ToSch, delta.FromSch)
	ri := NewDiffPartitionRowIter(*dp, ddb, j)
	colNames, diffTypes, err := calculateColDelta(ctx, ri, delta.ToSch.GetAllCols(), delta.FromSch.GetAllCols(), diffTableSchema.GetAllCols())
	if err != nil {
		return nil, err
	}

	return &tableColChange{
		tableName: delta.ToName,
		colNames:  colNames,
		diffTypes: diffTypes,
	}, nil
}

// calculateColDelta iterates through the rows of the given table delta and compares each cell in the to_ and from_
// cells to compile a list of modified columns
func calculateColDelta(ctx *sql.Context, iter sql.RowIter, toCols, fromCols, diffTableCols *schema.ColCollection) ([]string, []string, error) {
	colNamesSet := make(map[string]struct{})
	var resultColNames []string
	var resultDiffTypes []string
	modifiedCols, addedCols, droppedCols, _, _ := calculateColSchemaDiff(toCols, fromCols)

	// add all added/dropped columns to result
	for _, col := range addedCols {
		resultColNames = append(resultColNames, col)
		resultDiffTypes = append(resultDiffTypes, diffTypeAdded)
	}
	for _, col := range droppedCols {
		resultColNames = append(resultColNames, col)
		resultDiffTypes = append(resultDiffTypes, diffTypeRemoved)
	}

	// check each row for diffs in modified columns
	for {
		r, err := iter.Next(ctx)
		if err == io.EOF {
			for col := range colNamesSet {
				// append modified columns to result
				resultColNames = append(resultColNames, col)
				resultDiffTypes = append(resultDiffTypes, diffTypeModified)
			}
			return resultColNames, resultDiffTypes, nil
		} else if err != nil {
			return nil, nil, err
		}

		// only need to check modified columns
		for _, col := range modifiedCols {
			toColTag := diffTableCols.NameToCol["to_"+col].Tag
			fromColTag := diffTableCols.NameToCol["from_"+col].Tag
			toIdx := diffTableCols.TagToIdx[toColTag]
			fromIdx := diffTableCols.TagToIdx[fromColTag]

			if r[toIdx] != r[fromIdx] {
				colNamesSet[col] = struct{}{}
			}
		}

		// can stop checking rows when we already have all modified columns in the result set
		if len(colNamesSet) == len(modifiedCols) {
			for col := range colNamesSet {
				// append modified columns to result
				resultColNames = append(resultColNames, col)
				resultDiffTypes = append(resultDiffTypes, diffTypeModified)
			}
			return resultColNames, resultDiffTypes, nil
		}
	}
}

// calculateColSchemaDiff calculates which columns were modified, added, or dropped between to and from schemas and
// returns a list of column names for each type of change, the total list of column names, and a corresponding list of
// diff_types for each column
func calculateColSchemaDiff(toCols *schema.ColCollection, fromCols *schema.ColCollection) ([]string, []string, []string, []string, []string) {
	// put to/from columns into a set
	toColTags := make(map[uint64]struct{})
	fromColTags := make(map[uint64]struct{})
	if toCols != nil {
		for _, tag := range toCols.Tags {
			toColTags[tag] = struct{}{}
		}
	}
	if fromCols != nil {
		for _, tag := range fromCols.Tags {
			fromColTags[tag] = struct{}{}
		}
	}

	var modifiedCols []string
	var addedCols []string
	var droppedCols []string
	var allCols []string
	var diffTypes []string

	if toCols != nil {
		for _, tag := range toCols.Tags {
			if _, ok := fromColTags[tag]; ok {
				// if the tag is also in fromColumnTags, this column was modified
				modifiedCols = append(modifiedCols, toCols.TagToCol[tag].Name)
				allCols = append(allCols, toCols.TagToCol[tag].Name)
				diffTypes = append(diffTypes, diffTypeModified)
				delete(fromColTags, tag)
			} else {
				// else if it isn't in fromColumnTags, this column was added
				addedCols = append(addedCols, toCols.TagToCol[tag].Name)
				allCols = append(allCols, toCols.TagToCol[tag].Name)
				diffTypes = append(diffTypes, diffTypeAdded)
			}
		}
	}

	if fromCols != nil {
		for tag, _ := range fromColTags {
			// all remaining tags are columns not in toColumnTags, i.e. dropped columns
			droppedCols = append(droppedCols, fromCols.TagToCol[tag].Name)
			allCols = append(allCols, fromCols.TagToCol[tag].Name)
			diffTypes = append(diffTypes, diffTypeRemoved)
		}
	}

	return modifiedCols, addedCols, droppedCols, allCols, diffTypes
}
