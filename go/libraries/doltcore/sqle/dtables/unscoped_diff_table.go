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

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

var workingSetPartitionKey = []byte("workingset")
var commitHistoryPartitionKey = []byte("commithistory")
var commitHashCol = "commit_hash"
var filterColumnNameSet = set.NewStrSet([]string{commitHashCol})

var _ sql.FilteredTable = (*UnscopedDiffTable)(nil)

// UnscopedDiffTable is a sql.Table implementation of a system table that shows which tables have
// changed in each commit, across all branches.
type UnscopedDiffTable struct {
	ddb              *doltdb.DoltDB
	head             *doltdb.Commit
	partitionFilters []sql.Expression
	cmItr            doltdb.CommitItr
}

// tableChange is an internal data structure used to hold the results of processing
// a diff.TableDelta structure into the output data for this system table.
type tableChange struct {
	tableName    string
	dataChange   bool
	schemaChange bool
}

// NewUnscopedDiffTable creates an UnscopedDiffTable
func NewUnscopedDiffTable(_ *sql.Context, ddb *doltdb.DoltDB, head *doltdb.Commit) sql.Table {
	cmItr := doltdb.CommitItrForRoots(ddb, head)
	return &UnscopedDiffTable{ddb: ddb, head: head, cmItr: cmItr}
}

// Filters returns the list of filters that are applied to this table.
func (dt *UnscopedDiffTable) Filters() []sql.Expression {
	return dt.partitionFilters
}

// HandledFilters returns the list of filters that will be handled by the table itself
func (dt *UnscopedDiffTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	dt.partitionFilters = FilterFilters(filters, ColumnPredicate(filterColumnNameSet))
	return dt.partitionFilters
}

// WithFilters returns a new sql.Table instance with the filters applied
func (dt *UnscopedDiffTable) WithFilters(ctx *sql.Context, filters []sql.Expression) sql.Table {
	dt.partitionFilters = FilterFilters(filters, ColumnPredicate(filterColumnNameSet))

	if len(dt.partitionFilters) > 0 {
		commitCheck, err := commitFilterForDiffTableFilterExprs(dt.partitionFilters)
		if err != nil {
			return nil
		}
		ndt := *dt
		ndt.cmItr = doltdb.NewFilteringCommitItr(dt.cmItr, commitCheck)
		return &ndt
	}

	return dt
}

// Name is a sql.Table interface function which returns the name of the table which is defined by the constant
// LogTableName
func (dt *UnscopedDiffTable) Name() string {
	return doltdb.DiffTableName
}

// String is a sql.Table interface function which returns the name of the table which is defined by the constant
// DiffTableName
func (dt *UnscopedDiffTable) String() string {
	return doltdb.DiffTableName
}

// Schema is a sql.Table interface function that returns the sql.Schema for this system table.
func (dt *UnscopedDiffTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "commit_hash", Type: sql.Text, Source: doltdb.DiffTableName, PrimaryKey: true},
		{Name: "table_name", Type: sql.Text, Source: doltdb.DiffTableName, PrimaryKey: true},
		{Name: "committer", Type: sql.Text, Source: doltdb.DiffTableName, PrimaryKey: false},
		{Name: "email", Type: sql.Text, Source: doltdb.DiffTableName, PrimaryKey: false},
		{Name: "date", Type: sql.Datetime, Source: doltdb.DiffTableName, PrimaryKey: false},
		{Name: "message", Type: sql.Text, Source: doltdb.DiffTableName, PrimaryKey: false},
		{Name: "data_change", Type: sql.Boolean, Source: doltdb.DiffTableName, PrimaryKey: false},
		{Name: "schema_change", Type: sql.Boolean, Source: doltdb.DiffTableName, PrimaryKey: false},
	}
}

// Collation implements the sql.Table interface.
func (dt *UnscopedDiffTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data. Returns one
// partition for working set changes and one partition for all commit history.
func (dt *UnscopedDiffTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return NewSliceOfPartitionsItr([]sql.Partition{
		newDoltDiffPartition(workingSetPartitionKey),
		newDoltDiffPartition(commitHistoryPartitionKey),
	}), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition.
func (dt *UnscopedDiffTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	if bytes.Equal(partition.Key(), workingSetPartitionKey) {
		return dt.newWorkingSetRowItr(ctx)
	} else if bytes.Equal(partition.Key(), commitHistoryPartitionKey) {
		return dt.newCommitHistoryRowItr(ctx)
	} else {
		return nil, fmt.Errorf("unexpected partition: %v", partition)
	}
}

func (dt *UnscopedDiffTable) newWorkingSetRowItr(ctx *sql.Context) (sql.RowIter, error) {
	sess := dsess.DSessFromSess(ctx.Session)
	roots, ok := sess.GetRoots(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return nil, fmt.Errorf("unable to lookup roots for database %s", ctx.GetCurrentDatabase())
	}

	staged, unstaged, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		return nil, err
	}

	var ri sql.RowIter
	ri = &doltDiffWorkingSetRowItr{
		stagedTableDeltas:   staged,
		unstagedTableDeltas: unstaged,
	}

	for _, filter := range dt.partitionFilters {
		ri = plan.NewFilterIter(filter, ri)
	}

	return ri, nil
}

var _ sql.RowIter = &doltDiffWorkingSetRowItr{}

type doltDiffWorkingSetRowItr struct {
	stagedIndex         int
	unstagedIndex       int
	stagedTableDeltas   []diff.TableDelta
	unstagedTableDeltas []diff.TableDelta
}

func (d *doltDiffWorkingSetRowItr) Next(ctx *sql.Context) (sql.Row, error) {
	var changeSet string
	var tableDelta diff.TableDelta
	if d.stagedIndex < len(d.stagedTableDeltas) {
		changeSet = "STAGED"
		tableDelta = d.stagedTableDeltas[d.stagedIndex]
		d.stagedIndex++
	} else if d.unstagedIndex < len(d.unstagedTableDeltas) {
		changeSet = "WORKING"
		tableDelta = d.unstagedTableDeltas[d.unstagedIndex]
		d.unstagedIndex++
	} else {
		return nil, io.EOF
	}

	change, err := processTableDelta(ctx, tableDelta)
	if err != nil {
		return nil, err
	}

	sqlRow := sql.NewRow(
		changeSet,
		change.tableName,
		nil, // committer
		nil, // email
		nil, // date
		nil, // message
		change.dataChange,
		change.schemaChange,
	)

	return sqlRow, nil
}

func (d *doltDiffWorkingSetRowItr) Close(c *sql.Context) error {
	return nil
}

var _ sql.Partition = &doltDiffPartition{}

type doltDiffPartition struct {
	key []byte
}

func newDoltDiffPartition(key []byte) *doltDiffPartition {
	return &doltDiffPartition{
		key: key,
	}
}

func (d doltDiffPartition) Key() []byte {
	return d.key
}

// doltDiffCommitHistoryRowItr is a sql.RowItr implementation which iterates over each commit as if it's a row in the table.
type doltDiffCommitHistoryRowItr struct {
	ctx             *sql.Context
	ddb             *doltdb.DoltDB
	child           doltdb.CommitItr
	commits         []*doltdb.Commit
	meta            *datas.CommitMeta
	hash            hash.Hash
	tableChanges    []tableChange
	tableChangesIdx int
}

// newCommitHistoryRowItr creates a doltDiffCommitHistoryRowItr from the current environment.
func (dt *UnscopedDiffTable) newCommitHistoryRowItr(ctx *sql.Context) (*doltDiffCommitHistoryRowItr, error) {
	dchItr := &doltDiffCommitHistoryRowItr{
		ctx:             ctx,
		ddb:             dt.ddb,
		tableChangesIdx: -1,
	}
	cms, hasCommitHashEquality := getCommitsFromCommitHashEquality(ctx, dt.ddb, dt.partitionFilters)
	if hasCommitHashEquality {
		dchItr.commits = cms
	} else {
		dchItr.child = dt.cmItr
	}
	return dchItr, nil
}

// incrementIndexes increments the table changes index, and if it's the end of the table changes array, moves
// to the next commit, and resets the table changes index so that it can be populated when Next() is called.
func (itr *doltDiffCommitHistoryRowItr) incrementIndexes() {
	itr.tableChangesIdx++
	if itr.tableChangesIdx >= len(itr.tableChanges) {
		itr.tableChangesIdx = -1
		itr.tableChanges = nil
	}
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *doltDiffCommitHistoryRowItr) Next(ctx *sql.Context) (sql.Row, error) {
	defer itr.incrementIndexes()

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
	meta := itr.meta
	h := itr.hash

	return sql.NewRow(
		h.String(),
		tableChange.tableName,
		meta.Name,
		meta.Email,
		meta.Time(),
		meta.Description,
		tableChange.dataChange,
		tableChange.schemaChange,
	), nil
}

// loadTableChanges loads the current commit's table changes and metadata
// into the iterator.
func (itr *doltDiffCommitHistoryRowItr) loadTableChanges(ctx context.Context, commit *doltdb.Commit) error {
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
func (itr *doltDiffCommitHistoryRowItr) calculateTableChanges(ctx context.Context, commit *doltdb.Commit) ([]tableChange, error) {
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

	tableChanges := make([]tableChange, len(deltas))
	for i := 0; i < len(deltas); i++ {
		change, err := processTableDelta(itr.ctx, deltas[i])
		if err != nil {
			return nil, err
		}

		tableChanges[i] = *change
	}

	// Not all commits mutate tables (e.g. empty commits)
	if len(tableChanges) == 0 {
		return nil, nil
	}

	return tableChanges, nil
}

// processTableDelta processes the specified TableDelta to determine what kind of change it was (i.e. table drop,
// table rename, table create, or data update) and returns a tableChange struct representing the change.
func processTableDelta(ctx *sql.Context, delta diff.TableDelta) (*tableChange, error) {
	// Dropping a table is always a schema change, and also a data change if the table contained data
	if delta.IsDrop() {
		isEmpty, err := isTableDataEmpty(ctx, delta.FromTable)
		if err != nil {
			return nil, err
		}

		return &tableChange{
			tableName:    delta.FromName,
			dataChange:   !isEmpty,
			schemaChange: true,
		}, nil
	}

	// Renaming a table is always a schema change, and also a data change if the table data differs
	if delta.IsRename() {
		dataChanged, err := delta.HasHashChanged()
		if err != nil {
			return nil, err
		}

		return &tableChange{
			tableName:    delta.ToName,
			dataChange:   dataChanged,
			schemaChange: true,
		}, nil
	}

	// Creating a table is always a schema change, and also a data change if data was inserted
	if delta.IsAdd() {
		isEmpty, err := isTableDataEmpty(ctx, delta.ToTable)
		if err != nil {
			return nil, err
		}

		return &tableChange{
			tableName:    delta.ToName,
			dataChange:   !isEmpty,
			schemaChange: true,
		}, nil
	}

	dataChanged, err := delta.HasHashChanged()
	if err != nil {
		return nil, err
	}

	schemaChanged, err := delta.HasSchemaChanged(ctx)
	if err != nil {
		return nil, err
	}

	return &tableChange{
		tableName:    delta.ToName,
		dataChange:   dataChanged,
		schemaChange: schemaChanged,
	}, nil
}

// Close closes the iterator.
func (itr *doltDiffCommitHistoryRowItr) Close(*sql.Context) error {
	return nil
}

// isTableDataEmpty return true if the table does not contain any data
func isTableDataEmpty(ctx *sql.Context, table *doltdb.Table) (bool, error) {
	rowData, err := table.GetRowData(ctx)
	if err != nil {
		return false, err
	}

	return rowData.Empty()
}

// commitFilterForDiffTableFilterExprs returns CommitFilter used for CommitItr.
func commitFilterForDiffTableFilterExprs(filters []sql.Expression) (doltdb.CommitFilter, error) {
	filters = transformFilters(filters...)

	return func(ctx context.Context, h hash.Hash, cm *doltdb.Commit) (filterOut bool, err error) {
		sc := sql.NewContext(ctx)
		meta, err := cm.GetCommitMeta(ctx)
		if err != nil {
			return false, err
		}
		for _, filter := range filters {
			res, err := filter.Eval(sc, sql.Row{h.String(), meta.Name, meta.Time()})
			if err != nil {
				return false, err
			}
			b, ok := res.(bool)
			if ok && !b {
				return true, nil
			}
		}

		return false, err
	}, nil
}

// transformFilters return filter expressions with index specified for rows used in CommitFilter.
func transformFilters(filters ...sql.Expression) []sql.Expression {
	for i := range filters {
		filters[i], _, _ = transform.Expr(filters[i], func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			gf, ok := e.(*expression.GetField)
			if !ok {
				return e, transform.SameTree, nil
			}
			switch gf.Name() {
			case commitHashCol:
				return gf.WithIndex(0), transform.NewTree, nil
			default:
				return gf, transform.SameTree, nil
			}
		})
	}
	return filters
}

func getCommitsFromCommitHashEquality(ctx *sql.Context, ddb *doltdb.DoltDB, filters []sql.Expression) ([]*doltdb.Commit, bool) {
	var commits []*doltdb.Commit
	var isCommitHashEquality bool
	for i := range filters {
		switch f := filters[i].(type) {
		case *expression.Equals:
			v, err := f.Right().Eval(ctx, nil)
			if err == nil {
				isCommitHashEquality = true
				cm := getCommitFromHash(ctx, ddb, v.(string))
				if cm != nil {
					commits = append(commits, cm)
				}
			}
		case *expression.InTuple:
			switch r := f.Right().(type) {
			case expression.Tuple:
				right, err := r.Eval(ctx, nil)
				if err == nil && right != nil {
					isCommitHashEquality = true
					if len(r) == 1 {
						cm := getCommitFromHash(ctx, ddb, right.(string))
						if cm != nil {
							commits = append(commits, cm)
						}
					} else {
						for _, el := range right.([]interface{}) {
							cm := getCommitFromHash(ctx, ddb, el.(string))
							if cm != nil {
								commits = append(commits, cm)
							}
						}
					}
				}
			}
		}
	}
	return commits, isCommitHashEquality
}

func getCommitFromHash(ctx *sql.Context, ddb *doltdb.DoltDB, val string) *doltdb.Commit {
	cmSpec, err := doltdb.NewCommitSpec(val)
	if err != nil {
		return nil
	}
	headRef, err := dsess.DSessFromSess(ctx.Session).CWBHeadRef(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return nil
	}
	cm, err := ddb.Resolve(ctx, cmSpec, headRef)
	if err != nil {
		return nil
	}
	return cm
}
