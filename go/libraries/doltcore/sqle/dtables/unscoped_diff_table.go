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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

const unscopedDiffDefaultRowCount = 1000

var workingSetPartitionKey = []byte("workingset")
var commitHistoryPartitionKey = []byte("commithistory")
var commitHashCol = "commit_hash"
var filterColumnNameSet = set.NewStrSet([]string{commitHashCol})

// UnscopedDiffTable is a sql.Table implementation of a system table that shows which tables have
// changed in each commit, across all branches.
type UnscopedDiffTable struct {
	dbName           string
	tableName        string
	ddb              *doltdb.DoltDB
	head             *doltdb.Commit
	partitionFilters []sql.Expression
	commitCheck      doltdb.CommitFilter
}

var _ sql.Table = (*UnscopedDiffTable)(nil)
var _ sql.StatisticsTable = (*UnscopedDiffTable)(nil)
var _ sql.IndexAddressable = (*UnscopedDiffTable)(nil)

// NewUnscopedDiffTable creates an UnscopedDiffTable
func NewUnscopedDiffTable(_ *sql.Context, dbName, tableName string, ddb *doltdb.DoltDB, head *doltdb.Commit) sql.Table {
	return &UnscopedDiffTable{dbName: dbName, tableName: tableName, ddb: ddb, head: head}
}

func (dt *UnscopedDiffTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(dt.Schema())
	numRows, _, err := dt.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (dt *UnscopedDiffTable) RowCount(_ *sql.Context) (uint64, bool, error) {
	return unscopedDiffDefaultRowCount, false, nil
}

// Name is a sql.Table interface function which returns the name of the table
func (dt *UnscopedDiffTable) Name() string {
	return dt.tableName
}

// String is a sql.Table interface function which returns the name of the table
func (dt *UnscopedDiffTable) String() string {
	return dt.tableName
}

func getUnscopedDoltDiffSchema(dbName, tableName string) sql.Schema {
	return []*sql.Column{
		{Name: "commit_hash", Type: types.Text, Source: tableName, PrimaryKey: true, DatabaseSource: dbName},
		{Name: "table_name", Type: types.Text, Source: tableName, PrimaryKey: true, DatabaseSource: dbName},
		{Name: "committer", Type: types.Text, Source: tableName, PrimaryKey: false, DatabaseSource: dbName},
		{Name: "email", Type: types.Text, Source: tableName, PrimaryKey: false, DatabaseSource: dbName},
		{Name: "date", Type: types.Datetime, Source: tableName, PrimaryKey: false, DatabaseSource: dbName},
		{Name: "message", Type: types.Text, Source: tableName, PrimaryKey: false, DatabaseSource: dbName},
		{Name: "data_change", Type: types.Boolean, Source: tableName, PrimaryKey: false, DatabaseSource: dbName},
		{Name: "schema_change", Type: types.Boolean, Source: tableName, PrimaryKey: false, DatabaseSource: dbName},
	}
}

// GetUnscopedDoltDiffSchema returns the schema of the dolt_diff system table. This is used
// by Doltgres to update the dolt_diff schema using Doltgres types.
var GetUnscopedDoltDiffSchema = getUnscopedDoltDiffSchema

// Schema is a sql.Table interface function that returns the sql.Schema for this system table.
func (dt *UnscopedDiffTable) Schema() sql.Schema {
	return GetUnscopedDoltDiffSchema(dt.dbName, dt.tableName)
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
func (dt *UnscopedDiffTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return index.DoltCommitIndexes(dt.dbName, dt.Name(), dt.ddb, false)
}

// IndexedAccess implements sql.IndexAddressable
func (dt *UnscopedDiffTable) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	nt := *dt
	return &nt
}

// PreciseMatch implements sql.IndexAddressable
func (dt *UnscopedDiffTable) PreciseMatch() bool {
	return false
}

func (dt *UnscopedDiffTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
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

func (dt *UnscopedDiffTable) newWorkingSetRowItr(ctx *sql.Context) (sql.RowIter, error) {
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

	change, err := tableDelta.GetSummary(ctx)
	if err != nil {
		return nil, err
	}

	sqlRow := sql.NewRow(
		changeSet,
		change.TableName.String(),
		nil, // committer
		nil, // email
		nil, // date
		nil, // message
		change.DataChange,
		change.SchemaChange,
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
	tableChanges    []diff.TableDeltaSummary
	tableChangesIdx int
}

// newCommitHistoryRowItr creates a doltDiffCommitHistoryRowItr from a CommitItr.
func (dt *UnscopedDiffTable) newCommitHistoryRowItrFromItr(ctx *sql.Context, iter doltdb.CommitItr) (*doltDiffCommitHistoryRowItr, error) {
	dchItr := &doltDiffCommitHistoryRowItr{
		ctx:             ctx,
		ddb:             dt.ddb,
		tableChangesIdx: -1,
		child:           iter,
	}
	return dchItr, nil
}

// newCommitHistoryRowItr creates a doltDiffCommitHistoryRowItr from a list of commits.
func (dt *UnscopedDiffTable) newCommitHistoryRowItrFromCommits(ctx *sql.Context, commits []*doltdb.Commit) (*doltDiffCommitHistoryRowItr, error) {
	dchItr := &doltDiffCommitHistoryRowItr{
		ctx:             ctx,
		ddb:             dt.ddb,
		tableChangesIdx: -1,
		commits:         commits,
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
			_, optCmt, err := itr.child.Next(ctx)
			if err != nil {
				return nil, err
			}
			commit, ok := optCmt.ToCommit()
			if !ok {
				return nil, io.EOF
			}

			err = itr.loadTableChanges(ctx, commit)
			if err == doltdb.ErrGhostCommitEncountered {
				// When showing the diff table in a shallow clone, we show as much of the dolt_history_{table} as we can,
				// and don't consider it an error when we hit a ghost commit.
				return nil, io.EOF
			}
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
		tableChange.TableName.String(),
		meta.Name,
		meta.Email,
		meta.Time(),
		meta.Description,
		tableChange.DataChange,
		tableChange.SchemaChange,
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
func (itr *doltDiffCommitHistoryRowItr) calculateTableChanges(ctx context.Context, commit *doltdb.Commit) ([]diff.TableDeltaSummary, error) {
	if len(commit.DatasParents()) == 0 {
		return nil, nil
	}

	toRootValue, err := commit.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	optCmt, err := itr.ddb.ResolveParent(ctx, commit, 0)
	if err != nil {
		return nil, err
	}
	parent, ok := optCmt.ToCommit()
	if !ok {
		return nil, doltdb.ErrGhostCommitEncountered
	}

	fromRootValue, err := parent.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	deltas, err := diff.GetTableDeltas(ctx, fromRootValue, toRootValue)
	if err != nil {
		return nil, err
	}

	tableChanges := make([]diff.TableDeltaSummary, len(deltas))
	for i := 0; i < len(deltas); i++ {
		change, err := deltas[i].GetSummary(itr.ctx)
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

	return func(ctx context.Context, h hash.Hash, optCmt *doltdb.OptionalCommit) (filterOut bool, err error) {
		sc := sql.NewContext(ctx)

		cm, ok := optCmt.ToCommit()
		if !ok {
			return false, doltdb.ErrGhostCommitEncountered
		}

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
	optCmt, err := ddb.Resolve(ctx, cmSpec, headRef)
	if err != nil {
		return nil
	}
	cm, ok := optCmt.ToCommit()
	if !ok {
		return nil
	}

	return cm
}
