package dtables

import (
	"bytes"
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	dtypes "github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"io"
	"strings"
	"time"
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
		{Name: "data_change", Type: types.Boolean, Source: doltdb.ColumnDiffTableName, PrimaryKey: false},
		{Name: "schema_change", Type: types.Boolean, Source: doltdb.ColumnDiffTableName, PrimaryKey: false},
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
func (d *doltColDiffWorkingSetRowItr) incrementColIndex(tableDelta diff.TableDelta, staged bool) {
	d.colIndex++
	var numCols int
	if tableDelta.ToSch != nil {
		numCols = tableDelta.ToSch.GetAllCols().Size()
	} else {
		numCols = tableDelta.FromSch.GetAllCols().Size()
	}

	// move to next table once all modified columns are iterated through
	if d.colIndex >= numCols {
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
	} else if d.unstagedIndex < len(d.unstagedTableDeltas) {
		changeSet = "WORKING"
		tableDelta = d.unstagedTableDeltas[d.unstagedIndex]
	} else {
		return nil, io.EOF
	}

	defer d.incrementColIndex(tableDelta, staged)

	change, err := tableDelta.GetSummary(ctx)
	if err != nil {
		return nil, err
	}

	var colName string
	if tableDelta.FromSch != nil {
		colName = tableDelta.FromSch.GetAllCols().GetColumnNames()[d.colIndex]
	} else {
		colName = tableDelta.ToSch.GetAllCols().GetColumnNames()[d.colIndex]
	}

	sqlRow := sql.NewRow(
		changeSet,
		change.TableName,
		colName,
		nil, // committer
		nil, // email
		nil, // date
		nil, // message
		change.DataChange,
		change.SchemaChange,
	)

	return sqlRow, nil
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

	return sql.NewRow(
		h.String(),
		tableChange.tableName,
		col,
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
	for i := 0; i < len(deltas); i++ {
		change, err := processTableColDelta(itr.ctx, itr.ddb, deltas[i])
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
func (itr *doltColDiffCommitHistoryRowItr) Close(*sql.Context) error {
	return nil
}

// tableColChange is an internal data structure used to hold the results of processing
// a diff.TableDelta structure into the output data for this system table.
type tableColChange struct {
	tableName    string
	colNames     []string
	dataChange   bool
	schemaChange bool
}

// processTableColDelta processes the specified TableDelta to determine what kind of change it was (i.e. table drop,
// table rename, table create, or data update) and returns a tableChange struct representing the change.
func processTableColDelta(ctx *sql.Context, ddb *doltdb.DoltDB, delta diff.TableDelta) (*tableColChange, error) {
	// Dropping a table is always a schema change, and also a data change if the table contained data
	if delta.IsDrop() {
		isEmpty, err := isTableDataEmpty(ctx, delta.FromTable)
		if err != nil {
			return nil, err
		}

		return &tableColChange{
			tableName:    delta.FromName,
			colNames:     delta.FromSch.GetAllCols().GetColumnNames(),
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

		return &tableColChange{
			tableName:    delta.ToName,
			colNames:     delta.ToSch.GetAllCols().GetColumnNames(),
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

		return &tableColChange{
			tableName:    delta.ToName,
			colNames:     delta.ToSch.GetAllCols().GetColumnNames(),
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

	// calculate which columns have changed
	diffTableSchema, j, err := GetDiffTableSchemaAndJoiner(delta.ToTable.Format(), delta.FromSch, delta.ToSch)
	if err != nil {
		return nil, err
	}

	// accurate commit time returned elsewhere
	now := time.Now()
	dp := NewDiffPartition(delta.ToTable, delta.FromTable, delta.ToName, delta.FromName, (*dtypes.Timestamp)(&now), (*dtypes.Timestamp)(&now), delta.ToSch, delta.FromSch)
	ri := NewDiffPartitionRowIter(*dp, ddb, j)
	colNames, err := calculateColDelta(ctx, delta, ri, diffTableSchema)
	if err != nil {
		return nil, err
	}

	return &tableColChange{
		tableName:    delta.ToName,
		colNames:     colNames,
		dataChange:   dataChanged,
		schemaChange: schemaChanged,
	}, nil
}

func calculateColDelta(ctx *sql.Context, delta diff.TableDelta, iter sql.RowIter, diffTableSchema schema.Schema) ([]string, error) {
	diffPkSch, err := sqlutil.FromDoltSchema("", diffTableSchema)
	if err != nil {
		return nil, err
	}
	columnsWithDiff := getColumnNamesWithDiff(delta.FromSch, delta.ToSch)
	_, projections := getDiffSqlSchema(diffPkSch.Schema, columnsWithDiff)

	columns := make(map[string]string)
	var result []string
	numCols := (len(projections) - 1) / 2

	for {
		r, err := iter.Next(ctx)
		if err == io.EOF {
			for key := range columns {
				// returning only names of modified columns
				result = append(result, key)
			}
			return result, nil
		} else if err != nil {
			return nil, err
		}

		i := 0
		for i < numCols {
			// compare the to_ and from_ cells, accounting for to_commit and to_commit_date columns
			if r[i] != r[i+numCols+2] {
				cleanedName := strings.Split(projections[i].(*expression.GetField).Name(), "_")[1]
				columns[cleanedName] = ""
			}
			i++
		}

		// can stop checking rows when we already have all columns in the result set
		if len(columns) == numCols {
			for key := range columns {
				// returning only names of modified columns
				result = append(result, key)
			}
			return result, nil
		}
	}
}

// getColumnNamesWithDiff attaches the to_ and from_ prefixes to all columns
func getColumnNamesWithDiff(fromSch, toSch schema.Schema) []string {
	var cols []string

	if fromSch != nil {
		_ = fromSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			cols = append(cols, fmt.Sprintf("from_%s", col.Name))
			return false, nil
		})
	}
	if toSch != nil {
		_ = toSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			cols = append(cols, fmt.Sprintf("to_%s", col.Name))
			return false, nil
		})
	}
	return cols
}

// getDiffSqlSchema returns the schema of columns with data diff and "diff_type". This is used for diff splitter.
// When extracting the diff schema, the ordering must follow the ordering of given columns
func getDiffSqlSchema(diffTableSch sql.Schema, columns []string) (sql.Schema, []sql.Expression) {
	type column struct {
		sqlCol *sql.Column
		idx    int
	}

	columns = append(columns, "diff_type")
	colMap := make(map[string]*column)
	for _, c := range columns {
		colMap[c] = nil
	}

	var cols = make([]*sql.Column, len(columns))
	var getFieldCols = make([]sql.Expression, len(columns))

	for i, c := range diffTableSch {
		if _, ok := colMap[c.Name]; ok {
			colMap[c.Name] = &column{c, i}
		}
	}

	for i, c := range columns {
		col := colMap[c].sqlCol
		cols[i] = col
		getFieldCols[i] = expression.NewGetField(colMap[c].idx, col.Type, col.Name, col.Nullable)
	}

	return cols, getFieldCols
}
