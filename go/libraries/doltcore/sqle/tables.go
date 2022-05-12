// Copyright 2019 Dolthub, Inc.
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	partitionMultiplier = 2.0
)

var MaxRowsPerPartition uint64 = 32 * 1024
var MinRowsPerPartition uint64 = 1024

func init() {
	isTest := false
	for _, arg := range os.Args {
		lwr := strings.ToLower(arg)
		if lwr == "-test.v" ||
			lwr == "-test.run" ||
			strings.HasPrefix(lwr, "-test.testlogfile") ||
			strings.HasPrefix(lwr, "-test.timeout") ||
			strings.HasPrefix(lwr, "-test.count") {
			isTest = true
			break
		}
	}

	if isTest {
		MinRowsPerPartition = 2
	}
}

type projected interface {
	Project() []string
}

// DoltTable implements the sql.Table interface and gives access to dolt table rows and schema.
type DoltTable struct {
	tableName    string
	sqlSch       sql.PrimaryKeySchema
	db           SqlDatabase
	lockedToRoot *doltdb.RootValue
	nbf          *types.NomsBinFormat
	sch          schema.Schema
	autoIncCol   schema.Column

	projectedCols []string

	opts editor.Options
}

func NewDoltTable(name string, sch schema.Schema, tbl *doltdb.Table, db SqlDatabase, opts editor.Options) (*DoltTable, error) {
	var autoCol schema.Column
	_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.AutoIncrement {
			autoCol = col
			stop = true
		}
		return
	})

	sqlSch, err := sqlutil.FromDoltSchema(name, sch)
	if err != nil {
		return nil, err
	}

	return &DoltTable{
		tableName:     name,
		db:            db,
		nbf:           tbl.Format(),
		sch:           sch,
		sqlSch:        sqlSch,
		autoIncCol:    autoCol,
		projectedCols: nil,
		opts:          opts,
	}, nil
}

// LockedToRoot returns a version of this table with its root value locked to the given value. The table's values will
// not change as the session's root value changes. Appropriate for AS OF queries, or other use cases where the table's
// values should not change throughout execution of a session.
func (t DoltTable) LockedToRoot(rootValue *doltdb.RootValue) *DoltTable {
	t.lockedToRoot = rootValue
	return &t
}

// Internal interface for declaring the interfaces that read-only dolt tables are expected to implement
// Add new interfaces supported here, rather than in separate type assertions
type doltReadOnlyTableInterface interface {
	sql.Table2
	sql.TemporaryTable
	sql.IndexedTable
	sql.ForeignKeyTable
	sql.StatisticsTable
	sql.CheckTable
	sql.PrimaryKeyTable
}

var _ doltReadOnlyTableInterface = (*DoltTable)(nil)

// projected tables disabled for now.  Looks like some work needs to be done in the analyzer as there are cases
// where the projected columns do not contain every column needed.  Seed this with natural and other joins.  There
// may be other cases.
//var _ sql.ProjectedTable = (*DoltTable)(nil)

// WithIndexLookup implements sql.IndexedTable
func (t *DoltTable) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	return &IndexedDoltTable{
		table:       t,
		indexLookup: lookup,
	}
}

// doltTable returns the underlying doltTable from the current session
func (t *DoltTable) doltTable(ctx *sql.Context) (*doltdb.Table, error) {
	root := t.lockedToRoot
	var err error
	if root == nil {
		root, err = t.getRoot(ctx)
		if err != nil {
			return nil, err
		}
	}

	table, ok, err := root.GetTable(ctx, t.tableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("table not found: %s", t.tableName)
	}

	return table, nil
}

// getRoot returns the appropriate root value for this session. The only controlling factor
// is whether this is a temporary table or not.
func (t *DoltTable) getRoot(ctx *sql.Context) (*doltdb.RootValue, error) {
	return t.db.GetRoot(ctx)
}

// GetIndexes implements sql.IndexedTable
func (t *DoltTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	tbl, err := t.doltTable(ctx)
	if err != nil {
		return nil, err
	}

	return index.DoltIndexesFromTable(ctx, t.db.Name(), t.tableName, tbl)
}

// GetAutoIncrementValue gets the last AUTO_INCREMENT value
func (t *DoltTable) GetAutoIncrementValue(ctx *sql.Context) (interface{}, error) {
	table, err := t.doltTable(ctx)
	if err != nil {
		return nil, err
	}
	return table.GetAutoIncrementValue(ctx)
}

// Name returns the name of the table.
func (t *DoltTable) Name() string {
	return t.tableName
}

// String returns a human-readable string to display the name of this SQL node.
func (t *DoltTable) String() string {
	return t.tableName
}

// NumRows returns the unfiltered count of rows contained in the table
func (t *DoltTable) NumRows(ctx *sql.Context) (uint64, error) {
	table, err := t.doltTable(ctx)
	if err != nil {
		return 0, err
	}

	m, err := table.GetRowData(ctx)
	if err != nil {
		return 0, err
	}

	return m.Count(), nil
}

// Format returns the NomsBinFormat for the underlying table
func (t *DoltTable) Format() *types.NomsBinFormat {
	return t.nbf
}

// Schema returns the schema for this table.
func (t *DoltTable) Schema() sql.Schema {
	return t.sqlSchema().Schema
}

func (t *DoltTable) sqlSchema() sql.PrimaryKeySchema {
	if len(t.sqlSch.Schema) > 0 {
		return t.sqlSch
	}

	// TODO: fix panics
	sqlSch, err := sqlutil.FromDoltSchema(t.tableName, t.sch)
	if err != nil {
		panic(err)
	}

	t.sqlSch = sqlSch
	return sqlSch
}

// Partitions returns the partitions for this table.
func (t *DoltTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	table, err := t.doltTable(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := table.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	partitions := partitionsFromRows(ctx, rows)

	return newDoltTablePartitionIter(rows, partitions...), nil
}

func (t *DoltTable) IsTemporary() bool {
	return false
}

func (t *DoltTable) DataLength(ctx *sql.Context) (uint64, error) {
	schema := t.Schema()
	var numBytesPerRow uint64 = 0
	for _, col := range schema {
		switch n := col.Type.(type) {
		case sql.NumberType:
			numBytesPerRow += 8
		case sql.StringType:
			numBytesPerRow += uint64(n.MaxByteLength())
		case sql.BitType:
			numBytesPerRow += 1
		case sql.DatetimeType:
			numBytesPerRow += 8
		case sql.DecimalType:
			numBytesPerRow += uint64(n.MaximumScale())
		case sql.EnumType:
			numBytesPerRow += 2
		case sql.JsonType:
			numBytesPerRow += 20
		case sql.NullType:
			numBytesPerRow += 1
		case sql.TimeType:
			numBytesPerRow += 16
		case sql.YearType:
			numBytesPerRow += 8
		}
	}

	numRows, err := t.NumRows(ctx)
	if err != nil {
		return 0, err
	}

	return numBytesPerRow * numRows, nil
}

func (t *DoltTable) PrimaryKeySchema() sql.PrimaryKeySchema {
	return t.sqlSchema()
}

type emptyRowIterator struct{}

func (itr emptyRowIterator) Next(*sql.Context) (sql.Row, error) {
	return nil, io.EOF
}

func (itr emptyRowIterator) Close(*sql.Context) error {
	return nil
}

// PartitionRows returns the table rows for the partition given
func (t *DoltTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	table, err := t.doltTable(ctx)
	if err != nil {
		return nil, err
	}

	return partitionRows(ctx, table, t.projectedCols, partition)
}

func (t DoltTable) PartitionRows2(ctx *sql.Context, part sql.Partition) (sql.RowIter2, error) {
	table, err := t.doltTable(ctx)
	if err != nil {
		return nil, err
	}

	iter, err := partitionRows(ctx, table, t.projectedCols, part)
	if err != nil {
		return nil, err
	}

	return iter.(sql.RowIter2), err
}

func partitionRows(ctx *sql.Context, t *doltdb.Table, projCols []string, partition sql.Partition) (sql.RowIter, error) {
	switch typedPartition := partition.(type) {
	case doltTablePartition:
		return newRowIterator(ctx, t, projCols, typedPartition)
	case index.SinglePartition:
		return newRowIterator(ctx, t, projCols, doltTablePartition{rowData: typedPartition.RowData, end: NoUpperBound})
	}

	return nil, errors.New("unsupported partition type")
}

// WritableDoltTable allows updating, deleting, and inserting new rows. It implements sql.UpdatableTable and friends.
type WritableDoltTable struct {
	*DoltTable
	db Database
	ed writer.TableWriter
}

var _ doltTableInterface = (*WritableDoltTable)(nil)

// Internal interface for declaring the interfaces that writable dolt tables are expected to implement
type doltTableInterface interface {
	sql.Table2
	sql.UpdatableTable
	sql.DeletableTable
	sql.InsertableTable
	sql.ReplaceableTable
	sql.AutoIncrementTable
	sql.TruncateableTable
	sql.ProjectedTable
}

func (t *WritableDoltTable) setRoot(ctx *sql.Context, newRoot *doltdb.RootValue) error {
	return t.db.SetRoot(ctx, newRoot)
}

func (t *WritableDoltTable) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	return &WritableIndexedDoltTable{
		WritableDoltTable: t,
		indexLookup:       lookup,
	}
}

// WithProjections implements sql.ProjectedTable
func (t *WritableDoltTable) WithProjections(colNames []string) sql.Table {
	return &WritableDoltTable{
		DoltTable: t.DoltTable.WithProjections(colNames).(*DoltTable),
		db:        t.db,
		ed:        t.ed,
	}
}

// Inserter implements sql.InsertableTable
func (t *WritableDoltTable) Inserter(ctx *sql.Context) sql.RowInserter {
	te, err := t.getTableEditor(ctx)
	if err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	return te
}

func (t *WritableDoltTable) getTableEditor(ctx *sql.Context) (ed writer.TableWriter, err error) {
	ds := dsess.DSessFromSess(ctx.Session)

	var batched = ds.BatchMode() == dsess.Batched

	// In batched mode, reuse the same table editor. Otherwise, hand out a new one
	if batched {
		if t.ed != nil {
			return t.ed, nil
		}
	}

	state, _, err := ds.LookupDbState(ctx, t.db.name)
	if err != nil {
		return nil, err
	}

	setter := ds.SetRoot
	ed, err = state.WriteSession.GetTableWriter(ctx, t.tableName, t.db.Name(), setter, batched)

	if err != nil {
		return nil, err
	}
	if batched {
		t.ed = ed
	}

	return ed, nil
}

// Deleter implements sql.DeletableTable
func (t *WritableDoltTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	te, err := t.getTableEditor(ctx)
	if err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	return te
}

// Replacer implements sql.ReplaceableTable
func (t *WritableDoltTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	te, err := t.getTableEditor(ctx)
	if err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	return te
}

// Truncate implements sql.TruncateableTable
func (t *WritableDoltTable) Truncate(ctx *sql.Context) (int, error) {
	table, err := t.doltTable(ctx)
	if err != nil {
		return 0, err
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return 0, err
	}

	rowData, err := table.GetRowData(ctx)
	if err != nil {
		return 0, err
	}
	numOfRows := int(rowData.Count())

	newTable, err := truncate(ctx, table, sch)
	if err != nil {
		return 0, err
	}

	root, err := t.getRoot(ctx)
	if err != nil {
		return 0, err
	}
	newRoot, err := root.PutTable(ctx, t.tableName, newTable)
	if err != nil {
		return 0, err
	}

	err = t.setRoot(ctx, newRoot)
	if err != nil {
		return 0, err
	}

	return numOfRows, nil
}

// truncate returns an empty copy of the table given by setting the rows and indexes to empty. The schema can be
// updated at the same time.
func truncate(ctx *sql.Context, table *doltdb.Table, sch schema.Schema) (*doltdb.Table, error) {
	empty, err := durable.NewEmptyIndex(ctx, table.ValueReadWriter(), sch)
	if err != nil {
		return nil, err
	}

	idxSet, err := table.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	for _, idx := range sch.Indexes().AllIndexes() {
		idxSet, err = idxSet.PutIndex(ctx, idx.Name(), empty)
		if err != nil {
			return nil, err
		}
	}

	// truncate table resets auto-increment value
	return doltdb.NewTable(ctx, table.ValueReadWriter(), sch, empty, idxSet, nil)
}

// Updater implements sql.UpdatableTable
func (t *WritableDoltTable) Updater(ctx *sql.Context) sql.RowUpdater {
	te, err := t.getTableEditor(ctx)
	if err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	return te
}

// AutoIncrementSetter implements sql.AutoIncrementTable
func (t *WritableDoltTable) AutoIncrementSetter(ctx *sql.Context) sql.AutoIncrementSetter {
	te, err := t.getTableEditor(ctx)
	if err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	return te
}

// PeekNextAutoIncrementValue implements sql.AutoIncrementTable
func (t *WritableDoltTable) PeekNextAutoIncrementValue(ctx *sql.Context) (interface{}, error) {
	if !t.autoIncCol.AutoIncrement {
		return nil, sql.ErrNoAutoIncrementCol
	}

	return t.getTableAutoIncrementValue(ctx)
}

// GetNextAutoIncrementValue implements sql.AutoIncrementTable
func (t *WritableDoltTable) GetNextAutoIncrementValue(ctx *sql.Context, potentialVal interface{}) (uint64, error) {
	if !t.autoIncCol.AutoIncrement {
		return 0, sql.ErrNoAutoIncrementCol
	}

	ed, err := t.getTableEditor(ctx)
	if err != nil {
		return 0, err
	}

	return ed.GetNextAutoIncrementValue(ctx, potentialVal)
}

func (t *WritableDoltTable) getTableAutoIncrementValue(ctx *sql.Context) (interface{}, error) {
	return t.DoltTable.GetAutoIncrementValue(ctx)
}

func (t *DoltTable) GetChecks(ctx *sql.Context) ([]sql.CheckDefinition, error) {
	table, err := t.doltTable(ctx)
	if err != nil {
		return nil, err
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	return checksInSchema(sch), nil
}

func checksInSchema(sch schema.Schema) []sql.CheckDefinition {
	if sch.Checks() == nil {
		return nil
	}

	checks := make([]sql.CheckDefinition, sch.Checks().Count())
	for i, check := range sch.Checks().AllChecks() {
		checks[i] = sql.CheckDefinition{
			Name:            check.Name(),
			CheckExpression: check.Expression(),
			Enforced:        check.Enforced(),
		}
	}
	return checks
}

// GetDeclaredForeignKeys implements sql.ForeignKeyTable
func (t *DoltTable) GetDeclaredForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	root, err := t.getRoot(ctx)
	if err != nil {
		return nil, err
	}

	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	declaredFks, _ := fkc.KeysForTable(t.tableName)
	toReturn := make([]sql.ForeignKeyConstraint, len(declaredFks))

	for i, fk := range declaredFks {
		if len(fk.UnresolvedFKDetails.TableColumns) > 0 && len(fk.UnresolvedFKDetails.ReferencedTableColumns) > 0 {
			//TODO: implement multi-db support for foreign keys
			toReturn[i] = sql.ForeignKeyConstraint{
				Name:           fk.Name,
				Database:       t.db.Name(),
				Table:          fk.TableName,
				Columns:        fk.UnresolvedFKDetails.TableColumns,
				ParentDatabase: t.db.Name(),
				ParentTable:    fk.ReferencedTableName,
				ParentColumns:  fk.UnresolvedFKDetails.ReferencedTableColumns,
				OnUpdate:       toReferentialAction(fk.OnUpdate),
				OnDelete:       toReferentialAction(fk.OnDelete),
				IsResolved:     fk.IsResolved(),
			}
			continue
		}
		parent, ok, err := root.GetTable(ctx, fk.ReferencedTableName)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("cannot find table %s referenced in foreign key %s", fk.ReferencedTableName, fk.Name)
		}
		parentSch, err := parent.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		toReturn[i], err = toForeignKeyConstraint(fk, t.db.Name(), t.sch, parentSch)
		if err != nil {
			return nil, err
		}
	}

	return toReturn, nil
}

// GetReferencedForeignKeys implements sql.ForeignKeyTable
func (t *DoltTable) GetReferencedForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	root, err := t.getRoot(ctx)
	if err != nil {
		return nil, err
	}

	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	_, referencedByFk := fkc.KeysForTable(t.tableName)
	toReturn := make([]sql.ForeignKeyConstraint, len(referencedByFk))

	for i, fk := range referencedByFk {
		if len(fk.UnresolvedFKDetails.TableColumns) > 0 && len(fk.UnresolvedFKDetails.ReferencedTableColumns) > 0 {
			//TODO: implement multi-db support for foreign keys
			toReturn[i] = sql.ForeignKeyConstraint{
				Name:           fk.Name,
				Database:       t.db.Name(),
				Table:          fk.TableName,
				Columns:        fk.UnresolvedFKDetails.TableColumns,
				ParentDatabase: t.db.Name(),
				ParentTable:    fk.ReferencedTableName,
				ParentColumns:  fk.UnresolvedFKDetails.ReferencedTableColumns,
				OnUpdate:       toReferentialAction(fk.OnUpdate),
				OnDelete:       toReferentialAction(fk.OnDelete),
				IsResolved:     fk.IsResolved(),
			}
			continue
		}
		child, ok, err := root.GetTable(ctx, fk.TableName)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("cannot find table %s declared by foreign key %s", fk.TableName, fk.Name)
		}
		childSch, err := child.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		toReturn[i], err = toForeignKeyConstraint(fk, t.db.Name(), childSch, t.sch)
		if err != nil {
			return nil, err
		}
	}

	return toReturn, nil
}

// CreateIndexForForeignKey implements sql.ForeignKeyTable
func (t DoltTable) CreateIndexForForeignKey(ctx *sql.Context, indexName string, using sql.IndexUsing, constraint sql.IndexConstraint, columns []sql.IndexColumn) error {
	return fmt.Errorf("no foreign key operations on a read-only table")
}

// AddForeignKey implements sql.ForeignKeyTable
func (t DoltTable) AddForeignKey(ctx *sql.Context, fk sql.ForeignKeyConstraint) error {
	return fmt.Errorf("no foreign key operations on a read-only table")
}

// DropForeignKey implements sql.ForeignKeyTable
func (t DoltTable) DropForeignKey(ctx *sql.Context, fkName string) error {
	return fmt.Errorf("no foreign key operations on a read-only table")
}

// UpdateForeignKey implements sql.ForeignKeyTable
func (t DoltTable) UpdateForeignKey(ctx *sql.Context, fkName string, fk sql.ForeignKeyConstraint) error {
	return fmt.Errorf("no foreign key operations on a read-only table")
}

// GetForeignKeyUpdater implements sql.ForeignKeyTable
func (t DoltTable) GetForeignKeyUpdater(ctx *sql.Context) sql.ForeignKeyUpdater {
	return nil
}

// Projections implements sql.ProjectedTable
func (t *DoltTable) Projections() []string {
	return t.projectedCols
}

// WithProjections implements sql.ProjectedTable
func (t *DoltTable) WithProjections(colNames []string) sql.Table {
	nt := *t
	nt.projectedCols = colNames
	return &nt
}

var _ sql.PartitionIter = (*doltTablePartitionIter)(nil)

// doltTablePartitionIter, an object that knows how to return the single partition exactly once.
type doltTablePartitionIter struct {
	i          int
	mu         *sync.Mutex
	rowData    durable.Index
	partitions []doltTablePartition
}

func newDoltTablePartitionIter(rowData durable.Index, partitions ...doltTablePartition) *doltTablePartitionIter {
	return &doltTablePartitionIter{0, &sync.Mutex{}, rowData, partitions}
}

// Close is required by the sql.PartitionIter interface. Does nothing.
func (itr *doltTablePartitionIter) Close(*sql.Context) error {
	return nil
}

// Next returns the next partition if there is one, or io.EOF if there isn't.
func (itr *doltTablePartitionIter) Next(*sql.Context) (sql.Partition, error) {
	itr.mu.Lock()
	defer itr.mu.Unlock()

	if itr.i >= len(itr.partitions) {
		return nil, io.EOF
	}

	partition := itr.partitions[itr.i]
	itr.i++

	return partition, nil
}

var _ sql.Partition = doltTablePartition{}

const NoUpperBound = 0xffffffffffffffff

type doltTablePartition struct {
	// half-open index range of partition: [start, end)
	start, end uint64

	rowData durable.Index
}

func partitionsFromRows(ctx context.Context, rows durable.Index) []doltTablePartition {
	if rows.Empty() {
		return []doltTablePartition{
			{start: 0, end: 0, rowData: rows},
		}
	}

	return partitionsFromTableRows(rows)
}

func partitionsFromTableRows(rows durable.Index) []doltTablePartition {
	numElements := rows.Count()
	itemsPerPartition := MaxRowsPerPartition
	numPartitions := (numElements / itemsPerPartition) + 1

	if numPartitions < uint64(partitionMultiplier*runtime.NumCPU()) {
		itemsPerPartition = numElements / uint64(partitionMultiplier*runtime.NumCPU())
		if itemsPerPartition == 0 {
			itemsPerPartition = numElements
			numPartitions = 1
		} else {
			numPartitions = (numElements / itemsPerPartition) + 1
		}
	}

	partitions := make([]doltTablePartition, numPartitions)
	for i := uint64(0); i < numPartitions-1; i++ {
		partitions[i] = doltTablePartition{
			start:   i * itemsPerPartition,
			end:     (i + 1) * itemsPerPartition,
			rowData: rows,
		}
	}

	partitions[numPartitions-1] = doltTablePartition{
		start:   (numPartitions - 1) * itemsPerPartition,
		end:     numElements,
		rowData: rows,
	}

	return partitions
}

// Key returns the key for this partition, which must uniquely identity the partition.
func (p doltTablePartition) Key() []byte {
	return []byte(strconv.FormatUint(p.start, 10) + " >= i < " + strconv.FormatUint(p.end, 10))
}

// IteratorForPartition returns a types.MapIterator implementation which will iterate through the values
// for index = start; index < end.  This iterator is not thread safe and should only be used from a single go routine
// unless paired with a mutex
func (p doltTablePartition) IteratorForPartition(ctx context.Context, idx durable.Index) (types.MapTupleIterator, error) {
	m := durable.NomsMapFromIndex(idx)
	return m.RangeIterator(ctx, p.start, p.end)
}

type partitionIter struct {
	pos  uint64
	end  uint64
	iter types.MapIterator
}

func newPartitionIter(ctx context.Context, m types.Map, start, end uint64) (*partitionIter, error) {
	iter, err := m.BufferedIteratorAt(ctx, start)

	if err != nil {
		return nil, err
	}

	return &partitionIter{
		start,
		end,
		iter,
	}, nil
}

func (p *partitionIter) Next(ctx context.Context) (k, v types.Value, err error) {
	if p.pos >= p.end {
		// types package does not use io.EOF
		return nil, nil, nil
	}

	p.pos++
	return p.iter.Next(ctx)
}

// AlterableDoltTable allows altering the schema of the table. It implements sql.AlterableTable.
type AlterableDoltTable struct {
	WritableDoltTable
}

func (t *AlterableDoltTable) PrimaryKeySchema() sql.PrimaryKeySchema {
	return t.sqlSch
}

// Internal interface for declaring the interfaces that dolt tables with an alterable schema are expected to implement
// Add new interfaces supported here, rather than in separate type assertions
type doltAlterableTableInterface interface {
	sql.AlterableTable
	sql.IndexAlterableTable
	sql.ForeignKeyTable
	sql.CheckAlterableTable
	sql.PrimaryKeyAlterableTable
	sql.ProjectedTable
}

var _ doltAlterableTableInterface = (*AlterableDoltTable)(nil)
var _ sql.RewritableTable = (*AlterableDoltTable)(nil)

func (t *AlterableDoltTable) WithProjections(colNames []string) sql.Table {
	return &AlterableDoltTable{WritableDoltTable: *t.WritableDoltTable.WithProjections(colNames).(*WritableDoltTable)}
}

// AddColumn implements sql.AlterableTable
func (t *AlterableDoltTable) AddColumn(ctx *sql.Context, column *sql.Column, order *sql.ColumnOrder) error {
	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}

	table, _, err := root.GetTable(ctx, t.tableName)
	if err != nil {
		return err
	}

	ti, err := typeinfo.FromSqlType(column.Type)
	if err != nil {
		return err
	}
	tags, err := root.GenerateTagsForNewColumns(ctx, t.tableName, []string{column.Name}, []types.NomsKind{ti.NomsKind()}, nil)
	if err != nil {
		return err
	}

	col, err := sqlutil.ToDoltCol(tags[0], column)
	if err != nil {
		return err
	}

	if col.IsPartOfPK {
		return errors.New("adding primary keys is not supported")
	}

	nullable := NotNull
	if col.IsNullable() {
		nullable = Null
	}

	updatedTable, err := addColumnToTable(ctx, root, table, t.tableName, col.Tag, col.Name, col.TypeInfo, nullable, column.Default, col.Comment, order)
	if err != nil {
		return err
	}

	if column.AutoIncrement {
		ws, err := t.db.GetWorkingSet(ctx)
		if err != nil {
			return err
		}
		ait, err := t.db.gs.GetAutoIncrementTracker(ctx, ws)
		if err != nil {
			return err
		}
		ait.AddNewTable(t.tableName)
	}

	newRoot, err := root.PutTable(ctx, t.tableName, updatedTable)
	if err != nil {
		return err
	}

	err = t.setRoot(ctx, newRoot)
	if err != nil {
		return err
	}

	return t.updateFromRoot(ctx, newRoot)
}

func (t *AlterableDoltTable) ShouldRewriteTable(
	ctx *sql.Context,
	oldSchema sql.PrimaryKeySchema,
	newSchema sql.PrimaryKeySchema,
	modifiedColumn *sql.Column,
) bool {
	// TODO: this could be a lot more specific, we don't always need to rewrite on schema changes in either format
	return types.IsFormat_DOLT_1(t.nbf)
}

func (t *AlterableDoltTable) RewriteInserter(ctx *sql.Context, newSchema sql.PrimaryKeySchema) (sql.RowInserter, error) {
	sess := dsess.DSessFromSess(ctx.Session)

	// Begin by creating a new table with the same name and the new schema, then removing all its existing rows
	dbState, ok, err := sess.LookupDbState(ctx, t.db.Name())
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, fmt.Errorf("database %s not found in session", t.db.Name())
	}

	ws := dbState.WorkingSet

	head, err := sess.GetHeadCommit(ctx, t.db.Name())
	if err != nil {
		return nil, err
	}

	headRoot, err := head.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	dt, err := t.doltTable(ctx)
	if err != nil {
		return nil, err
	}

	oldSch, err := dt.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	newSch, err := sqlutil.ToDoltSchema(ctx, ws.WorkingRoot(), t.Name(), newSchema, headRoot)
	if err != nil {
		return nil, err
	}

	newSch, err = schema.Adapt(oldSch, newSch) // improvise, overcome
	if err != nil {
		return nil, err
	}

	// If we have an auto increment column, we need to set it here before we begin the rewrite process
	if schema.HasAutoIncrement(newSch) {
		newSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			if col.AutoIncrement {
				t.autoIncCol = col
				return true, nil
			}
			return false, nil
		})
	}

	dt, err = truncate(ctx, dt, newSch)
	if err != nil {
		return nil, err
	}

	// We can't just call getTableEditor here because it uses the session state, which we can't update until after the
	// rewrite operation
	opts := dbState.WriteSession.GetOptions()
	opts.ForeignKeyChecksDisabled = true

	newRoot, err := ws.WorkingRoot().PutTable(ctx, t.Name(), dt)
	if err != nil {
		return nil, err
	}

	newWs := ws.WithWorkingRoot(newRoot)

	// TODO: figure out locking. Other DBs automatically lock a table during this kind of operation, we should probably
	//  do the same. We're messing with global auto-increment values here and it's not safe.
	ait, err := t.db.gs.GetAutoIncrementTracker(ctx, newWs)
	if err != nil {
		return nil, err
	}

	writeSession := writer.NewWriteSession(dt.Format(), newWs, ait, opts)
	ed, err := writeSession.GetTableWriter(ctx, t.Name(), t.db.Name(), sess.SetRoot, false)
	if err != nil {
		return nil, err
	}

	return ed, nil
}

// DropColumn implements sql.AlterableTable
func (t *AlterableDoltTable) DropColumn(ctx *sql.Context, columnName string) error {
	if types.IsFormat_DOLT_1(t.nbf) {
		return nil
	}

	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}

	updatedTable, _, err := root.GetTable(ctx, t.tableName)
	if err != nil {
		return err
	}

	sch, err := updatedTable.GetSchema(ctx)
	if err != nil {
		return err
	}

	for _, index := range sch.Indexes().IndexesWithColumn(columnName) {
		_, err = sch.Indexes().RemoveIndex(index.Name())
		if err != nil {
			return err
		}
		updatedTable, err = updatedTable.DeleteIndexRowData(ctx, index.Name())
		if err != nil {
			return err
		}
	}

	updatedTable, err = dropColumn(ctx, updatedTable, columnName)
	if err != nil {
		return err
	}

	updatedTable, err = t.dropColumnData(ctx, updatedTable, sch, columnName)
	if err != nil {
		return err
	}

	newRoot, err := root.PutTable(ctx, t.tableName, updatedTable)
	if err != nil {
		return err
	}

	err = t.setRoot(ctx, newRoot)
	if err != nil {
		return err
	}

	return t.updateFromRoot(ctx, newRoot)
}

// dropColumnData drops values for the specified column from the underlying storage layer
func (t *AlterableDoltTable) dropColumnData(ctx *sql.Context, updatedTable *doltdb.Table, sch schema.Schema, columnName string) (*doltdb.Table, error) {
	nomsRowData, err := updatedTable.GetNomsRowData(ctx)
	if err != nil {
		return nil, err
	}

	column, ok := sch.GetAllCols().GetByName(columnName)
	if !ok {
		return nil, sql.ErrColumnNotFound.New(columnName)
	}

	mapEditor := nomsRowData.Edit()
	defer mapEditor.Close(ctx)

	err = nomsRowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
		if t, ok := value.(types.Tuple); ok {
			newTuple, err := types.NewTuple(nomsRowData.Format())
			if err != nil {
				return true, err
			}

			idx := uint64(0)
			for idx < t.Len() {
				tTag, err := t.Get(idx)
				if err != nil {
					return true, err
				}

				tValue, err := t.Get(idx + 1)
				if err != nil {
					return true, err
				}

				if tTag.Equals(types.Uint(column.Tag)) == false {
					newTuple, err = newTuple.Append(tTag, tValue)
					if err != nil {
						return true, err
					}
				}

				idx += 2
			}
			mapEditor.Set(key, newTuple)
		}

		return false, nil
	})
	if err != nil {
		return nil, err
	}

	newMapData, err := mapEditor.Map(ctx)
	if err != nil {
		return nil, err
	}

	return updatedTable.UpdateNomsRows(ctx, newMapData)
}

// ModifyColumn implements sql.AlterableTable
func (t *AlterableDoltTable) ModifyColumn(ctx *sql.Context, columnName string, column *sql.Column, order *sql.ColumnOrder) error {
	if types.IsFormat_DOLT_1(t.nbf) {
		return nil
	}

	ws, err := t.db.GetWorkingSet(ctx)
	if err != nil {
		return err
	}
	root := ws.WorkingRoot()

	table, _, err := root.GetTable(ctx, t.tableName)
	if err != nil {
		return err
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return err
	}

	existingCol, ok := sch.GetAllCols().GetByNameCaseInsensitive(columnName)
	if !ok {
		panic(fmt.Sprintf("Column %s not found. This is a bug.", columnName))
	}

	col, err := sqlutil.ToDoltCol(existingCol.Tag, column)
	if err != nil {
		return err
	}

	if !existingCol.TypeInfo.Equals(col.TypeInfo) {
		if existingCol.Kind != col.Kind { // We only change the tag when the underlying Noms kind changes
			tags, err := root.GenerateTagsForNewColumns(ctx, t.tableName, []string{col.Name}, []types.NomsKind{col.Kind}, nil)
			if err != nil {
				return err
			}
			if len(tags) != 1 {
				return fmt.Errorf("expected a generated tag length of 1")
			}
			col.Tag = tags[0]
		}
	}

	updatedTable, err := modifyColumn(ctx, table, existingCol, col, order, t.opts)
	if err != nil {
		return err
	}

	// For auto columns modified to be auto increment, we have more work to do
	if !existingCol.AutoIncrement && col.AutoIncrement {
		updatedSch, err := updatedTable.GetSchema(ctx)
		if err != nil {
			return err
		}

		rowData, err := updatedTable.GetRowData(ctx)
		if err != nil {
			return err
		}

		// Note that we aren't calling the public PartitionRows, because it always gets the table data from the session
		// root, which hasn't been updated yet
		rowIter, err := partitionRows(ctx, updatedTable, t.projectedCols, index.SinglePartition{RowData: rowData})
		if err != nil {
			return err
		}

		initialValue := column.Type.Zero()
		colIdx := updatedSch.GetAllCols().IndexOf(columnName)

		for {
			r, err := rowIter.Next(ctx)
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			cmp, err := column.Type.Compare(initialValue, r[colIdx])
			if err != nil {
				return err
			}
			if cmp < 0 {
				initialValue = r[colIdx]
			}
		}

		seq, err := globalstate.CoerceAutoIncrementValue(initialValue)
		if err != nil {
			return err
		}
		seq++

		updatedTable, err = updatedTable.SetAutoIncrementValue(ctx, seq)
		if err != nil {
			return err
		}

		ait, err := t.db.gs.GetAutoIncrementTracker(ctx, ws)
		if err != nil {
			return err
		}
		ait.AddNewTable(t.tableName)
		ait.Set(t.tableName, seq)
	}

	newRoot, err := root.PutTable(ctx, t.tableName, updatedTable)
	if err != nil {
		return err
	}

	err = t.setRoot(ctx, newRoot)
	if err != nil {
		return err
	}

	return nil
	// TODO: we can't make this update right now because renames happen in two passes if you rename a column mentioned in
	//  a default value, and one of those two passes will have the old name for the column. Fix this by not analyzing
	//  column defaults in NewDoltTable.
	// return t.updateFromRoot(ctx, newRoot)
}

func increment(val types.Value) types.Value {
	switch val := val.(type) {
	case types.Int:
		return val + 1
	case types.Float:
		return val + 1
	case types.Uint:
		return val + 1
	default:
		panic(fmt.Sprintf("unexpected auto inc column type %T", val))
	}
}

// CreateIndex implements sql.IndexAlterableTable
func (t *AlterableDoltTable) CreateIndex(
	ctx *sql.Context,
	indexName string,
	using sql.IndexUsing,
	constraint sql.IndexConstraint,
	indexColumns []sql.IndexColumn,
	comment string,
) error {
	if constraint != sql.IndexConstraint_None && constraint != sql.IndexConstraint_Unique {
		return fmt.Errorf("only the following types of index constraints are supported: none, unique")
	}
	columns := make([]string, len(indexColumns))
	for i, indexCol := range indexColumns {
		columns[i] = indexCol.Name
	}

	table, err := t.doltTable(ctx)
	if err != nil {
		return err
	}

	ret, err := creation.CreateIndex(
		ctx,
		table,
		indexName,
		columns,
		constraint == sql.IndexConstraint_Unique,
		true,
		comment,
		t.opts,
	)
	if err != nil {
		return err
	}
	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}
	if ret.OldIndex != nil && ret.OldIndex != ret.NewIndex { // old index was replaced, so we update foreign keys
		fkc, err := root.GetForeignKeyCollection(ctx)
		if err != nil {
			return err
		}
		for _, fk := range fkc.AllKeys() {
			newFk := fk
			if t.tableName == fk.TableName && fk.TableIndex == ret.OldIndex.Name() {
				newFk.TableIndex = ret.NewIndex.Name()
			}
			if t.tableName == fk.ReferencedTableName && fk.ReferencedTableIndex == ret.OldIndex.Name() {
				newFk.ReferencedTableIndex = ret.NewIndex.Name()
			}
			fkc.RemoveKeys(fk)
			err = fkc.AddKeys(newFk)
			if err != nil {
				return err
			}
		}
		root, err = root.PutForeignKeyCollection(ctx, fkc)
		if err != nil {
			return err
		}
	}
	newRoot, err := root.PutTable(ctx, t.tableName, ret.NewTable)
	if err != nil {
		return err
	}

	err = t.setRoot(ctx, newRoot)

	if err != nil {
		return err
	}
	return t.updateFromRoot(ctx, newRoot)
}

// DropIndex implements sql.IndexAlterableTable
func (t *AlterableDoltTable) DropIndex(ctx *sql.Context, indexName string) error {
	if types.IsFormat_DOLT_1(t.nbf) {
		return nil
	}

	// We disallow removing internal dolt_ tables from SQL directly
	if strings.HasPrefix(indexName, "dolt_") {
		return fmt.Errorf("dolt internal indexes may not be dropped")
	}
	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}

	newTable, _, err := t.dropIndex(ctx, indexName)
	if err != nil {
		return err
	}
	newRoot, err := root.PutTable(ctx, t.tableName, newTable)
	if err != nil {
		return err
	}
	err = t.setRoot(ctx, newRoot)
	if err != nil {
		return err
	}
	return t.updateFromRoot(ctx, newRoot)
}

// RenameIndex implements sql.IndexAlterableTable
func (t *AlterableDoltTable) RenameIndex(ctx *sql.Context, fromIndexName string, toIndexName string) error {
	// RenameIndex will error if there is a name collision or an index does not exist
	_, err := t.sch.Indexes().RenameIndex(fromIndexName, toIndexName)
	if err != nil {
		return err
	}

	table, err := t.doltTable(ctx)
	if err != nil {
		return err
	}

	newTable, err := table.UpdateSchema(ctx, t.sch)
	if err != nil {
		return err
	}
	newTable, err = newTable.RenameIndexRowData(ctx, fromIndexName, toIndexName)
	if err != nil {
		return err
	}

	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}
	newRoot, err := root.PutTable(ctx, t.tableName, newTable)
	if err != nil {
		return err
	}

	err = t.setRoot(ctx, newRoot)
	if err != nil {
		return err
	}
	return t.updateFromRoot(ctx, newRoot)
}

// AddForeignKey implements sql.ForeignKeyTable
func (t *AlterableDoltTable) AddForeignKey(ctx *sql.Context, sqlFk sql.ForeignKeyConstraint) error {
	if sqlFk.Name != "" && !doltdb.IsValidForeignKeyName(sqlFk.Name) {
		return fmt.Errorf("invalid foreign key name `%s` as it must match the regular expression %s", sqlFk.Name, doltdb.ForeignKeyNameRegexStr)
	}
	if strings.ToLower(sqlFk.Database) != strings.ToLower(sqlFk.ParentDatabase) || strings.ToLower(sqlFk.Database) != strings.ToLower(t.db.Name()) {
		return fmt.Errorf("only foreign keys on the same database are currently supported")
	}

	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}
	tbl, _, ok, err := root.GetTableInsensitive(ctx, t.tableName)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrTableNotFound.New(t.tableName)
	}

	onUpdateRefAction, err := parseFkReferentialAction(sqlFk.OnUpdate)
	if err != nil {
		return err
	}
	onDeleteRefAction, err := parseFkReferentialAction(sqlFk.OnDelete)
	if err != nil {
		return err
	}

	var doltFk doltdb.ForeignKey

	if sqlFk.IsResolved {
		colTags := make([]uint64, len(sqlFk.Columns))
		for i, col := range sqlFk.Columns {
			tableCol, ok := t.sch.GetAllCols().GetByNameCaseInsensitive(col)
			if !ok {
				return fmt.Errorf("table `%s` does not have column `%s`", sqlFk.Table, col)
			}
			colTags[i] = tableCol.Tag
		}

		var refTbl *doltdb.Table
		var ok bool
		var refSch schema.Schema
		if sqlFk.IsSelfReferential() {
			refTbl = tbl
			refSch = t.sch
		} else {
			refTbl, _, ok, err = root.GetTableInsensitive(ctx, sqlFk.ParentTable)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("referenced table `%s` does not exist", sqlFk.ParentTable)
			}
			refSch, err = refTbl.GetSchema(ctx)
			if err != nil {
				return err
			}
		}

		refColTags := make([]uint64, len(sqlFk.ParentColumns))
		for i, name := range sqlFk.ParentColumns {
			refCol, ok := refSch.GetAllCols().GetByNameCaseInsensitive(name)
			if !ok {
				return fmt.Errorf("table `%s` does not have column `%s`", sqlFk.ParentTable, name)
			}
			refColTags[i] = refCol.Tag
		}

		tableIndex, ok, err := findIndexWithPrefix(t.sch, sqlFk.Columns)
		if err != nil {
			return err
		}
		if !ok {
			// The engine matched on a primary key, and Dolt does not yet support using the primary key within the
			// schema.Index interface (which is used internally to represent indexes across the codebase). In the
			// meantime, we must generate a duplicate key over the primary key.
			//TODO: use the primary key as-is
			idxReturn, err := creation.CreateIndex(ctx, tbl, "", sqlFk.Columns, false, false, "", editor.Options{
				ForeignKeyChecksDisabled: true,
				Deaf:                     t.opts.Deaf,
				Tempdir:                  t.opts.Tempdir,
			})
			if err != nil {
				return err
			}
			tableIndex = idxReturn.NewIndex
			tbl = idxReturn.NewTable
			root, err = root.PutTable(ctx, t.tableName, idxReturn.NewTable)
			if sqlFk.IsSelfReferential() {
				refTbl = idxReturn.NewTable
			}
		}

		refTableIndex, ok, err := findIndexWithPrefix(refSch, sqlFk.ParentColumns)
		if err != nil {
			return err
		}
		if !ok {
			// The engine matched on a primary key, and Dolt does not yet support using the primary key within the
			// schema.Index interface (which is used internally to represent indexes across the codebase). In the
			// meantime, we must generate a duplicate key over the primary key.
			//TODO: use the primary key as-is
			var refPkTags []uint64
			for _, i := range refSch.GetPkOrdinals() {
				refPkTags = append(refPkTags, refSch.GetAllCols().GetAtIndex(i).Tag)
			}

			var colNames []string
			for _, t := range refColTags {
				c, _ := refSch.GetAllCols().GetByTag(t)
				colNames = append(colNames, c.Name)
			}

			// Our duplicate index is only unique if it's the entire primary key (which is by definition unique)
			unique := len(refPkTags) == len(refColTags)
			idxReturn, err := creation.CreateIndex(ctx, refTbl, "", colNames, unique, false, "", editor.Options{
				ForeignKeyChecksDisabled: true,
				Deaf:                     t.opts.Deaf,
				Tempdir:                  t.opts.Tempdir,
			})
			if err != nil {
				return err
			}
			refTbl = idxReturn.NewTable
			refTableIndex = idxReturn.NewIndex
			root, err = root.PutTable(ctx, sqlFk.ParentTable, idxReturn.NewTable)
			if err != nil {
				return err
			}
		}

		doltFk = doltdb.ForeignKey{
			Name:                   sqlFk.Name,
			TableName:              sqlFk.Table,
			TableIndex:             tableIndex.Name(),
			TableColumns:           colTags,
			ReferencedTableName:    sqlFk.ParentTable,
			ReferencedTableIndex:   refTableIndex.Name(),
			ReferencedTableColumns: refColTags,
			OnUpdate:               onUpdateRefAction,
			OnDelete:               onDeleteRefAction,
			UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
				TableColumns:           sqlFk.Columns,
				ReferencedTableColumns: sqlFk.ParentColumns,
			},
		}
	} else {
		doltFk = doltdb.ForeignKey{
			Name:                   sqlFk.Name,
			TableName:              sqlFk.Table,
			TableIndex:             "",
			TableColumns:           nil,
			ReferencedTableName:    sqlFk.ParentTable,
			ReferencedTableIndex:   "",
			ReferencedTableColumns: nil,
			OnUpdate:               onUpdateRefAction,
			OnDelete:               onDeleteRefAction,
			UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
				TableColumns:           sqlFk.Columns,
				ReferencedTableColumns: sqlFk.ParentColumns,
			},
		}
	}

	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	err = fkc.AddKeys(doltFk)
	if err != nil {
		return err
	}
	root, err = root.PutForeignKeyCollection(ctx, fkc)
	if err != nil {
		return err
	}

	err = t.setRoot(ctx, root)
	if err != nil {
		return err
	}
	return t.updateFromRoot(ctx, root)
}

// DropForeignKey implements sql.ForeignKeyTable
func (t *AlterableDoltTable) DropForeignKey(ctx *sql.Context, fkName string) error {
	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}
	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	if !fkc.RemoveKeyByName(fkName) {
		return sql.ErrForeignKeyNotFound.New(fkName, t.tableName)
	}
	newRoot, err := root.PutForeignKeyCollection(ctx, fkc)
	if err != nil {
		return err
	}

	err = t.setRoot(ctx, newRoot)
	if err != nil {
		return err
	}
	return t.updateFromRoot(ctx, newRoot)
}

// UpdateForeignKey implements sql.ForeignKeyTable
func (t *AlterableDoltTable) UpdateForeignKey(ctx *sql.Context, fkName string, sqlFk sql.ForeignKeyConstraint) error {
	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}
	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	doltFk, ok := fkc.GetByNameCaseInsensitive(fkName)
	if !ok {
		return sql.ErrForeignKeyNotFound.New(fkName, t.tableName)
	}
	fkc.RemoveKeyByName(doltFk.Name)
	doltFk.TableName = sqlFk.Table
	doltFk.ReferencedTableName = sqlFk.ParentTable
	doltFk.UnresolvedFKDetails.TableColumns = sqlFk.Columns
	doltFk.UnresolvedFKDetails.ReferencedTableColumns = sqlFk.ParentColumns

	if doltFk.IsResolved() && !sqlFk.IsResolved { // Need to unresolve the foreign key
		doltFk.TableIndex = ""
		doltFk.TableColumns = nil
		doltFk.ReferencedTableIndex = ""
		doltFk.ReferencedTableColumns = nil
	} else if !doltFk.IsResolved() && sqlFk.IsResolved { // Need to assign tags and indexes since it's resolved
		tbl, _, ok, err := root.GetTableInsensitive(ctx, t.tableName)
		if err != nil {
			return err
		}
		if !ok {
			return sql.ErrTableNotFound.New(t.tableName)
		}

		colTags := make([]uint64, len(sqlFk.Columns))
		for i, col := range sqlFk.Columns {
			tableCol, ok := t.sch.GetAllCols().GetByNameCaseInsensitive(col)
			if !ok {
				return fmt.Errorf("table `%s` does not have column `%s`", sqlFk.Table, col)
			}
			colTags[i] = tableCol.Tag
		}

		var refTbl *doltdb.Table
		var refSch schema.Schema
		if sqlFk.IsSelfReferential() {
			refTbl = tbl
			refSch = t.sch
		} else {
			refTbl, _, ok, err = root.GetTableInsensitive(ctx, sqlFk.ParentTable)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("referenced table `%s` does not exist", sqlFk.ParentTable)
			}
			refSch, err = refTbl.GetSchema(ctx)
			if err != nil {
				return err
			}
		}

		refColTags := make([]uint64, len(sqlFk.ParentColumns))
		for i, name := range sqlFk.ParentColumns {
			refCol, ok := refSch.GetAllCols().GetByNameCaseInsensitive(name)
			if !ok {
				return fmt.Errorf("table `%s` does not have column `%s`", sqlFk.ParentTable, name)
			}
			refColTags[i] = refCol.Tag
		}

		tableIndex, ok, err := findIndexWithPrefix(t.sch, sqlFk.Columns)
		if err != nil {
			return err
		}
		if !ok {
			// The engine matched on a primary key, and Dolt does not yet support using the primary key within the
			// schema.Index interface (which is used internally to represent indexes across the codebase). In the
			// meantime, we must generate a duplicate key over the primary key.
			//TODO: use the primary key as-is
			idxReturn, err := creation.CreateIndex(ctx, tbl, "", sqlFk.Columns, false, false, "", editor.Options{
				ForeignKeyChecksDisabled: true,
				Deaf:                     t.opts.Deaf,
				Tempdir:                  t.opts.Tempdir,
			})
			if err != nil {
				return err
			}
			tableIndex = idxReturn.NewIndex
			tbl = idxReturn.NewTable
			root, err = root.PutTable(ctx, t.tableName, idxReturn.NewTable)
			if sqlFk.IsSelfReferential() {
				refTbl = idxReturn.NewTable
			}
		}

		refTableIndex, ok, err := findIndexWithPrefix(refSch, sqlFk.ParentColumns)
		if err != nil {
			return err
		}
		if !ok {
			// The engine matched on a primary key, and Dolt does not yet support using the primary key within the
			// schema.Index interface (which is used internally to represent indexes across the codebase). In the
			// meantime, we must generate a duplicate key over the primary key.
			//TODO: use the primary key as-is
			var refPkTags []uint64
			for _, i := range refSch.GetPkOrdinals() {
				refPkTags = append(refPkTags, refSch.GetAllCols().GetAtIndex(i).Tag)
			}

			var colNames []string
			for _, t := range refColTags {
				c, _ := refSch.GetAllCols().GetByTag(t)
				colNames = append(colNames, c.Name)
			}

			// Our duplicate index is only unique if it's the entire primary key (which is by definition unique)
			unique := len(refPkTags) == len(refColTags)
			idxReturn, err := creation.CreateIndex(ctx, refTbl, "", colNames, unique, false, "", editor.Options{
				ForeignKeyChecksDisabled: true,
				Deaf:                     t.opts.Deaf,
				Tempdir:                  t.opts.Tempdir,
			})
			if err != nil {
				return err
			}
			refTbl = idxReturn.NewTable
			refTableIndex = idxReturn.NewIndex
			root, err = root.PutTable(ctx, sqlFk.ParentTable, idxReturn.NewTable)
			if err != nil {
				return err
			}
		}

		doltFk.TableIndex = tableIndex.Name()
		doltFk.TableColumns = colTags
		doltFk.ReferencedTableIndex = refTableIndex.Name()
		doltFk.ReferencedTableColumns = refColTags
	}

	err = fkc.AddKeys(doltFk)
	if err != nil {
		return err
	}
	newRoot, err := root.PutForeignKeyCollection(ctx, fkc)
	if err != nil {
		return err
	}
	err = t.setRoot(ctx, newRoot)
	if err != nil {
		return err
	}
	return t.updateFromRoot(ctx, newRoot)
}

// CreateIndexForForeignKey implements sql.ForeignKeyTable
func (t *AlterableDoltTable) CreateIndexForForeignKey(ctx *sql.Context, indexName string, using sql.IndexUsing, constraint sql.IndexConstraint, indexColumns []sql.IndexColumn) error {
	if constraint != sql.IndexConstraint_None && constraint != sql.IndexConstraint_Unique {
		return fmt.Errorf("only the following types of index constraints are supported: none, unique")
	}
	columns := make([]string, len(indexColumns))
	for i, indexCol := range indexColumns {
		columns[i] = indexCol.Name
	}

	table, err := t.doltTable(ctx)
	if err != nil {
		return err
	}

	ret, err := creation.CreateIndex(
		ctx,
		table,
		indexName,
		columns,
		constraint == sql.IndexConstraint_Unique,
		false,
		"",
		t.opts,
	)
	if err != nil {
		return err
	}
	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}
	newRoot, err := root.PutTable(ctx, t.tableName, ret.NewTable)
	if err != nil {
		return err
	}

	err = t.setRoot(ctx, newRoot)

	if err != nil {
		return err
	}
	return t.updateFromRoot(ctx, newRoot)
}

// GetForeignKeyUpdater implements sql.ForeignKeyTable
func (t *AlterableDoltTable) GetForeignKeyUpdater(ctx *sql.Context) sql.ForeignKeyUpdater {
	te, err := t.getTableEditor(ctx)
	if err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	return te
}

// toForeignKeyConstraint converts a Dolt resolved foreign key to a GMS foreign key. If the key is unresolved, then this
// function should not be used.
func toForeignKeyConstraint(fk doltdb.ForeignKey, dbName string, childSch, parentSch schema.Schema) (cst sql.ForeignKeyConstraint, err error) {
	cst = sql.ForeignKeyConstraint{
		Name:           fk.Name,
		Database:       dbName,
		Table:          fk.TableName,
		Columns:        make([]string, len(fk.TableColumns)),
		ParentDatabase: dbName,
		ParentTable:    fk.ReferencedTableName,
		ParentColumns:  make([]string, len(fk.ReferencedTableColumns)),
		OnUpdate:       toReferentialAction(fk.OnUpdate),
		OnDelete:       toReferentialAction(fk.OnDelete),
		IsResolved:     fk.IsResolved(),
	}

	for i, tag := range fk.TableColumns {
		c, ok := childSch.GetAllCols().GetByTag(tag)
		if !ok {
			return cst, fmt.Errorf("cannot find column for tag %d "+
				"in table %s used in foreign key %s", tag, fk.TableName, fk.Name)
		}
		cst.Columns[i] = c.Name
	}

	for i, tag := range fk.ReferencedTableColumns {
		c, ok := parentSch.GetAllCols().GetByTag(tag)
		if !ok {
			return cst, fmt.Errorf("cannot find column for tag %d "+
				"in table %s used in foreign key %s", tag, fk.ReferencedTableName, fk.Name)
		}
		cst.ParentColumns[i] = c.Name

	}

	return cst, nil
}

func toReferentialAction(opt doltdb.ForeignKeyReferentialAction) sql.ForeignKeyReferentialAction {
	switch opt {
	case doltdb.ForeignKeyReferentialAction_DefaultAction:
		return sql.ForeignKeyReferentialAction_DefaultAction
	case doltdb.ForeignKeyReferentialAction_Cascade:
		return sql.ForeignKeyReferentialAction_Cascade
	case doltdb.ForeignKeyReferentialAction_NoAction:
		return sql.ForeignKeyReferentialAction_NoAction
	case doltdb.ForeignKeyReferentialAction_Restrict:
		return sql.ForeignKeyReferentialAction_Restrict
	case doltdb.ForeignKeyReferentialAction_SetNull:
		return sql.ForeignKeyReferentialAction_SetNull
	default:
		panic(fmt.Sprintf("Unhandled foreign key referential action %v", opt))
	}
}

func parseFkReferentialAction(refOp sql.ForeignKeyReferentialAction) (doltdb.ForeignKeyReferentialAction, error) {
	switch refOp {
	case sql.ForeignKeyReferentialAction_DefaultAction:
		return doltdb.ForeignKeyReferentialAction_DefaultAction, nil
	case sql.ForeignKeyReferentialAction_Restrict:
		return doltdb.ForeignKeyReferentialAction_Restrict, nil
	case sql.ForeignKeyReferentialAction_Cascade:
		return doltdb.ForeignKeyReferentialAction_Cascade, nil
	case sql.ForeignKeyReferentialAction_NoAction:
		return doltdb.ForeignKeyReferentialAction_NoAction, nil
	case sql.ForeignKeyReferentialAction_SetNull:
		return doltdb.ForeignKeyReferentialAction_SetNull, nil
	case sql.ForeignKeyReferentialAction_SetDefault:
		return doltdb.ForeignKeyReferentialAction_DefaultAction, sql.ErrForeignKeySetDefault.New()
	default:
		return doltdb.ForeignKeyReferentialAction_DefaultAction, fmt.Errorf("unknown foreign key referential action: %v", refOp)
	}
}

// dropIndex drops the given index on the given table with the given schema. Returns the updated table and updated schema.
func (t *AlterableDoltTable) dropIndex(ctx *sql.Context, indexName string) (*doltdb.Table, schema.Schema, error) {
	// RemoveIndex returns an error if the index does not exist, no need to do twice
	_, err := t.sch.Indexes().RemoveIndex(indexName)
	if err != nil {
		return nil, nil, err
	}

	table, err := t.doltTable(ctx)
	if err != nil {
		return nil, nil, err
	}

	newTable, err := table.UpdateSchema(ctx, t.sch)
	if err != nil {
		return nil, nil, err
	}
	newTable, err = newTable.DeleteIndexRowData(ctx, indexName)
	if err != nil {
		return nil, nil, err
	}
	tblSch, err := newTable.GetSchema(ctx)
	if err != nil {
		return nil, nil, err
	}
	return newTable, tblSch, nil
}

func (t *AlterableDoltTable) updateFromRoot(ctx *sql.Context, root *doltdb.RootValue) error {
	updatedTableSql, ok, err := t.db.getTable(ctx, root, t.tableName)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("table `%s` cannot find itself", t.tableName)
	}
	var updatedTable *AlterableDoltTable
	if doltdb.HasDoltPrefix(t.tableName) && !doltdb.IsReadOnlySystemTable(t.tableName) {
		updatedTable = &AlterableDoltTable{*updatedTableSql.(*WritableDoltTable)}
	} else {
		updatedTable = updatedTableSql.(*AlterableDoltTable)
	}
	t.WritableDoltTable.DoltTable = updatedTable.WritableDoltTable.DoltTable
	return nil
}

func (t *AlterableDoltTable) CreateCheck(ctx *sql.Context, check *sql.CheckDefinition) error {
	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}

	updatedTable, _, err := root.GetTable(ctx, t.tableName)
	if err != nil {
		return err
	}

	sch, err := updatedTable.GetSchema(ctx)
	if err != nil {
		return err
	}

	check = &(*check)
	if check.Name == "" {
		var err error
		check.Name, err = t.generateCheckName(ctx, check)
		if err != nil {
			return err
		}
	}

	_, err = sch.Checks().AddCheck(check.Name, check.CheckExpression, check.Enforced)
	if err != nil {
		return err
	}

	table, err := t.doltTable(ctx)
	if err != nil {
		return err
	}

	newTable, err := table.UpdateSchema(ctx, sch)
	if err != nil {
		return err
	}

	newRoot, err := root.PutTable(ctx, t.tableName, newTable)
	if err != nil {
		return err
	}

	err = t.setRoot(ctx, newRoot)
	if err != nil {
		return err
	}

	return t.updateFromRoot(ctx, newRoot)
}

func (t *AlterableDoltTable) DropCheck(ctx *sql.Context, chName string) error {
	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}

	updatedTable, _, err := root.GetTable(ctx, t.tableName)
	if err != nil {
		return err
	}

	sch, err := updatedTable.GetSchema(ctx)
	if err != nil {
		return err
	}

	err = sch.Checks().DropCheck(chName)
	if err != nil {
		return err
	}

	table, err := t.doltTable(ctx)
	if err != nil {
		return err
	}

	newTable, err := table.UpdateSchema(ctx, sch)
	if err != nil {
		return err
	}

	newRoot, err := root.PutTable(ctx, t.tableName, newTable)
	if err != nil {
		return err
	}

	err = t.setRoot(ctx, newRoot)
	if err != nil {
		return err
	}

	return t.updateFromRoot(ctx, newRoot)
}

func (t *AlterableDoltTable) generateCheckName(ctx *sql.Context, check *sql.CheckDefinition) (string, error) {
	var bb bytes.Buffer
	bb.Write([]byte(check.CheckExpression))
	hash := hash.Of(bb.Bytes())

	hashedName := fmt.Sprintf("chk_%s", hash.String()[:8])
	name := hashedName

	var i int
	for {
		exists, err := t.constraintNameExists(ctx, name)
		if err != nil {
			return "", err
		}
		if !exists {
			break
		}

		name = fmt.Sprintf("%s_%d", hashedName, i)
		i++
	}

	return name, nil
}

func (t *AlterableDoltTable) constraintNameExists(ctx *sql.Context, name string) (bool, error) {
	keys, err := t.GetDeclaredForeignKeys(ctx)
	if err != nil {
		return false, err
	}

	for _, key := range keys {
		if strings.ToLower(key.Name) == strings.ToLower(name) {
			return true, nil
		}
	}

	checks, err := t.GetChecks(ctx)
	if err != nil {
		return false, err
	}

	for _, check := range checks {
		if strings.ToLower(check.Name) == strings.ToLower(name) {
			return true, nil
		}
	}

	return false, nil
}

func (t *AlterableDoltTable) CreatePrimaryKey(ctx *sql.Context, columns []sql.IndexColumn) error {
	if types.IsFormat_DOLT_1(t.nbf) {
		return nil
	}

	table, err := t.doltTable(ctx)
	if err != nil {
		return err
	}

	table, err = addPrimaryKeyToTable(ctx, table, t.tableName, t.nbf, columns, t.opts)
	if err != nil {
		return err
	}

	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}

	// Update the root with the new table
	newRoot, err := root.PutTable(ctx, t.tableName, table)
	if err != nil {
		return err
	}

	err = t.setRoot(ctx, newRoot)
	if err != nil {
		return err
	}

	return t.updateFromRoot(ctx, newRoot)
}

func (t *AlterableDoltTable) DropPrimaryKey(ctx *sql.Context) error {
	if types.IsFormat_DOLT_1(t.nbf) {
		return nil
	}

	// Ensure that no auto increment requirements exist on this table
	if t.autoIncCol.AutoIncrement {
		return sql.ErrWrongAutoKey.New()
	}

	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}

	fkcUpdates, err := t.backupFkcIndexesForPkDrop(ctx, root)
	if err != nil {
		return err
	}

	newRoot, err := t.updateFkcIndex(ctx, root, fkcUpdates)
	if err != nil {
		return err
	}

	table, err := t.doltTable(ctx)
	if err != nil {
		return err
	}

	table, err = dropPrimaryKeyFromTable(ctx, table, t.nbf, t.opts)
	if err != nil {
		return err
	}

	// Update the root with the new table
	newRoot, err = newRoot.PutTable(ctx, t.tableName, table)
	if err != nil {
		return err
	}

	err = t.setRoot(ctx, newRoot)
	if err != nil {
		return err
	}

	return t.updateFromRoot(ctx, newRoot)
}

type fkIndexUpdate struct {
	fkName  string
	fromIdx string
	toIdx   string
}

// updateFkcIndex applies a list of fkIndexUpdates to a ForeignKeyCollection and returns a new root value
func (t *AlterableDoltTable) updateFkcIndex(ctx *sql.Context, root *doltdb.RootValue, updates []fkIndexUpdate) (*doltdb.RootValue, error) {
	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	for _, u := range updates {
		fk, ok := fkc.GetByNameCaseInsensitive(u.fkName)
		if !ok {
			return nil, errors.New("foreign key not found")
		}
		fkc.RemoveKeys(fk)
		fk.ReferencedTableIndex = u.toIdx
		fkc.AddKeys(fk)
		err := fk.ValidateReferencedTableSchema(t.sch)
		if err != nil {
			return nil, err
		}
	}

	root, err = root.PutForeignKeyCollection(ctx, fkc)
	if err != nil {
		return nil, err
	}
	return root, nil
}

// backupFkcIndexesForKeyDrop finds backup indexes to cover foreign key references during a primary
// key drop. If multiple indexes are valid, we sort by unique and select the first.
// This will not work with a non-pk index drop without an additional index filter argument.
func (t *AlterableDoltTable) backupFkcIndexesForPkDrop(ctx *sql.Context, root *doltdb.RootValue) ([]fkIndexUpdate, error) {
	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	indexes := t.sch.Indexes().AllIndexes()
	if err != nil {
		return nil, err
	}

	// pkBackups is a mapping from the table's PK tags to potentially compensating indexes
	pkBackups := make(map[uint64][]schema.Index, len(t.sch.GetPKCols().TagToIdx))
	for tag, _ := range t.sch.GetPKCols().TagToIdx {
		pkBackups[tag] = nil
	}

	// prefer unique key backups
	sort.Slice(indexes[:], func(i, j int) bool {
		return indexes[i].IsUnique() && !indexes[j].IsUnique()
	})

	for _, idx := range indexes {
		if !idx.IsUserDefined() {
			continue
		}

		for _, tag := range idx.AllTags() {
			if _, ok := pkBackups[tag]; ok {
				pkBackups[tag] = append(pkBackups[tag], idx)
			}
		}
	}

	fkUpdates := make([]fkIndexUpdate, 0)
	for _, fk := range fkc.AllKeys() {
		// check if this FK references a parent PK tag we are trying to change
		if backups, ok := pkBackups[fk.ReferencedTableColumns[0]]; ok {
			covered := false
			for _, idx := range backups {
				idxTags := idx.AllTags()
				if len(fk.TableColumns) > len(idxTags) {
					continue
				}
				failed := false
				for i := 0; i < len(fk.ReferencedTableColumns); i++ {
					if idxTags[i] != fk.ReferencedTableColumns[i] {
						failed = true
						break
					}
				}
				if failed {
					continue
				}
				fkUpdates = append(fkUpdates, fkIndexUpdate{fk.Name, fk.ReferencedTableIndex, idx.Name()})
				covered = true
				break
			}
			if !covered {
				return nil, sql.ErrCantDropIndex.New("PRIMARY")
			}
		}
	}
	return fkUpdates, nil
}

func findIndexWithPrefix(sch schema.Schema, prefixCols []string) (schema.Index, bool, error) {
	type idxWithLen struct {
		schema.Index
		colLen int
	}

	prefixCols = lowercaseSlice(prefixCols)
	indexes := sch.Indexes().AllIndexes()
	colLen := len(prefixCols)
	var indexesWithLen []idxWithLen
	for _, idx := range indexes {
		idxCols := lowercaseSlice(idx.ColumnNames())
		if ok, prefixCount := colsAreIndexSubset(prefixCols, idxCols); ok && prefixCount == colLen {
			indexesWithLen = append(indexesWithLen, idxWithLen{idx, len(idxCols)})
		}
	}
	if len(indexesWithLen) == 0 {
		return nil, false, nil
	}

	sort.Slice(indexesWithLen, func(i, j int) bool {
		idxI := indexesWithLen[i]
		idxJ := indexesWithLen[j]
		if idxI.colLen == colLen && idxJ.colLen != colLen {
			return true
		} else if idxI.colLen != colLen && idxJ.colLen == colLen {
			return false
		} else if idxI.colLen != idxJ.colLen {
			return idxI.colLen > idxJ.colLen
		} else {
			return idxI.Index.Name() < idxJ.Index.Name()
		}
	})
	sortedIndexes := make([]schema.Index, len(indexesWithLen))
	for i := 0; i < len(sortedIndexes); i++ {
		sortedIndexes[i] = indexesWithLen[i].Index
	}
	return sortedIndexes[0], true, nil
}

func colsAreIndexSubset(cols, indexCols []string) (ok bool, prefixCount int) {
	if len(cols) > len(indexCols) {
		return false, 0
	}

	visitedIndexCols := make([]bool, len(indexCols))
	for _, expr := range cols {
		found := false
		for j, indexExpr := range indexCols {
			if visitedIndexCols[j] {
				continue
			}
			if expr == indexExpr {
				visitedIndexCols[j] = true
				found = true
				break
			}
		}
		if !found {
			return false, 0
		}
	}

	// This checks the length of the prefix by checking how many true booleans are encountered before the first false
	for i, visitedCol := range visitedIndexCols {
		if visitedCol {
			continue
		}
		return true, i
	}

	return true, len(cols)
}

func lowercaseSlice(strs []string) []string {
	newStrs := make([]string, len(strs))
	for i, str := range strs {
		newStrs[i] = strings.ToLower(str)
	}
	return newStrs
}
