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
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/alterschema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
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
	temporary     bool

	opts editor.Options
}

func NewDoltTable(name string, sch schema.Schema, tbl *doltdb.Table, db SqlDatabase, isTemporary bool, opts editor.Options) (*DoltTable, error) {
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
		temporary:     isTemporary,
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
	sql.Table
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
	if t.temporary {
		root, ok := t.db.GetTemporaryTablesRoot(ctx)
		if !ok {
			return nil, fmt.Errorf("error: manipulating temporary table root when it does not exist")
		}

		return root, nil
	}

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

	val, err := table.GetAutoIncrementValue(ctx)
	if err != nil {
		return nil, err
	}
	return t.autoIncCol.TypeInfo.ConvertNomsValueToValue(val)
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

// Returns the partitions for this table.
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
	return t.temporary
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
	sql.UpdatableTable
	sql.DeletableTable
	sql.InsertableTable
	sql.ReplaceableTable
	sql.AutoIncrementTable
	sql.TruncateableTable
}

func (t *WritableDoltTable) setRoot(ctx *sql.Context, newRoot *doltdb.RootValue) error {
	if t.temporary {
		return t.db.SetTemporaryRoot(ctx, newRoot)
	}

	return t.db.SetRoot(ctx, newRoot)
}

func (t *WritableDoltTable) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	return &WritableIndexedDoltTable{
		WritableDoltTable: t,
		indexLookup:       lookup,
	}
}

func (t *WritableDoltTable) WithProjection(colNames []string) sql.Table {
	return &WritableDoltTable{
		DoltTable: t.DoltTable.WithProjection(colNames).(*DoltTable),
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

	ws, err := ds.WorkingSet(ctx, t.db.name)
	if err != nil {
		return nil, err
	}
	ait := t.db.gs.GetAutoIncrementTracker(ws.Ref())

	state, _, err := ds.LookupDbState(ctx, t.db.name)
	if err != nil {
		return nil, err
	}

	if t.temporary {
		setter := ds.SetTempTableRoot
		ed, err = state.TempTableWriteSession.GetTableWriter(ctx, t.tableName, t.db.Name(), ait, setter, batched)
	} else {
		setter := ds.SetRoot
		ed, err = state.WriteSession.GetTableWriter(ctx, t.tableName, t.db.Name(), ait, setter, batched)
	}
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

	rowData, err := table.GetRowData(ctx)
	if err != nil {
		return 0, err
	}
	numOfRows := int(rowData.Count())

	empty, err := durable.NewEmptyIndex(ctx, table.ValueReadWriter(), t.sch)
	if err != nil {
		return 0, err
	}

	// truncate table resets auto-increment value
	newTable, err := doltdb.NewTable(ctx, table.ValueReadWriter(), t.sch, empty, nil, nil)
	if err != nil {
		return 0, err
	}

	newTable, err = editor.RebuildAllIndexes(ctx, newTable, t.opts)
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
func (t *WritableDoltTable) GetNextAutoIncrementValue(ctx *sql.Context, potentialVal interface{}) (interface{}, error) {
	if !t.autoIncCol.AutoIncrement {
		return nil, sql.ErrNoAutoIncrementCol
	}

	ed, err := t.getTableEditor(ctx)
	if err != nil {
		return nil, err
	}

	tableVal, err := t.getTableAutoIncrementValue(ctx)
	if err != nil {
		return nil, err
	}

	return ed.NextAutoIncrementValue(potentialVal, tableVal)
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

// GetForeignKeys implements sql.ForeignKeyTable
func (t *DoltTable) GetForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	root, err := t.getRoot(ctx)
	if err != nil {
		return nil, err
	}

	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	declaredFk, _ := fkc.KeysForTable(t.tableName)
	toReturn := make([]sql.ForeignKeyConstraint, len(declaredFk))

	for i, fk := range declaredFk {
		if !fk.IsResolved() {
			toReturn[i] = sql.ForeignKeyConstraint{
				Name:              fk.Name,
				Columns:           fk.UnresolvedFKDetails.TableColumns,
				ReferencedTable:   fk.ReferencedTableName,
				ReferencedColumns: fk.UnresolvedFKDetails.ReferencedTableColumns,
				OnUpdate:          toReferenceOption(fk.OnUpdate),
				OnDelete:          toReferenceOption(fk.OnDelete),
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
		toReturn[i], err = toForeignKeyConstraint(fk, t.sch, parentSch)
		if err != nil {
			return nil, err
		}
	}

	return toReturn, nil
}

func (t *DoltTable) Projection() []string {
	return t.projectedCols
}

func (t DoltTable) WithProjection(colNames []string) sql.Table {
	t.projectedCols = colNames
	return &t
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

	// value range of partition for "prolly" implementation
	rowRange prolly.Range

	rowData durable.Index
}

func partitionsFromRows(ctx context.Context, rows durable.Index) []doltTablePartition {
	if rows.Empty() {
		return []doltTablePartition{
			{start: 0, end: 0, rowData: rows},
		}
	}

	nbf := rows.Format()
	switch nbf {
	case types.Format_LD_1, types.Format_7_18:
		nm := durable.NomsMapFromIndex(rows)
		return partitionsFromNomsRows(nm, durable.VrwFromNomsIndex(rows))

	case types.Format_DOLT_1:
		return partitionsFromProllyRows(rows)
	}

	return nil
}

func partitionsFromNomsRows(rows types.Map, vrw types.ValueReadWriter) []doltTablePartition {
	numElements := rows.Len()
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
			rowData: durable.IndexFromNomsMap(rows, vrw),
		}
	}

	partitions[numPartitions-1] = doltTablePartition{
		start:   (numPartitions - 1) * itemsPerPartition,
		end:     numElements,
		rowData: durable.IndexFromNomsMap(rows, vrw),
	}

	return partitions
}

func partitionsFromProllyRows(rows durable.Index) []doltTablePartition {
	pm := durable.ProllyMapFromIndex(rows)
	keyDesc, _ := pm.Descriptors()

	// naively divide map by top-level keys
	keys := prolly.PartitionKeysFromMap(pm)

	first := prolly.Range{
		Start:   prolly.RangeCut{Unbound: true},
		Stop:    prolly.RangeCut{Key: keys[0], Inclusive: true},
		KeyDesc: keyDesc,
	}

	parts := make([]doltTablePartition, len(keys))
	parts[0] = doltTablePartition{rowRange: first, rowData: rows}
	for i := range parts {
		if i == 0 {
			continue
		}
		rng := prolly.Range{
			Start:   prolly.RangeCut{Key: keys[i-1], Inclusive: false},
			Stop:    prolly.RangeCut{Key: keys[i], Inclusive: true},
			KeyDesc: keyDesc,
		}
		parts[i] = doltTablePartition{rowRange: rng, rowData: rows}
	}

	return parts
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
	sql.ForeignKeyAlterableTable
	sql.ForeignKeyTable
	sql.CheckAlterableTable
	sql.PrimaryKeyAlterableTable
}

var _ doltAlterableTableInterface = (*AlterableDoltTable)(nil)

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

	nullable := alterschema.NotNull
	if col.IsNullable() {
		nullable = alterschema.Null
	}

	updatedTable, err := alterschema.AddColumnToTable(ctx, root, table, t.tableName, col.Tag, col.Name, col.TypeInfo, nullable, column.Default, col.Comment, orderToOrder(order))
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

func orderToOrder(order *sql.ColumnOrder) *alterschema.ColumnOrder {
	if order == nil {
		return nil
	}
	return &alterschema.ColumnOrder{
		First: order.First,
		After: order.AfterColumn,
	}
}

// DropColumn implements sql.AlterableTable
func (t *AlterableDoltTable) DropColumn(ctx *sql.Context, columnName string) error {
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

	updatedTable, err = updatedTable.UpdateSchema(ctx, sch)
	if err != nil {
		return err
	}

	fkCollection, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	declaresFk, referencesFk := fkCollection.KeysForTable(t.tableName)

	updatedTable, err = alterschema.DropColumn(ctx, updatedTable, columnName, append(declaresFk, referencesFk...))
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

// ModifyColumn implements sql.AlterableTable
func (t *AlterableDoltTable) ModifyColumn(ctx *sql.Context, columnName string, column *sql.Column, order *sql.ColumnOrder) error {
	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}

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

	fkCollection, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	declaresFk, referencedByFk := fkCollection.KeysForTable(t.tableName)
	for _, foreignKey := range declaresFk {
		if (foreignKey.OnUpdate == doltdb.ForeignKeyReferenceOption_SetNull || foreignKey.OnDelete == doltdb.ForeignKeyReferenceOption_SetNull) &&
			col.IsNullable() {
			return fmt.Errorf("foreign key `%s` has SET NULL thus column `%s` cannot be altered to accept null values", foreignKey.Name, col.Name)
		}
	}

	if !existingCol.TypeInfo.Equals(col.TypeInfo) {
		for _, foreignKey := range declaresFk {
			for _, tag := range foreignKey.TableColumns {
				if tag == existingCol.Tag {
					return fmt.Errorf("cannot alter a column's type when it is used in a foreign key")
				}
			}
		}
		for _, foreignKey := range referencedByFk {
			for _, tag := range foreignKey.ReferencedTableColumns {
				if tag == existingCol.Tag {
					return fmt.Errorf("cannot alter a column's type when it is used in a foreign key")
				}
			}
		}
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

	updatedTable, err := alterschema.ModifyColumn(ctx, table, existingCol, col, orderToOrder(order), t.opts)
	if err != nil {
		return err
	}

	// For auto columns modified to be auto increment, we have more work to do
	if !existingCol.AutoIncrement && col.AutoIncrement {
		updatedSch, err := updatedTable.GetSchema(ctx)
		if err != nil {
			return err
		}

		initialValue := column.Type.Zero()

		colIdx := updatedSch.GetAllCols().IndexOf(columnName)

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

		initialValNoms, err := col.TypeInfo.ConvertValueToNomsValue(ctx, root.VRW(), initialValue)
		if err != nil {
			return err
		}

		initialValNoms = increment(initialValNoms)

		updatedTable, err = updatedTable.SetAutoIncrementValue(ctx, initialValNoms)
		if err != nil {
			return err
		}
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
	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	ourKeys, referencingKeys := fkc.KeysForTable(t.tableName)
	for _, k := range ourKeys {
		if k.TableIndex == indexName {
			return fmt.Errorf("cannot drop index: %s is referenced by foreign key %s",
				k.TableIndex, k.Name)
		}
	}
	for _, k := range referencingKeys {
		if k.ReferencedTableIndex == indexName {
			return fmt.Errorf("cannot drop index: %s is referenced by foreign key %s",
				k.ReferencedTableIndex, k.Name)
		}
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

// CreateForeignKey implements sql.ForeignKeyAlterableTable
func (t *AlterableDoltTable) CreateForeignKey(
	ctx *sql.Context,
	fkName string,
	columns []string,
	refTblName string,
	refColumns []string,
	onUpdate, onDelete sql.ForeignKeyReferenceOption,
) error {
	if types.IsFormat_DOLT_1(t.nbf) {
		// todo(andy)
		return nil
	}

	if fkName != "" && !doltdb.IsValidForeignKeyName(fkName) {
		return fmt.Errorf("invalid foreign key name `%s` as it must match the regular expression %s", fkName, doltdb.ForeignKeyNameRegexStr)
	}
	isSelfFk := strings.ToLower(t.tableName) == strings.ToLower(refTblName)
	if isSelfFk {
		if len(columns) > 1 {
			return fmt.Errorf("support for self referential composite foreign keys is not yet implemented")
		}
	}

	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}
	table, err := t.doltTable(ctx)
	if err != nil {
		return err
	}

	onUpdateRefOp, err := parseFkReferenceOption(onUpdate)
	if err != nil {
		return err
	}
	onDeleteRefOp, err := parseFkReferenceOption(onDelete)
	if err != nil {
		return err
	}

	foreignKey := doltdb.ForeignKey{
		Name:                   fkName,
		TableName:              t.tableName,
		TableIndex:             "",
		TableColumns:           nil,
		ReferencedTableName:    refTblName,
		ReferencedTableIndex:   "",
		ReferencedTableColumns: nil,
		OnUpdate:               onUpdateRefOp,
		OnDelete:               onDeleteRefOp,
		UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
			TableColumns:           columns,
			ReferencedTableColumns: refColumns,
		},
	}

	fkChecks, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
	if err != nil {
		return err
	}
	if fkChecks.(int8) == 1 {
		root, foreignKey, err = creation.ResolveForeignKey(ctx, root, table, foreignKey, t.opts)
		if err != nil {
			return err
		}
	} else {
		fkc, err := root.GetForeignKeyCollection(ctx)
		if err != nil {
			return err
		}
		err = fkc.AddKeys(foreignKey)
		if err != nil {
			return err
		}
		root, err = root.PutForeignKeyCollection(ctx, fkc)
		if err != nil {
			return err
		}
	}

	err = t.setRoot(ctx, root)
	if err != nil {
		return err
	}
	return t.updateFromRoot(ctx, root)
}

// DropForeignKey implements sql.ForeignKeyAlterableTable
func (t *AlterableDoltTable) DropForeignKey(ctx *sql.Context, fkName string) error {
	if types.IsFormat_DOLT_1(t.nbf) {
		// todo(andy)
		return nil
	}

	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}
	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	err = fkc.RemoveKeyByName(fkName)
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

func toForeignKeyConstraint(fk doltdb.ForeignKey, childSch, parentSch schema.Schema) (cst sql.ForeignKeyConstraint, err error) {
	cst = sql.ForeignKeyConstraint{
		Name:              fk.Name,
		Columns:           make([]string, len(fk.TableColumns)),
		ReferencedTable:   fk.ReferencedTableName,
		ReferencedColumns: make([]string, len(fk.ReferencedTableColumns)),
		OnUpdate:          toReferenceOption(fk.OnUpdate),
		OnDelete:          toReferenceOption(fk.OnDelete),
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
		cst.ReferencedColumns[i] = c.Name

	}

	return cst, nil
}

func toReferenceOption(opt doltdb.ForeignKeyReferenceOption) sql.ForeignKeyReferenceOption {
	switch opt {
	case doltdb.ForeignKeyReferenceOption_DefaultAction:
		return sql.ForeignKeyReferenceOption_DefaultAction
	case doltdb.ForeignKeyReferenceOption_Cascade:
		return sql.ForeignKeyReferenceOption_Cascade
	case doltdb.ForeignKeyReferenceOption_NoAction:
		return sql.ForeignKeyReferenceOption_NoAction
	case doltdb.ForeignKeyReferenceOption_Restrict:
		return sql.ForeignKeyReferenceOption_Restrict
	case doltdb.ForeignKeyReferenceOption_SetNull:
		return sql.ForeignKeyReferenceOption_SetNull
	default:
		panic(fmt.Sprintf("Unhandled foreign key reference option %v", opt))
	}
}

func parseFkReferenceOption(refOp sql.ForeignKeyReferenceOption) (doltdb.ForeignKeyReferenceOption, error) {
	switch refOp {
	case sql.ForeignKeyReferenceOption_DefaultAction:
		return doltdb.ForeignKeyReferenceOption_DefaultAction, nil
	case sql.ForeignKeyReferenceOption_Restrict:
		return doltdb.ForeignKeyReferenceOption_Restrict, nil
	case sql.ForeignKeyReferenceOption_Cascade:
		return doltdb.ForeignKeyReferenceOption_Cascade, nil
	case sql.ForeignKeyReferenceOption_NoAction:
		return doltdb.ForeignKeyReferenceOption_NoAction, nil
	case sql.ForeignKeyReferenceOption_SetNull:
		return doltdb.ForeignKeyReferenceOption_SetNull, nil
	case sql.ForeignKeyReferenceOption_SetDefault:
		return doltdb.ForeignKeyReferenceOption_DefaultAction, fmt.Errorf(`"SET DEFAULT" is not supported`)
	default:
		return doltdb.ForeignKeyReferenceOption_DefaultAction, fmt.Errorf("unknown foreign key reference option: %v", refOp)
	}
}

type createIndexReturn struct {
	newTable *doltdb.Table
	sch      schema.Schema
	oldIndex schema.Index
	newIndex schema.Index
}

// createIndex creates the given index on the given table with the given schema. Returns the updated table, updated schema, and created index.
func createIndexForTable(
	ctx *sql.Context,
	table *doltdb.Table,
	indexName string,
	using sql.IndexUsing,
	constraint sql.IndexConstraint,
	columns []sql.IndexColumn,
	isUserDefined bool,
	comment string,
	opts editor.Options,
) (*createIndexReturn, error) {
	if constraint != sql.IndexConstraint_None && constraint != sql.IndexConstraint_Unique {
		return nil, fmt.Errorf("not yet supported")
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	// get the real column names as CREATE INDEX columns are case-insensitive
	var realColNames []string
	allTableCols := sch.GetAllCols()
	for _, indexCol := range columns {
		tableCol, ok := allTableCols.GetByNameCaseInsensitive(indexCol.Name)
		if !ok {
			return nil, fmt.Errorf("column `%s` does not exist for the table", indexCol.Name)
		}
		realColNames = append(realColNames, tableCol.Name)
	}

	if indexName == "" {
		indexName = strings.Join(realColNames, "")
		_, ok := sch.Indexes().GetByNameCaseInsensitive(indexName)
		var i int
		for ok {
			i++
			indexName = fmt.Sprintf("%s_%d", strings.Join(realColNames, ""), i)
			_, ok = sch.Indexes().GetByNameCaseInsensitive(indexName)
		}
	}
	if !doltdb.IsValidIndexName(indexName) {
		return nil, fmt.Errorf("invalid index name `%s` as they must match the regular expression %s", indexName, doltdb.IndexNameRegexStr)
	}

	// if an index was already created for the column set but was not generated by the user then we replace it
	replacingIndex := false
	existingIndex, ok := sch.Indexes().GetIndexByColumnNames(realColNames...)
	if ok && !existingIndex.IsUserDefined() {
		replacingIndex = true
		_, err = sch.Indexes().RemoveIndex(existingIndex.Name())
		if err != nil {
			return nil, err
		}
	}

	// create the index metadata, will error if index names are taken or an index with the same columns in the same order exists
	index, err := sch.Indexes().AddIndexByColNames(
		indexName,
		realColNames,
		schema.IndexProperties{
			IsUnique:      constraint == sql.IndexConstraint_Unique,
			IsUserDefined: isUserDefined,
			Comment:       comment,
		},
	)
	if err != nil {
		return nil, err
	}

	// update the table schema with the new index
	newTable, err := table.UpdateSchema(ctx, sch)
	if err != nil {
		return nil, err
	}

	if replacingIndex { // verify that the pre-existing index data is valid
		newTable, err = newTable.RenameIndexRowData(ctx, existingIndex.Name(), index.Name())
		if err != nil {
			return nil, err
		}
		err = newTable.VerifyIndexRowData(ctx, index.Name())
		if err != nil {
			return nil, err
		}
	} else { // set the index row data and get a new root with the updated table
		indexRowData, err := editor.RebuildIndex(ctx, newTable, index.Name(), opts)
		if err != nil {
			return nil, err
		}
		newTable, err = newTable.SetNomsIndexRows(ctx, index.Name(), indexRowData)
		if err != nil {
			return nil, err
		}
	}
	return &createIndexReturn{
		newTable: newTable,
		sch:      sch,
		oldIndex: existingIndex,
		newIndex: index,
	}, nil
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
	updatedTableSql, ok, err := t.db.getTable(ctx, root, t.tableName, t.temporary)
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
	keys, err := t.GetForeignKeys(ctx)
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
	table, err := t.doltTable(ctx)
	if err != nil {
		return err
	}

	table, err = alterschema.AddPrimaryKeyToTable(ctx, table, t.tableName, t.nbf, columns, t.opts)
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

	table, err = alterschema.DropPrimaryKeyFromTable(ctx, table, t.nbf, t.opts)
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
