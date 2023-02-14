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
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	sqltypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
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

	projectedCols   []uint64
	projectedSchema sql.Schema

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
func (t DoltTable) LockedToRoot(ctx *sql.Context, root *doltdb.RootValue) (*DoltTable, error) {
	tbl, ok, err := root.GetTable(ctx, t.tableName)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, doltdb.ErrTableNotFound
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	var autoCol schema.Column
	_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.AutoIncrement {
			autoCol = col
			stop = true
		}
		return
	})

	sqlSch, err := sqlutil.FromDoltSchema(t.tableName, sch)
	if err != nil {
		return nil, err
	}

	dt := &DoltTable{
		tableName:    t.tableName,
		db:           t.db,
		nbf:          tbl.Format(),
		sch:          sch,
		sqlSch:       sqlSch,
		autoIncCol:   autoCol,
		opts:         t.opts,
		lockedToRoot: root,
	}
	return dt.WithProjections(t.Projections()).(*DoltTable), nil
}

// Internal interface for declaring the interfaces that read-only dolt tables are expected to implement
// Add new interfaces supported here, rather than in separate type assertions
type doltReadOnlyTableInterface interface {
	sql.Table2
	sql.TemporaryTable
	sql.IndexAddressableTable
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

// IndexedAccess implements sql.IndexAddressableTable
func (t *DoltTable) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	return NewIndexedDoltTable(t, lookup.Index.(index.DoltIndex))
}

// doltTable returns the underlying doltTable from the current session
func (t *DoltTable) DoltTable(ctx *sql.Context) (*doltdb.Table, error) {
	root, err := t.workingRoot(ctx)
	if err != nil {
		return nil, err
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

// DataCacheKey returns an opaque key that can be compared for equality to see if this
// table's data still matches a previous view of the data that was retrieved
// through DoltTable() or NumRows(), for example.
//
// Returns |false| for |ok| if this table's data is not cacheable.
func (t *DoltTable) DataCacheKey(ctx *sql.Context) (doltdb.DataCacheKey, bool, error) {
	r, err := t.workingRoot(ctx)
	if err != nil {
		return doltdb.DataCacheKey{}, false, err
	}
	key, err := doltdb.NewDataCacheKey(r)
	if err != nil {
		return doltdb.DataCacheKey{}, false, err
	}

	return key, true, nil
}

func (t *DoltTable) workingRoot(ctx *sql.Context) (*doltdb.RootValue, error) {
	root := t.lockedToRoot
	if root == nil {
		return t.getRoot(ctx)
	}
	return root, nil
}

// getRoot returns the appropriate root value for this session. The only controlling factor
// is whether this is a temporary table or not.
func (t *DoltTable) getRoot(ctx *sql.Context) (*doltdb.RootValue, error) {
	return t.db.GetRoot(ctx)
}

// GetIndexes implements sql.IndexedTable
func (t *DoltTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	key, tableIsCacheable, err := t.DataCacheKey(ctx)
	if err != nil {
		return nil, err
	}

	if !tableIsCacheable {
		tbl, err := t.DoltTable(ctx)
		if err != nil {
			return nil, err
		}
		return index.DoltIndexesFromTable(ctx, t.db.Name(), t.tableName, tbl)
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbState, ok, err := sess.LookupDbState(ctx, t.db.Name())
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, fmt.Errorf("couldn't find db state for database %s", t.db.Name())
	}

	indexes, ok := dbState.SessionCache().GetTableIndexesCache(key, t.Name())
	if ok {
		return indexes, nil
	}

	tbl, err := t.DoltTable(ctx)
	if err != nil {
		return nil, err
	}

	indexes, err = index.DoltIndexesFromTable(ctx, t.db.Name(), t.tableName, tbl)
	if err != nil {
		return nil, err
	}

	dbState.SessionCache().CacheTableIndexes(key, t.Name(), indexes)
	return indexes, nil
}

// HasIndex returns whether the given index is present in the table
func (t *DoltTable) HasIndex(ctx *sql.Context, idx sql.Index) (bool, error) {
	tbl, err := t.DoltTable(ctx)
	if err != nil {
		return false, err
	}

	return index.TableHasIndex(ctx, t.db.Name(), t.tableName, tbl, idx)
}

// GetAutoIncrementValue gets the last AUTO_INCREMENT value
func (t *DoltTable) GetAutoIncrementValue(ctx *sql.Context) (uint64, error) {
	table, err := t.DoltTable(ctx)
	if err != nil {
		return 0, err
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
func (t *DoltTable) DebugString() string {
	p := sql.NewTreePrinter()

	children := []string{fmt.Sprintf("name: %s", t.tableName)}

	cols := t.sch.GetAllCols()
	if len(t.projectedCols) > 0 {
		var projections []string
		for _, tag := range t.projectedCols {
			projections = append(projections, fmt.Sprintf("%d", cols.TagToIdx[tag]))
		}
		children = append(children, fmt.Sprintf("projections: %s", projections))

	}

	_ = p.WriteNode("Table")
	p.WriteChildren(children...)
	return p.String()
}

// NumRows returns the unfiltered count of rows contained in the table
func (t *DoltTable) numRows(ctx *sql.Context) (uint64, error) {
	table, err := t.DoltTable(ctx)
	if err != nil {
		return 0, err
	}

	m, err := table.GetRowData(ctx)
	if err != nil {
		return 0, err
	}

	return m.Count()
}

// Format returns the NomsBinFormat for the underlying table
func (t *DoltTable) Format() *types.NomsBinFormat {
	return t.nbf
}

// Schema returns the schema for this table.
func (t *DoltTable) Schema() sql.Schema {
	if t.projectedSchema != nil {
		return t.projectedSchema
	}
	return t.sqlSchema().Schema
}

// Collation returns the collation for this table.
func (t *DoltTable) Collation() sql.CollationID {
	return sql.CollationID(t.sch.GetCollation())
}

func (t *DoltTable) sqlSchema() sql.PrimaryKeySchema {
	// TODO: this should consider projections
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
	table, err := t.DoltTable(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := table.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	partitions, err := partitionsFromRows(ctx, rows)
	if err != nil {
		return nil, err
	}

	return newDoltTablePartitionIter(rows, partitions...), nil
}

func (t *DoltTable) IsTemporary() bool {
	return false
}

// DataLength implements the sql.StatisticsTable interface.
func (t *DoltTable) DataLength(ctx *sql.Context) (uint64, error) {
	schema := t.Schema()
	var numBytesPerRow uint64 = 0
	for _, col := range schema {
		switch n := col.Type.(type) {
		case sql.NumberType:
			numBytesPerRow += 8
		case sql.StringType:
			numBytesPerRow += uint64(n.MaxByteLength())
		case sqltypes.BitType:
			numBytesPerRow += 1
		case sql.DatetimeType:
			numBytesPerRow += 8
		case sql.DecimalType:
			numBytesPerRow += uint64(n.MaximumScale())
		case sql.EnumType:
			numBytesPerRow += 2
		case sqltypes.JsonType:
			numBytesPerRow += 20
		case sql.NullType:
			numBytesPerRow += 1
		case sqltypes.TimeType:
			numBytesPerRow += 16
		case sql.YearType:
			numBytesPerRow += 8
		}
	}

	numRows, err := t.numRows(ctx)
	if err != nil {
		return 0, err
	}

	return numBytesPerRow * numRows, nil
}

// RowCount implements the sql.StatisticsTable interface.
func (t *DoltTable) RowCount(ctx *sql.Context) (uint64, error) {
	return t.numRows(ctx)
}

func (t *DoltTable) PrimaryKeySchema() sql.PrimaryKeySchema {
	return t.sqlSchema()
}

// PartitionRows returns the table rows for the partition given
func (t *DoltTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	table, err := t.DoltTable(ctx)
	if err != nil {
		return nil, err
	}

	return partitionRows(ctx, table, t.sqlSch.Schema, t.projectedCols, partition)
}

func (t DoltTable) PartitionRows2(ctx *sql.Context, part sql.Partition) (sql.RowIter2, error) {
	table, err := t.DoltTable(ctx)
	if err != nil {
		return nil, err
	}

	iter, err := partitionRows(ctx, table, t.sqlSch.Schema, t.projectedCols, part)
	if err != nil {
		return nil, err
	}

	return iter.(sql.RowIter2), err
}

func partitionRows(ctx *sql.Context, t *doltdb.Table, sqlSch sql.Schema, projCols []uint64, partition sql.Partition) (sql.RowIter, error) {
	switch typedPartition := partition.(type) {
	case doltTablePartition:
		return newRowIterator(ctx, t, sqlSch, projCols, typedPartition)
	case index.SinglePartition:
		return newRowIterator(ctx, t, sqlSch, projCols, doltTablePartition{rowData: typedPartition.RowData, end: NoUpperBound})
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

func (t *WritableDoltTable) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	return NewWritableIndexedDoltTable(t, lookup.Index.(index.DoltIndex))
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
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
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
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	te, err := t.getTableEditor(ctx)
	if err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	return te
}

// Replacer implements sql.ReplaceableTable
func (t *WritableDoltTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	te, err := t.getTableEditor(ctx)
	if err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	return te
}

// Truncate implements sql.TruncateableTable
func (t *WritableDoltTable) Truncate(ctx *sql.Context) (int, error) {
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return 0, err
	}
	table, err := t.DoltTable.DoltTable(ctx)
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
	c, err := rowData.Count()
	if err != nil {
		return 0, err
	}
	numOfRows := int(c)

	sess := dsess.DSessFromSess(ctx.Session)
	newTable, err := t.truncate(ctx, table, sch, sess)
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
func (t *WritableDoltTable) truncate(
	ctx *sql.Context,
	table *doltdb.Table,
	sch schema.Schema,
	sess *dsess.DoltSession,
) (*doltdb.Table, error) {
	empty, err := durable.NewEmptyIndex(ctx, table.ValueReadWriter(), table.NodeStore(), sch)
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

	ws, err := sess.WorkingSet(ctx, t.db.name)
	if err != nil {
		return nil, err
	}

	if schema.HasAutoIncrement(sch) {
		ddb, _ := sess.GetDoltDB(ctx, t.db.name)
		err = t.db.removeTableFromAutoIncrementTracker(ctx, t.Name(), ddb, ws.Ref())
		if err != nil {
			return nil, err
		}
	}

	// truncate table resets auto-increment value
	newEmptyTable, err := doltdb.NewTable(ctx, table.ValueReadWriter(), table.NodeStore(), sch, empty, idxSet, nil)
	if err != nil {
		return nil, err
	}

	newEmptyTable, err = copyConstraintViolationsAndConflicts(ctx, table, newEmptyTable)
	if err != nil {
		return nil, err
	}

	return newEmptyTable, nil
}

func copyConstraintViolationsAndConflicts(ctx context.Context, from, to *doltdb.Table) (*doltdb.Table, error) {
	if !types.IsFormat_DOLT(to.Format()) {
		if has, err := from.HasConflicts(ctx); err != nil {
			return nil, err
		} else if has {
			confSch, conf, err := from.GetConflicts(ctx)
			if err != nil {
				return nil, err
			}
			to, err = to.SetConflicts(ctx, confSch, conf)
			if err != nil {
				return nil, err
			}
		}

		viols, err := from.GetConstraintViolations(ctx)
		if err != nil {
			return nil, err
		}
		to, err = to.SetConstraintViolations(ctx, viols)
		if err != nil {
			return nil, err
		}
	} else {
		arts, err := from.GetArtifacts(ctx)
		if err != nil {
			return nil, err
		}
		to, err = to.SetArtifacts(ctx, arts)
		if err != nil {
			return nil, err
		}
	}

	return to, nil
}

// Updater implements sql.UpdatableTable
func (t *WritableDoltTable) Updater(ctx *sql.Context) sql.RowUpdater {
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	te, err := t.getTableEditor(ctx)
	if err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	return te
}

// AutoIncrementSetter implements sql.AutoIncrementTable
func (t *WritableDoltTable) AutoIncrementSetter(ctx *sql.Context) sql.AutoIncrementSetter {
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	te, err := t.getTableEditor(ctx)
	if err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	return te
}

// PeekNextAutoIncrementValue implements sql.AutoIncrementTable
func (t *WritableDoltTable) PeekNextAutoIncrementValue(ctx *sql.Context) (uint64, error) {
	if !t.autoIncCol.AutoIncrement {
		return 0, sql.ErrNoAutoIncrementCol
	}

	return t.DoltTable.GetAutoIncrementValue(ctx)
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

func (t *DoltTable) GetChecks(ctx *sql.Context) ([]sql.CheckDefinition, error) {
	table, err := t.DoltTable(ctx)
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
func (t DoltTable) CreateIndexForForeignKey(ctx *sql.Context, idx sql.IndexDef) error {
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

// GetForeignKeyEditor implements sql.ForeignKeyTable
func (t DoltTable) GetForeignKeyEditor(ctx *sql.Context) sql.ForeignKeyEditor {
	return nil
}

// Projections implements sql.ProjectedTable
func (t *DoltTable) Projections() []string {
	// The semantics of nil v. empty are important for this inteface, they display differently in explain plans
	if t.projectedCols == nil {
		return nil
	}

	names := make([]string, len(t.projectedCols))
	cols := t.sch.GetAllCols()
	for i := range t.projectedCols {
		col := cols.TagToCol[t.projectedCols[i]]
		names[i] = col.Name
	}
	return names
}

func (t *DoltTable) ProjectedTags() []uint64 {
	if len(t.projectedCols) > 0 {
		return t.projectedCols
	}
	return t.sch.GetAllCols().Tags
}

// WithProjections implements sql.ProjectedTable
func (t *DoltTable) WithProjections(colNames []string) sql.Table {
	nt := *t

	if colNames == nil {
		nt.projectedCols = nil
		nt.projectedSchema = nil
		return &nt
	}

	// In the case of the history table, some columns may not exist, so the projected schema may be smaller than the
	// requested column list in that case.
	nt.projectedCols = make([]uint64, 0)
	nt.projectedSchema = make(sql.Schema, 0)
	cols := t.sch.GetAllCols()
	sch := t.Schema()
	for i := range colNames {
		lowerName := strings.ToLower(colNames[i])
		col, ok := cols.LowerNameToCol[lowerName]
		if !ok {
			// The history iter projects a new schema onto an
			// older table. When a requested projection does not
			// exist in the older schema, the table will ignore
			// the field. The history table is responsible for
			// filling the gaps with nil values.
			continue
		}
		nt.projectedCols = append(nt.projectedCols, col.Tag)
		nt.projectedSchema = append(nt.projectedSchema, sch[sch.IndexOfColName(lowerName)])
	}

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

const NoUpperBound = math.MaxUint64

type doltTablePartition struct {
	// half-open index range of partition: [start, end)
	start, end uint64

	rowData durable.Index
}

func partitionsFromRows(ctx context.Context, rows durable.Index) ([]doltTablePartition, error) {
	empty, err := rows.Empty()
	if err != nil {
		return nil, err
	}
	if empty {
		return []doltTablePartition{
			{start: 0, end: 0, rowData: rows},
		}, nil
	}

	return partitionsFromTableRows(rows)
}

func partitionsFromTableRows(rows durable.Index) ([]doltTablePartition, error) {
	numElements, err := rows.Count()
	if err != nil {
		return nil, err
	}
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

	return partitions, nil
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
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return err
	}
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
		ait, err := t.db.gs.GetAutoIncrementTracker(ctx)
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
	oldColumn *sql.Column,
	newColumn *sql.Column,
) bool {
	return t.isIncompatibleTypeChange(oldColumn, newColumn) ||
		orderChanged(oldSchema, newSchema, oldColumn, newColumn) ||
		isColumnDrop(oldSchema, newSchema) ||
		isPrimaryKeyChange(oldSchema, newSchema)
}

func orderChanged(oldSchema, newSchema sql.PrimaryKeySchema, oldColumn, newColumn *sql.Column) bool {
	if oldColumn == nil || newColumn == nil {
		return false
	}

	return oldSchema.Schema.IndexOfColName(oldColumn.Name) != newSchema.Schema.IndexOfColName(newColumn.Name)
}

func (t *AlterableDoltTable) isIncompatibleTypeChange(oldColumn *sql.Column, newColumn *sql.Column) bool {
	if oldColumn == nil || newColumn == nil {
		return false
	}

	existingCol, _ := t.sch.GetAllCols().GetByNameCaseInsensitive(oldColumn.Name)
	newCol, err := sqlutil.ToDoltCol(schema.SystemTableReservedMin, newColumn)
	if err != nil {
		panic(err) // should be impossible, we check compatibility before this point
	}

	if !existingCol.TypeInfo.Equals(newCol.TypeInfo) {
		if types.IsFormat_DOLT(t.Format()) {
			// This is overly broad, we could narrow this down a bit
			return true
		}
		if existingCol.Kind != newCol.Kind {
			return true
		} else if schema.IsColSpatialType(newCol) {
			// TODO: we need to do this because some spatial type changes require a full table check, but not all.
			//  We could narrow this check down.
			return true
		}
	}

	return false
}

func isColumnDrop(oldSchema sql.PrimaryKeySchema, newSchema sql.PrimaryKeySchema) bool {
	return len(oldSchema.Schema) > len(newSchema.Schema)
}

func getDroppedColumn(oldSchema sql.PrimaryKeySchema, newSchema sql.PrimaryKeySchema) *sql.Column {
	for _, col := range oldSchema.Schema {
		if newSchema.IndexOf(col.Name, col.Source) < 0 {
			return col
		}
	}
	return nil
}

func isPrimaryKeyChange(oldSchema sql.PrimaryKeySchema,
	newSchema sql.PrimaryKeySchema) bool {
	return len(newSchema.PkOrdinals) != len(oldSchema.PkOrdinals)
}

func (t *AlterableDoltTable) RewriteInserter(
	ctx *sql.Context,
	oldSchema sql.PrimaryKeySchema,
	newSchema sql.PrimaryKeySchema,
	oldColumn *sql.Column,
	newColumn *sql.Column,
	idxCols []sql.IndexColumn,
) (sql.RowInserter, error) {
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return nil, err
	}
	err := validateSchemaChange(t.Name(), oldSchema, newSchema, oldColumn, newColumn, idxCols)
	if err != nil {
		return nil, err
	}

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

	dt, err := t.DoltTable.DoltTable(ctx)
	if err != nil {
		return nil, err
	}

	oldSch, err := dt.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	newSch, err := t.getNewSch(ctx, oldColumn, newColumn, oldSch, newSchema, ws.WorkingRoot(), headRoot)
	if err != nil {
		return nil, err
	}
	newSch = schema.CopyChecksConstraints(oldSch, newSch)

	if isColumnDrop(oldSchema, newSchema) {
		newSch = schema.CopyIndexes(oldSch, newSch)
		droppedCol := getDroppedColumn(oldSchema, newSchema)
		for _, index := range newSch.Indexes().IndexesWithColumn(droppedCol.Name) {
			_, err = newSch.Indexes().RemoveIndex(index.Name())
			if err != nil {
				return nil, err
			}
		}
	} else if newColumn != nil && oldColumn != nil { // modify column
		// It may be possible to optimize this and not always rewrite every index, but since we're already truncating the
		// table to rewrite it we also truncate all the indexes. Much easier to get right.
		for _, index := range oldSch.Indexes().AllIndexes() {
			var colNames []string
			prefixLengths := index.PrefixLengths()
			for i, colName := range index.ColumnNames() {
				if strings.ToLower(oldColumn.Name) == strings.ToLower(colName) {
					colNames = append(colNames, newColumn.Name)
					if len(prefixLengths) > 0 {
						if !sqltypes.IsText(newColumn.Type) {
							// drop prefix lengths if column is not a string type
							prefixLengths[i] = 0
						} else if uint32(prefixLengths[i]) > newColumn.Type.MaxTextResponseByteLength() {
							// drop prefix length if prefixLength is too long
							prefixLengths[i] = 0
						}
					}
				} else {
					colNames = append(colNames, colName)
				}
			}

			// check if prefixLengths should be dropped entirely
			var nonZeroPrefixLength bool
			for _, prefixLength := range prefixLengths {
				if prefixLength > 0 {
					nonZeroPrefixLength = true
					break
				}
			}
			if !nonZeroPrefixLength {
				prefixLengths = nil
			}

			newSch.Indexes().AddIndexByColNames(
				index.Name(),
				colNames,
				prefixLengths,
				schema.IndexProperties{
					IsUnique:      index.IsUnique(),
					IsUserDefined: index.IsUserDefined(),
					Comment:       index.Comment(),
				})
		}
	} else {
		newSch = schema.CopyIndexes(oldSch, newSch)
	}

	// If we have an auto increment column, we need to set it here before we begin the rewrite process (it may have changed)
	if schema.HasAutoIncrement(newSch) {
		newSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			if col.AutoIncrement {
				t.autoIncCol = col
				return true, nil
			}
			return false, nil
		})
	}

	// TODO: test for this when the table is auto increment and exists on another branch
	dt, err = t.truncate(ctx, dt, newSch, sess)
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

	if len(oldSchema.PkOrdinals) > 0 && len(newSchema.PkOrdinals) == 0 {
		newRoot, err = t.adjustForeignKeysForDroppedPk(ctx, newRoot)
		if err != nil {
			return nil, err
		}
	}

	newWs := ws.WithWorkingRoot(newRoot)

	// TODO: figure out locking. Other DBs automatically lock a table during this kind of operation, we should probably
	//  do the same. We're messing with global auto-increment values here and it's not safe.
	ait, err := t.db.gs.GetAutoIncrementTracker(ctx)
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

func (t *AlterableDoltTable) getNewSch(ctx context.Context, oldColumn, newColumn *sql.Column, oldSch schema.Schema, newSchema sql.PrimaryKeySchema, root, headRoot *doltdb.RootValue) (schema.Schema, error) {
	if oldColumn == nil || newColumn == nil {
		// Adding or dropping a column
		newSch, err := sqlutil.ToDoltSchema(ctx, root, t.Name(), newSchema, headRoot, sql.CollationID(oldSch.GetCollation()))
		if err != nil {
			return nil, err
		}
		return newSch, err
	}

	oldTi, err := typeinfo.FromSqlType(oldColumn.Type)
	if err != nil {
		return nil, err
	}
	newTi, err := typeinfo.FromSqlType(newColumn.Type)
	if err != nil {
		return nil, err
	}

	if oldTi.NomsKind() != newTi.NomsKind() {
		oldCol, ok := oldSch.GetAllCols().GetByName(oldColumn.Name)
		if !ok {
			return nil, fmt.Errorf("expected column %s to exist in the old schema but did not find it", oldColumn.Name)
		}
		// Remove the old column from |root| so that its kind will not seed the
		// new tag.
		root, err = filterColumnFromRoot(ctx, root, oldCol.Tag)
		if err != nil {
			return nil, err
		}
	}

	newSch, err := sqlutil.ToDoltSchema(ctx, root, t.Name(), newSchema, headRoot, sql.CollationID(oldSch.GetCollation()))
	if err != nil {
		return nil, err
	}

	return newSch, nil
}

// filterColumnFromRoot removes any columns matching |colTag| from a |root|. Returns the updated root.
func filterColumnFromRoot(ctx context.Context, root *doltdb.RootValue, colTag uint64) (*doltdb.RootValue, error) {
	newRoot := root
	err := root.IterTables(ctx, func(name string, table *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		_, ok := sch.GetAllCols().GetByTag(colTag)
		if !ok {
			return false, nil
		}

		newSch, err := filterColumnFromSch(sch, colTag)
		if err != nil {
			return true, err
		}
		t, err := table.UpdateSchema(ctx, newSch)
		if err != nil {
			return true, err
		}
		newRoot, err = newRoot.PutTable(ctx, name, t)
		if err != nil {
			return true, err
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return newRoot, nil
}

func filterColumnFromSch(sch schema.Schema, colTag uint64) (schema.Schema, error) {
	var cols []schema.Column
	_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if tag == colTag {
			return false, nil
		}
		cols = append(cols, col)
		return false, nil
	})
	colCol := schema.NewColCollection(cols...)
	newSch, err := schema.SchemaFromCols(colCol)
	if err != nil {
		return nil, err
	}
	return newSch, nil
}

// validateSchemaChange returns an error if the schema change given is not legal
func validateSchemaChange(
	tableName string,
	oldSchema sql.PrimaryKeySchema,
	newSchema sql.PrimaryKeySchema,
	oldColumn *sql.Column,
	newColumn *sql.Column,
	idxCols []sql.IndexColumn,
) error {
	for _, idxCol := range idxCols {
		col := newSchema.Schema[newSchema.Schema.IndexOfColName(idxCol.Name)]
		if col.PrimaryKey && idxCol.Length > 0 && sqltypes.IsText(col.Type) {
			return sql.ErrUnsupportedIndexPrefix.New(col.Name)
		}
	}

	if newColumn != nil {
		newCol, err := sqlutil.ToDoltCol(schema.SystemTableReservedMin, newColumn)
		if err != nil {
			panic(err)
		}

		if newCol.IsPartOfPK && schema.IsColSpatialType(newCol) {
			return schema.ErrUsingSpatialKey.New(tableName)
		}
	}
	return nil
}

func (t *AlterableDoltTable) adjustForeignKeysForDroppedPk(ctx *sql.Context, root *doltdb.RootValue) (*doltdb.RootValue, error) {
	if t.autoIncCol.AutoIncrement {
		return nil, sql.ErrWrongAutoKey.New()
	}

	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	fkcUpdates, err := backupFkcIndexesForPkDrop(ctx, t.sch, fkc)
	if err != nil {
		return nil, err
	}

	err = fkc.UpdateIndexes(ctx, t.sch, fkcUpdates)
	if err != nil {
		return nil, err
	}

	root, err = root.PutForeignKeyCollection(ctx, fkc)
	if err != nil {
		return nil, err
	}

	return root, nil
}

// DropColumn implements sql.AlterableTable
func (t *AlterableDoltTable) DropColumn(*sql.Context, string) error {
	return fmt.Errorf("not implemented: AlterableDoltTable.DropColumn()")
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

// ModifyColumn implements sql.AlterableTable. ModifyColumn operations are only used for operations that change only
// the schema of a table, not the data. For those operations, |RewriteInserter| is used.
func (t *AlterableDoltTable) ModifyColumn(ctx *sql.Context, columnName string, column *sql.Column, order *sql.ColumnOrder) error {
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return err
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

	// TODO: move this logic into ShouldRewrite
	if !existingCol.TypeInfo.Equals(col.TypeInfo) {
		if existingCol.Kind != col.Kind {
			panic("table cannot be modified in place")
		}
	}

	updatedTable, err := modifyColumn(ctx, table, existingCol, col, order)
	if err != nil {
		return err
	}

	// For auto columns modified to be auto increment, we have more work to do
	if !existingCol.AutoIncrement && col.AutoIncrement {
		seq, err := t.getFirstAutoIncrementValue(ctx, columnName, column.Type, updatedTable)
		if err != nil {
			return err
		}

		updatedTable, err = updatedTable.SetAutoIncrementValue(ctx, seq)
		if err != nil {
			return err
		}

		ait, err := t.db.gs.GetAutoIncrementTracker(ctx)
		if err != nil {
			return err
		}

		// TODO: this isn't transactional, and it should be
		ait.AddNewTable(t.tableName)
		ait.Set(t.tableName, seq)
	}

	// If we're removing an auto inc property, we just need to update global auto increment tracking
	if existingCol.AutoIncrement && !col.AutoIncrement {
		// TODO: this isn't transactional, and it should be
		sess := dsess.DSessFromSess(ctx.Session)
		ddb, _ := sess.GetDoltDB(ctx, t.db.name)
		err = t.db.removeTableFromAutoIncrementTracker(ctx, t.Name(), ddb, ws.Ref())
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

	return t.updateFromRoot(ctx, newRoot)
}

// getFirstAutoIncrementValue returns the next auto increment value for a table that just acquired one through an
// ALTER statement.
// TODO: this could use an index and avoid a full table scan in many cases
func (t *AlterableDoltTable) getFirstAutoIncrementValue(
	ctx *sql.Context,
	columnName string,
	columnType sql.Type,
	table *doltdb.Table,
) (uint64, error) {
	updatedSch, err := table.GetSchema(ctx)
	if err != nil {
		return 0, err
	}

	rowData, err := table.GetRowData(ctx)
	if err != nil {
		return 0, err
	}

	// Note that we aren't calling the public PartitionRows, because it always gets the table data from the session
	// root, which hasn't been updated yet
	rowIter, err := partitionRows(ctx, table, t.sqlSch.Schema, t.projectedCols, index.SinglePartition{RowData: rowData})
	if err != nil {
		return 0, err
	}

	initialValue := columnType.Zero()
	colIdx := updatedSch.GetAllCols().IndexOf(columnName)

	for {
		r, err := rowIter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return 0, err
		}

		cmp, err := columnType.Compare(initialValue, r[colIdx])
		if err != nil {
			return 0, err
		}
		if cmp < 0 {
			initialValue = r[colIdx]
		}
	}

	seq, err := globalstate.CoerceAutoIncrementValue(initialValue)
	if err != nil {
		return 0, err
	}
	seq++

	return seq, nil
}

// hasNonZeroPrefixLength will return true if at least one of the sql.IndexColumns has a Length > 0
func hasNonZeroPrefixLength(idxCols []sql.IndexColumn) bool {
	for _, idxCol := range idxCols {
		if idxCol.Length > 0 {
			return true
		}
	}
	return false
}

// allocatePrefixLengths will return a []uint16 populated with the Length field from sql.IndexColumn
// if all the lengths have a value of 0, it will return nil
func allocatePrefixLengths(idxCols []sql.IndexColumn) []uint16 {
	if !hasNonZeroPrefixLength(idxCols) {
		return nil
	}
	prefixLengths := make([]uint16, len(idxCols))
	for i, idxCol := range idxCols {
		prefixLengths[i] = uint16(idxCol.Length)
	}
	return prefixLengths
}

// CreateIndex implements sql.IndexAlterableTable
func (t *AlterableDoltTable) CreateIndex(ctx *sql.Context, idx sql.IndexDef) error {
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return err
	}
	if idx.Constraint != sql.IndexConstraint_None && idx.Constraint != sql.IndexConstraint_Unique {
		return fmt.Errorf("only the following types of index constraints are supported: none, unique")
	}

	columns := make([]string, len(idx.Columns))
	for i, indexCol := range idx.Columns {
		columns[i] = indexCol.Name
	}

	table, err := t.DoltTable.DoltTable(ctx)
	if err != nil {
		return err
	}

	ret, err := creation.CreateIndex(
		ctx,
		table,
		idx.Name,
		columns,
		allocatePrefixLengths(idx.Columns),
		idx.Constraint == sql.IndexConstraint_Unique,
		true,
		idx.Comment,
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
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return err
	}
	// We disallow removing internal dolt_ tables from SQL directly
	if strings.HasPrefix(indexName, "dolt_") {
		return fmt.Errorf("dolt internal indexes may not be dropped")
	}
	newTable, _, err := t.dropIndex(ctx, indexName)
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

// RenameIndex implements sql.IndexAlterableTable
func (t *AlterableDoltTable) RenameIndex(ctx *sql.Context, fromIndexName string, toIndexName string) error {
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return err
	}
	// RenameIndex will error if there is a name collision or an index does not exist
	_, err := t.sch.Indexes().RenameIndex(fromIndexName, toIndexName)
	if err != nil {
		return err
	}

	table, err := t.DoltTable.DoltTable(ctx)
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

// createForeignKey creates a doltdb.ForeignKey from a sql.ForeignKeyConstraint
func (t *AlterableDoltTable) createForeignKey(
	ctx *sql.Context,
	root *doltdb.RootValue,
	tbl *doltdb.Table,
	sqlFk sql.ForeignKeyConstraint,
	onUpdateRefAction, onDeleteRefAction doltdb.ForeignKeyReferentialAction) (doltdb.ForeignKey, error) {
	if !sqlFk.IsResolved {
		return doltdb.ForeignKey{
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
		}, nil
	}
	colTags := make([]uint64, len(sqlFk.Columns))
	for i, col := range sqlFk.Columns {
		tableCol, ok := t.sch.GetAllCols().GetByNameCaseInsensitive(col)
		if !ok {
			return doltdb.ForeignKey{}, fmt.Errorf("table `%s` does not have column `%s`", sqlFk.Table, col)
		}
		colTags[i] = tableCol.Tag
	}

	var refTbl *doltdb.Table
	var refSch schema.Schema
	if sqlFk.IsSelfReferential() {
		refTbl = tbl
		refSch = t.sch
	} else {
		var ok bool
		var err error
		refTbl, _, ok, err = root.GetTableInsensitive(ctx, sqlFk.ParentTable)
		if err != nil {
			return doltdb.ForeignKey{}, err
		}
		if !ok {
			return doltdb.ForeignKey{}, fmt.Errorf("referenced table `%s` does not exist", sqlFk.ParentTable)
		}
		refSch, err = refTbl.GetSchema(ctx)
		if err != nil {
			return doltdb.ForeignKey{}, err
		}
	}

	refColTags := make([]uint64, len(sqlFk.ParentColumns))
	for i, name := range sqlFk.ParentColumns {
		refCol, ok := refSch.GetAllCols().GetByNameCaseInsensitive(name)
		if !ok {
			return doltdb.ForeignKey{}, fmt.Errorf("table `%s` does not have column `%s`", sqlFk.ParentTable, name)
		}
		refColTags[i] = refCol.Tag
	}

	var tableIndexName, refTableIndexName string
	tableIndex, ok, err := findIndexWithPrefix(t.sch, sqlFk.Columns)
	if err != nil {
		return doltdb.ForeignKey{}, err
	}
	// Use secondary index if found; otherwise it will use empty string, indicating primary key
	if ok {
		tableIndexName = tableIndex.Name()
	}
	refTableIndex, ok, err := findIndexWithPrefix(refSch, sqlFk.ParentColumns)
	if err != nil {
		return doltdb.ForeignKey{}, err
	}
	// Use secondary index if found; otherwise it will use  empty string, indicating primary key
	if ok {
		refTableIndexName = refTableIndex.Name()
	}
	return doltdb.ForeignKey{
		Name:                   sqlFk.Name,
		TableName:              sqlFk.Table,
		TableIndex:             tableIndexName,
		TableColumns:           colTags,
		ReferencedTableName:    sqlFk.ParentTable,
		ReferencedTableIndex:   refTableIndexName,
		ReferencedTableColumns: refColTags,
		OnUpdate:               onUpdateRefAction,
		OnDelete:               onDeleteRefAction,
		UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
			TableColumns:           sqlFk.Columns,
			ReferencedTableColumns: sqlFk.ParentColumns,
		},
	}, nil
}

// AddForeignKey implements sql.ForeignKeyTable
func (t *AlterableDoltTable) AddForeignKey(ctx *sql.Context, sqlFk sql.ForeignKeyConstraint) error {
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return err
	}
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

	doltFk, err := t.createForeignKey(ctx, root, tbl, sqlFk, onUpdateRefAction, onDeleteRefAction)
	if err != nil {
		return err
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
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return err
	}
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
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return err
	}
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

	if !doltFk.IsResolved() || !sqlFk.IsResolved {
		tbl, _, ok, err := root.GetTableInsensitive(ctx, t.tableName)
		if err != nil {
			return err
		}
		if !ok {
			return sql.ErrTableNotFound.New(t.tableName)
		}
		doltFk, err = t.createForeignKey(ctx, root, tbl, sqlFk, doltFk.OnUpdate, doltFk.OnDelete)
		if err != nil {
			return err
		}
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
func (t *AlterableDoltTable) CreateIndexForForeignKey(ctx *sql.Context, idx sql.IndexDef) error {
	if idx.Constraint != sql.IndexConstraint_None && idx.Constraint != sql.IndexConstraint_Unique {
		return fmt.Errorf("only the following types of index constraints are supported: none, unique")
	}
	columns := make([]string, len(idx.Columns))
	for i, indexCol := range idx.Columns {
		columns[i] = indexCol.Name
	}

	table, err := t.DoltTable.DoltTable(ctx)
	if err != nil {
		return err
	}

	ret, err := creation.CreateIndex(
		ctx,
		table,
		idx.Name,
		columns,
		allocatePrefixLengths(idx.Columns),
		idx.Constraint == sql.IndexConstraint_Unique,
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

// GetForeignKeyEditor implements sql.ForeignKeyTable
func (t *AlterableDoltTable) GetForeignKeyEditor(ctx *sql.Context) sql.ForeignKeyEditor {
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
	oldIdx, err := t.sch.Indexes().RemoveIndex(indexName)
	if err != nil {
		return nil, nil, err
	}

	// any foreign keys that used this underlying index need to find another one
	root, err := t.getRoot(ctx)
	if err != nil {
		return nil, nil, err
	}
	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, fk := range fkc.AllKeys() {
		if fk.ReferencedTableIndex != oldIdx.Name() {
			continue
		}
		// get column names from tags in foreign key
		fkParentCols := make([]string, len(fk.ReferencedTableColumns))
		for i, colTag := range fk.ReferencedTableColumns {
			col, _ := oldIdx.GetColumn(colTag)
			fkParentCols[i] = col.Name
		}
		newIdx, ok, err := findIndexWithPrefix(t.sch, fkParentCols)
		if err != nil {
			return nil, nil, err
		}
		newFk := fk
		if ok {
			newFk.ReferencedTableIndex = newIdx.Name()
		} else {
			// if a replacement index wasn't found; it matched on primary key, so use empty string
			newFk.ReferencedTableIndex = ""
		}
		fkc.RemoveKeys(fk)
		err = fkc.AddKeys(newFk)
		if err != nil {
			return nil, nil, err
		}
	}
	root, err = root.PutForeignKeyCollection(ctx, fkc)
	if err != nil {
		return nil, nil, err
	}
	err = t.setRoot(ctx, root)
	if err != nil {
		return nil, nil, err
	}

	table, err := t.DoltTable.DoltTable(ctx)
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

// updateFromRoot updates the table using data and schema in the root given. This is necessary for some schema change
// statements that take place in multiple steps (e.g. adding a foreign key may create an index, then add a constraint).
// We can't update the session's working set until the statement boundary, so we have to do it here.
// TODO: eliminate this pattern, store all table data and schema in the session rather than in these objects.
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

	// When we update this table we need to also clear any cached versions of the object, since they may now have
	// incorrect schema information
	sess := dsess.DSessFromSess(ctx.Session)
	dbState, ok, err := sess.LookupDbState(ctx, t.db.name)
	if !ok {
		return fmt.Errorf("no db state found for %s", t.db.name)
	}

	dbState.SessionCache().ClearTableCache()

	return nil
}

func (t *AlterableDoltTable) CreateCheck(ctx *sql.Context, check *sql.CheckDefinition) error {
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return err
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

	table, err := t.DoltTable.DoltTable(ctx)
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
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return err
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

	err = sch.Checks().DropCheck(chName)
	if err != nil {
		return err
	}

	table, err := t.DoltTable.DoltTable(ctx)
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

func (t *AlterableDoltTable) CreatePrimaryKey(*sql.Context, []sql.IndexColumn) error {
	return fmt.Errorf("not implemented: AlterableDoltTable.CreatePrimaryKey()")
}

func (t *AlterableDoltTable) DropPrimaryKey(ctx *sql.Context) error {
	return fmt.Errorf("not implemented: AlterableDoltTable.DropPrimaryKey()")
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
		} else if idxI.IsUnique() != idxJ.IsUnique() {
			// prefer unique indexes
			return idxI.IsUnique() && !idxJ.IsUnique()
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
