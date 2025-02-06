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
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/vector"
	"github.com/dolthub/go-mysql-server/sql/fulltext"
	sqltypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
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

var _ dtables.VersionableTable = (*DoltTable)(nil)
var _ dtables.VersionableTable = (*DoltTable)(nil)

// DoltTable implements the sql.Table interface and gives access to dolt table rows and schema.
type DoltTable struct {
	tableName    string
	sqlSch       sql.PrimaryKeySchema
	db           dsess.SqlDatabase
	lockedToRoot doltdb.RootValue
	nbf          *types.NomsBinFormat
	sch          schema.Schema
	autoIncCol   schema.Column

	projectedCols   []uint64
	projectedSchema sql.Schema

	opts editor.Options

	// overriddenSchema is set when the @@dolt_override_schema system var is in use
	overriddenSchema schema.Schema
}

func (t *DoltTable) TableName() doltdb.TableName {
	return doltdb.TableName{Name: t.tableName, Schema: t.db.Schema()}
}

func (t *DoltTable) DatabaseSchema() sql.DatabaseSchema {
	return t.db
}

func (t *DoltTable) SkipIndexCosting() bool {
	return false
}

func (t *DoltTable) LookupForExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.IndexLookup, *sql.FuncDepSet, sql.Expression, bool, error) {
	root, err := t.workingRoot(ctx)
	if err != nil {
		return sql.IndexLookup{}, nil, nil, false, err
	}

	schHash, err := doltdb.GetSchemaHash(ctx, root, t.TableName(), t.overriddenSchema)
	if err != nil {
		return sql.IndexLookup{}, nil, nil, false, err
	}

	sess, ok := ctx.Session.(*dsess.DoltSession)
	if !ok {
		return sql.IndexLookup{}, nil, nil, false, nil
	}

	dbState, ok, err := sess.LookupDbState(ctx, t.db.AliasedName())
	if err != nil {
		return sql.IndexLookup{}, nil, nil, false, nil
	}
	if !ok {
		return sql.IndexLookup{}, nil, nil, false, fmt.Errorf("no state for database %s", t.db.AliasedName())
	}

	var lookupCols []expression.LookupColumn
	var leftoverExpr sql.Expression
	for _, e := range exprs {
		col, ok := expression.LookupEqualityColumn(t.db.Name(), t.tableName, e)
		if ok {
			lookupCols = append(lookupCols, col)
		} else if leftoverExpr == nil {
			leftoverExpr = e
		} else {
			leftoverExpr = expression.NewAnd(leftoverExpr, e)
		}
	}

	colset := sql.NewFastIntSet()
	schCols := t.sch.GetAllCols()
	for _, c := range lookupCols {
		col := schCols.LowerNameToCol[c.Col]
		idx := schCols.TagToIdx[col.Tag]
		colset.Add(idx + 1)
	}

	schKey := doltdb.DataCacheKey{Hash: schHash}

	lookups, ok := dbState.SessionCache().GetCachedStrictLookup(schKey)
	if !ok {
		indexes, err := t.GetIndexes(ctx)
		if err != nil {
			return sql.IndexLookup{}, nil, nil, false, err
		}
		lookups = index.GetStrictLookups(schCols, indexes)
		dbState.SessionCache().CacheStrictLookup(schKey, lookups)
	}

	for _, lookup := range lookups {
		if lookup.Cols.Intersection(colset).Len() == lookup.Cols.Len() {
			// (1) assign lookup columns to range expressions in the appropriate
			// order for the given lookup.
			// (2) aggregate the unused expressions into the return filter.
			rb := sql.NewEqualityIndexBuilder(lookup.Idx)
			for _, c2 := range lookupCols {
				var matched bool
				var matchIdx int
				for i, ord := range lookup.Ordinals {
					// the ordinals redirection accounts for index
					// columns not in schema order
					idx := ord - 1
					c := schCols.GetColumns()[idx]
					if strings.EqualFold(c2.Col, c.Name) {
						matched = true
						matchIdx = i
						break
					}
				}
				if matched {
					if err := rb.AddEquality(ctx, matchIdx, c2.Lit.Value()); err != nil {
						return sql.IndexLookup{}, nil, nil, false, nil
					}
				}
				if !matched || !expression.PreciseComparison(c2.Eq) {
					if leftoverExpr == nil {
						leftoverExpr = c2.Eq
					} else {
						leftoverExpr = expression.NewAnd(leftoverExpr, c2.Eq)
					}
				}
			}
			ret, err := rb.Build(ctx)
			if err != nil {
				return sql.IndexLookup{}, nil, nil, false, err
			}

			return ret, lookup.Fds, leftoverExpr, true, nil
		}
	}

	return sql.IndexLookup{}, nil, nil, false, nil

}

func NewDoltTable(name string, sch schema.Schema, tbl *doltdb.Table, db dsess.SqlDatabase, opts editor.Options) (*DoltTable, error) {
	var autoCol schema.Column
	_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.AutoIncrement {
			autoCol = col
			stop = true
		}
		return
	})

	sqlSch, err := sqlutil.FromDoltSchema(db.Name(), name, sch)
	if err != nil {
		return nil, err
	}

	return &DoltTable{
		tableName:        name,
		db:               db,
		nbf:              tbl.Format(),
		sch:              sch,
		sqlSch:           sqlSch,
		autoIncCol:       autoCol,
		projectedCols:    nil,
		overriddenSchema: tbl.GetOverriddenSchema(),
		opts:             opts,
	}, nil
}

// LockedToRoot returns a version of this table with its root value locked to the given value. The table's values will
// not change as the session's root value changes. Appropriate for AS OF queries, or other use cases where the table's
// values should not change throughout execution of a session.
func (t *DoltTable) LockedToRoot(ctx *sql.Context, root doltdb.RootValue) (sql.IndexAddressableTable, error) {
	tbl, ok, err := root.GetTable(ctx, t.TableName())
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

	sqlSch, err := sqlutil.FromDoltSchema(t.db.Name(), t.tableName, sch)
	if err != nil {
		return nil, err
	}

	dt := &DoltTable{
		tableName:        t.tableName,
		db:               t.db,
		nbf:              tbl.Format(),
		sch:              sch,
		sqlSch:           sqlSch,
		autoIncCol:       autoCol,
		opts:             t.opts,
		lockedToRoot:     root,
		overriddenSchema: t.overriddenSchema,
	}
	return dt.WithProjections(t.Projections()).(*DoltTable), nil
}

// Internal interface for declaring the interfaces that read-only dolt tables are expected to implement
// Add new interfaces supported here, rather than in separate type assertions
type doltReadOnlyTableInterface interface {
	sql.TemporaryTable
	sql.IndexAddressableTable
	sql.ForeignKeyTable
	sql.StatisticsTable
	sql.CheckTable
	sql.PrimaryKeyTable
	sql.CommentedTable
	sql.DatabaseSchemaTable
}

var _ doltReadOnlyTableInterface = (*DoltTable)(nil)
var _ sql.ProjectedTable = (*DoltTable)(nil)
var _ sql.IndexSearchable = (*DoltTable)(nil)

// IndexedAccess implements sql.IndexAddressableTable
func (t *DoltTable) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	return NewIndexedDoltTable(t, lookup.Index.(index.DoltIndex))
}

// DoltTable returns the underlying doltTable from the current session
func (t *DoltTable) DoltTable(ctx *sql.Context) (*doltdb.Table, error) {
	root, err := t.workingRoot(ctx)
	if err != nil {
		return nil, err
	}

	table, ok, err := root.GetTable(ctx, t.TableName())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(t.tableName)
	}

	if t.overriddenSchema != nil {
		table.OverrideSchema(t.overriddenSchema)
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

func (t *DoltTable) IndexCacheKey(ctx *sql.Context) (doltdb.DataCacheKey, bool, error) {
	root, err := t.workingRoot(ctx)
	if err != nil {
		return doltdb.DataCacheKey{}, false, err
	}

	key, err := doltdb.GetSchemaHash(ctx, root, t.TableName(), t.overriddenSchema)
	if err != nil {
		return doltdb.DataCacheKey{}, false, err
	}

	return doltdb.DataCacheKey{Hash: key}, true, nil
}

func (t *DoltTable) workingRoot(ctx *sql.Context) (doltdb.RootValue, error) {
	root := t.lockedToRoot
	if root == nil {
		return t.getRoot(ctx)
	}
	return root, nil
}

// getRoot returns the current root value for this session, to be used for all table data access.
func (t *DoltTable) getRoot(ctx *sql.Context) (doltdb.RootValue, error) {
	return t.db.GetRoot(ctx)
}

// GetIndexes implements sql.IndexedTable
func (t *DoltTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	// If a schema override is in place, we can't trust that the indexes stored with the data
	// will match up to the overridden schema, so we disable indexes. We could improve this by
	// adding schema mapping for the indexes.
	if t.overriddenSchema != nil {
		return nil, nil
	}

	key, tableIsCacheable, err := t.IndexCacheKey(ctx)
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
	dbState, ok, err := sess.LookupDbState(ctx, t.db.RevisionQualifiedName())
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

func (t *DoltTable) PreciseMatch() bool {
	return true
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
	// If this table has been set with a specific projection, always prefer returning that as the schema. This
	// enables rules like eraseProjections to operate correctly.
	if t.projectedSchema != nil {
		return t.projectedSchema
	}

	// If there is an overridden schema, prefer that next
	if t.overriddenSchema != nil {
		sqlSchema, err := sqlutil.FromDoltSchema(t.db.Name(), t.tableName, t.overriddenSchema)
		if err != nil {
			// panic'ing isn't ideal, but this method doesn't allow returning an error.
			// We could log this and return nil, but that will just cause a problem when
			// the caller tries to use the value, so panic'ing seems appropriate.
			panic("error converting to sql schema: " + err.Error())
		}
		return sqlSchema.Schema
	}

	// Finally, use the original schema that matches the data if nothing has been overridden
	return t.sqlSchema().Schema
}

// Collation returns the collation for this table.
func (t *DoltTable) Collation() sql.CollationID {
	return sql.CollationID(t.sch.GetCollation())
}

// Comment returns the comment for this table.
func (t *DoltTable) Comment() string {
	return t.sch.GetComment()
}

func (t *DoltTable) sqlSchema() sql.PrimaryKeySchema {
	// TODO: this should consider projections
	if len(t.sqlSch.Schema) > 0 {
		return t.sqlSch
	}

	// TODO: fix panics
	sqlSch, err := sqlutil.FromDoltSchema(t.db.RevisionQualifiedName(), t.tableName, t.sch)
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
	numBytesPerRow := schema.SchemaAvgLength(t.Schema())
	numRows, err := t.numRows(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

// RowCount implements the sql.StatisticsTable interface.
func (t *DoltTable) RowCount(ctx *sql.Context) (uint64, bool, error) {
	rows, err := t.numRows(ctx)
	return rows, true, err
}

func (t *DoltTable) PrimaryKeySchema() sql.PrimaryKeySchema {
	if t.overriddenSchema != nil {
		doltSchema, err := sqlutil.FromDoltSchema(t.db.Name(), t.tableName, t.overriddenSchema)
		if err != nil {
			// panic'ing isn't ideal, but this method doesn't allow returning an error.
			// We could log this and return nil, but that will just cause a problem when
			// the caller tries to use the value, so panic'ing seems appropriate.
			panic("error converting to sql schema: " + err.Error())
		}
		return doltSchema
	}

	return t.sqlSchema()
}

// PartitionRows returns the table rows for the partition given
func (t *DoltTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	table, err := t.DoltTable(ctx)
	if err != nil {
		return nil, err
	}

	// If we DON'T have an overridden schema, then we can pass in our projected columns as the
	// ones set in the table that the analyzer has told us need to be projected. This will limit our returned
	// sql.Row to only the requested projected columns. If we DO have an overridden schema in use, then we need
	// to pass in the full column projection for the original/data schema so that we get all columns back. Then,
	// the mappingRowIterator that we apply on top of the original row iterator will take care of mapping the
	// original row and shrinking it down to the projected columns.
	projCols := t.projectedCols
	if t.overriddenSchema != nil {
		originalSchemaCols := t.sch.GetAllCols().GetColumns()
		projCols = make([]uint64, len(originalSchemaCols))
		for i, col := range originalSchemaCols {
			projCols[i] = col.Tag
		}
	}

	originalRowIter, err := partitionRows(ctx, table, projCols, partition)
	if err != nil {
		return originalRowIter, err
	}

	if t.overriddenSchema != nil {
		return newMappingRowIter(ctx, t, originalRowIter)
	} else {
		return originalRowIter, err
	}
}

func partitionRows(ctx *sql.Context, t *doltdb.Table, projCols []uint64, partition sql.Partition) (sql.RowIter, error) {
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
	db                 Database
	pinnedWriteSession dsess.WriteSession
}

var _ doltTableInterface = (*WritableDoltTable)(nil)

// WritableDoltTableWrapper is an interface that allows a table to be returned as an sql.Table, but actually be a wrapped
// fake table. Specifically, databases.getTableInsensitive will returns an sql.Table, and there are cases where we
// want to return a table that hasn't been materialized yet.
type WritableDoltTableWrapper interface {
	// Unwrap returns the underlying WritableDoltTable, nil returns are expected when the wrapped table hasn't been materialized
	UnWrap() *WritableDoltTable
}

// Internal interface for declaring the interfaces that writable dolt tables are expected to implement
type doltTableInterface interface {
	sql.UpdatableTable
	sql.DeletableTable
	sql.InsertableTable
	sql.ReplaceableTable
	sql.AutoIncrementTable
	sql.TruncateableTable
	sql.ProjectedTable
	sql.Databaseable
}

func (t *WritableDoltTable) Database() string {
	return t.db.baseName
}

func (t *WritableDoltTable) setRoot(ctx *sql.Context, newRoot doltdb.RootValue) error {
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
	}
}

// Inserter implements sql.InsertableTable
func (t *WritableDoltTable) Inserter(ctx *sql.Context) sql.RowInserter {
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	te, err := t.getTableEditor(ctx)
	if err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	return te
}

func (t *WritableDoltTable) getTableEditor(ctx *sql.Context) (ed dsess.TableWriter, err error) {
	ds := dsess.DSessFromSess(ctx.Session)

	var writeSession dsess.WriteSession
	if t.pinnedWriteSession != nil {
		writeSession = t.pinnedWriteSession
	} else {
		state, _, err := ds.LookupDbState(ctx, t.db.RevisionQualifiedName())
		if err != nil {
			return nil, err
		}
		writeSession = state.WriteSession()
	}

	setter := ds.SetWorkingRoot

	ed, err = writeSession.GetTableWriter(ctx, t.TableName(), t.db.RevisionQualifiedName(), setter, false)
	if err != nil {
		return nil, err
	}

	if t.sch.Indexes().ContainsFullTextIndex() {
		ftEditor, err := t.getFullTextEditor(ctx)
		if err != nil {
			return nil, err
		}
		multiEditor, err := fulltext.CreateMultiTableEditor(ctx, ed, ftEditor)
		if err != nil {
			return nil, err
		}
		return multiEditor.(dsess.TableWriter), nil
	} else {
		return ed, nil
	}
}

// getFullTextEditor gathers all pseudo-index tables for a Full-Text index and returns an editor that will write
// to all of them. This assumes that there are Full-Text indexes in the schema.
func (t *WritableDoltTable) getFullTextEditor(ctx *sql.Context) (fulltext.TableEditor, error) {
	workingRoot, err := t.workingRoot(ctx)
	if err != nil {
		return fulltext.TableEditor{}, err
	}

	configTable, sets, err := t.fulltextTableSets(ctx, workingRoot)
	if err != nil {
		return fulltext.TableEditor{}, err
	}

	return fulltext.CreateEditor(ctx, t, configTable, sets...)
}

func (t *WritableDoltTable) fulltextTableSets(ctx *sql.Context, workingRoot doltdb.RootValue) (fulltext.EditableTable, []fulltext.TableSet, error) {
	var configTable fulltext.EditableTable
	var sets []fulltext.TableSet
	for _, idx := range t.sch.Indexes().AllIndexes() {
		if !idx.IsFullText() {
			continue
		}
		props := idx.FullTextProperties()
		// We only load the config table once since it's shared by all indexes
		// TODO: should this load directly from the root, bypassing the session?
		if configTable == nil {
			tbl, ok, err := t.db.getTable(ctx, workingRoot, props.ConfigTable)
			if err != nil {
				return nil, nil, err
			} else if !ok {
				return nil, nil, fmt.Errorf("missing Full-Text table: %s", props.ConfigTable)
			}
			configTable = tbl.(fulltext.EditableTable)
		}
		// Load the rest of the tables
		positionTable, ok, err := t.db.getTable(ctx, workingRoot, props.PositionTable)
		if err != nil {
			return nil, nil, err
		} else if !ok {
			return nil, nil, fmt.Errorf("missing Full-Text table: %s", props.PositionTable)
		}
		docCountTable, ok, err := t.db.getTable(ctx, workingRoot, props.DocCountTable)
		if err != nil {
			return nil, nil, err
		} else if !ok {
			return nil, nil, fmt.Errorf("missing Full-Text table: %s", props.DocCountTable)
		}
		globalCountTable, ok, err := t.db.getTable(ctx, workingRoot, props.GlobalCountTable)
		if err != nil {
			return nil, nil, err
		} else if !ok {
			return nil, nil, fmt.Errorf("missing Full-Text table: %s", props.GlobalCountTable)
		}
		rowCountTable, ok, err := t.db.getTable(ctx, workingRoot, props.RowCountTable)
		if err != nil {
			return nil, nil, err
		} else if !ok {
			return nil, nil, fmt.Errorf("missing Full-Text table: %s", props.RowCountTable)
		}
		// Convert the index into a sql.Index
		sqlIdx, err := index.ConvertFullTextToSql(ctx, t.db.RevisionQualifiedName(), t.tableName, t.sch, idx)
		if err != nil {
			return nil, nil, err
		}

		sets = append(sets, fulltext.TableSet{
			Index:       sqlIdx.(fulltext.Index),
			Position:    positionTable.(fulltext.EditableTable),
			DocCount:    docCountTable.(fulltext.EditableTable),
			GlobalCount: globalCountTable.(fulltext.EditableTable),
			RowCount:    rowCountTable.(fulltext.EditableTable),
		})
	}
	return configTable, sets, nil
}

// tableSetsForRewrite returns the fulltext.TableSet for each Full-Text index in the table, truncated and modified
// for a table rewrite operation. Returns the root given with all full-text pseudo tables updated with their new
// truncated value.
func (t *WritableDoltTable) tableSetsForRewrite(
	ctx *sql.Context,
	workingRoot doltdb.RootValue,
) (doltdb.RootValue, fulltext.EditableTable, []fulltext.TableSet, error) {
	configTable, sets, err := t.fulltextTableSets(ctx, workingRoot)
	if err != nil {
		return nil, nil, nil, err
	}

	// truncate each of the fulltext tables in each set before returning them
	_, insertCols, err := fulltext.GetKeyColumns(ctx, t)
	if err != nil {
		return nil, nil, nil, err
	}

	newSets := make([]fulltext.TableSet, len(sets))
	for i := range sets {
		set := sets[i]

		positionSch, err := fulltext.NewSchema(fulltext.SchemaPosition, insertCols, set.Position.Name(), t.Collation())
		if err != nil {
			return nil, nil, nil, err
		}

		posTableDolt, posTable, err := emptyFulltextTable(ctx, t, workingRoot, set.Position, positionSch)
		if err != nil {
			return nil, nil, nil, err
		}

		docCountSch, err := fulltext.NewSchema(fulltext.SchemaDocCount, insertCols, set.DocCount.Name(), t.Collation())
		if err != nil {
			return nil, nil, nil, err
		}

		dcTableDolt, dcTable, err := emptyFulltextTable(ctx, t, workingRoot, set.DocCount, docCountSch)
		if err != nil {
			return nil, nil, nil, err
		}

		globalCountSch, err := fulltext.NewSchema(fulltext.SchemaGlobalCount, nil, set.GlobalCount.Name(), t.Collation())
		if err != nil {
			return nil, nil, nil, err
		}

		gcTableDolt, gcTable, err := emptyFulltextTable(ctx, t, workingRoot, set.GlobalCount, globalCountSch)
		if err != nil {
			return nil, nil, nil, err
		}

		rowCountSch, err := fulltext.NewSchema(fulltext.SchemaRowCount, nil, set.RowCount.Name(), t.Collation())
		if err != nil {
			return nil, nil, nil, err
		}

		rcTableDolt, rcTable, err := emptyFulltextTable(ctx, t, workingRoot, set.RowCount, rowCountSch)
		if err != nil {
			return nil, nil, nil, err
		}

		set.Position = posTable
		set.DocCount = dcTable
		set.GlobalCount = gcTable
		set.RowCount = rcTable

		workingRoot, err = workingRoot.PutTable(ctx, doltdb.TableName{Name: posTable.Name(), Schema: t.db.schemaName}, posTableDolt)
		if err != nil {
			return nil, nil, nil, err
		}

		workingRoot, err = workingRoot.PutTable(ctx, doltdb.TableName{Name: dcTable.Name(), Schema: t.db.schemaName}, dcTableDolt)
		if err != nil {
			return nil, nil, nil, err
		}

		workingRoot, err = workingRoot.PutTable(ctx, doltdb.TableName{Name: gcTable.Name(), Schema: t.db.schemaName}, gcTableDolt)
		if err != nil {
			return nil, nil, nil, err
		}

		workingRoot, err = workingRoot.PutTable(ctx, doltdb.TableName{Name: rcTable.Name(), Schema: t.db.schemaName}, rcTableDolt)
		if err != nil {
			return nil, nil, nil, err
		}

		newSets[i] = set
	}

	return workingRoot, configTable, newSets, nil
}

// emptyFulltextTable returns a new empty fulltext table with the given schema, and the underlying dolt table.
func emptyFulltextTable(
	ctx *sql.Context,
	parentTable *WritableDoltTable,
	workingRoot doltdb.RootValue,
	fulltextTable fulltext.EditableTable,
	fulltextSch sql.Schema,
) (*doltdb.Table, fulltext.EditableTable, error) {
	doltTable, ok := fulltextTable.(*AlterableDoltTable)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected row count table type: %T", fulltextTable)
	}

	// TODO: this should be the head root, not working root
	doltSchema, err := sqlutil.ToDoltSchema(ctx, workingRoot, doltTable.TableName(), sql.NewPrimaryKeySchema(fulltextSch), workingRoot, parentTable.Collation())
	if err != nil {
		return nil, nil, err
	}

	dt, err := doltTable.DoltTable.DoltTable(ctx)
	if err != nil {
		return nil, nil, err
	}

	empty, err := durable.NewEmptyPrimaryIndex(ctx, dt.ValueReadWriter(), dt.NodeStore(), doltSchema)
	if err != nil {
		return nil, nil, err
	}

	dt, err = doltdb.NewTable(ctx, dt.ValueReadWriter(), dt.NodeStore(), doltSchema, empty, nil, nil)
	if err != nil {
		return nil, nil, err
	}

	newTable, err := parentTable.db.newDoltTable(fulltextTable.Name(), doltSchema, dt)
	if err != nil {
		return nil, nil, err
	}

	return dt, newTable.(fulltext.EditableTable), nil
}

// Deleter implements sql.DeletableTable
func (t *WritableDoltTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
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
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
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
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
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
	newRoot, err := root.PutTable(ctx, t.TableName(), newTable)
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
	idxSet, err := table.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	for _, idx := range sch.Indexes().AllIndexes() {
		empty, err := durable.NewEmptyIndexFromTableSchema(ctx, table.ValueReadWriter(), table.NodeStore(), idx, sch)
		if err != nil {
			return nil, err
		}

		idxSet, err = idxSet.PutIndex(ctx, idx.Name(), empty)
		if err != nil {
			return nil, err
		}
	}

	ws, err := sess.WorkingSet(ctx, t.db.RevisionQualifiedName())
	if err != nil {
		return nil, err
	}

	if schema.HasAutoIncrement(sch) {
		ddb, _ := sess.GetDoltDB(ctx, t.db.RevisionQualifiedName())
		err = t.db.removeTableFromAutoIncrementTracker(ctx, t.Name(), ddb, ws.Ref())
		if err != nil {
			return nil, err
		}
	}

	empty, err := durable.NewEmptyPrimaryIndex(ctx, table.ValueReadWriter(), table.NodeStore(), sch)
	if err != nil {
		return nil, err
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
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
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
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
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

	if multiTableEditor, ok := ed.(fulltext.MultiTableEditor); ok {
		return multiTableEditor.PrimaryEditor().(writer.AutoIncrementGetter).GetNextAutoIncrementValue(ctx, potentialVal)
	} else {
		return ed.(writer.AutoIncrementGetter).GetNextAutoIncrementValue(ctx, potentialVal)
	}
}

func (t *DoltTable) GetChecks(ctx *sql.Context) ([]sql.CheckDefinition, error) {
	table, err := t.DoltTable(ctx)
	if err != nil {
		return nil, err
	}

	key, tableIsCacheable, err := t.IndexCacheKey(ctx)
	if err != nil {
		return nil, err
	}

	if !tableIsCacheable {
		sch, err := table.GetSchema(ctx)
		if err != nil {
			return nil, err
		}

		return checksInSchema(sch), nil
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbState, ok, err := sess.LookupDbState(ctx, t.db.RevisionQualifiedName())
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, fmt.Errorf("couldn't find db state for database %s", t.db.Name())
	}

	checks, ok := dbState.SessionCache().GetCachedTableChecks(key)
	if ok {
		return checks, nil
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	checks = checksInSchema(sch)

	dbState.SessionCache().CacheTableChecks(key, checks)
	return checks, nil
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

// GetForeignKeyEditor implements sql.ForeignKeyTable
func (t *WritableDoltTable) GetForeignKeyEditor(ctx *sql.Context) sql.ForeignKeyEditor {
	te, err := t.getTableEditor(ctx)
	if err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	return te
}

// GetDeclaredForeignKeys implements sql.ForeignKeyTable
func (t *DoltTable) GetDeclaredForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	root, err := t.workingRoot(ctx)
	if err != nil {
		return nil, err
	}

	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	declaredFks, _ := fkc.KeysForTable(t.TableName())
	toReturn := make([]sql.ForeignKeyConstraint, len(declaredFks))

	for i, fk := range declaredFks {
		if len(fk.UnresolvedFKDetails.TableColumns) > 0 && len(fk.UnresolvedFKDetails.ReferencedTableColumns) > 0 {
			//TODO: implement multi-db support for foreign keys
			toReturn[i] = sql.ForeignKeyConstraint{
				Name:           fk.Name,
				Database:       t.db.Name(),
				Table:          fk.TableName.Name,
				SchemaName:     fk.TableName.Schema,
				Columns:        fk.UnresolvedFKDetails.TableColumns,
				ParentDatabase: t.db.Name(),
				ParentTable:    fk.ReferencedTableName.Name,
				ParentSchema:   fk.ReferencedTableName.Schema,
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

	_, referencedByFk := fkc.KeysForTable(t.TableName())
	toReturn := make([]sql.ForeignKeyConstraint, len(referencedByFk))

	for i, fk := range referencedByFk {
		if len(fk.UnresolvedFKDetails.TableColumns) > 0 && len(fk.UnresolvedFKDetails.ReferencedTableColumns) > 0 {
			//TODO: implement multi-db support for foreign keys
			toReturn[i] = sql.ForeignKeyConstraint{
				Name:           fk.Name,
				Database:       t.db.Name(),
				Table:          fk.TableName.Name, // TODO: schema name
				Columns:        fk.UnresolvedFKDetails.TableColumns,
				ParentDatabase: t.db.Name(),
				ParentTable:    fk.ReferencedTableName.Name, // TODO: schema name
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
func (t *DoltTable) CreateIndexForForeignKey(ctx *sql.Context, idx sql.IndexDef) error {
	return fmt.Errorf("no foreign key operations on a read-only table")
}

// AddForeignKey implements sql.ForeignKeyTable
func (t *DoltTable) AddForeignKey(ctx *sql.Context, fk sql.ForeignKeyConstraint) error {
	return fmt.Errorf("no foreign key operations on a read-only table")
}

// DropForeignKey implements sql.ForeignKeyTable
func (t *DoltTable) DropForeignKey(ctx *sql.Context, fkName string) error {
	return fmt.Errorf("no foreign key operations on a read-only table")
}

// UpdateForeignKey implements sql.ForeignKeyTable
func (t *DoltTable) UpdateForeignKey(ctx *sql.Context, fkName string, fk sql.ForeignKeyConstraint) error {
	return fmt.Errorf("no foreign key operations on a read-only table")
}

// GetForeignKeyEditor implements sql.ForeignKeyTable
func (t *DoltTable) GetForeignKeyEditor(ctx *sql.Context) sql.ForeignKeyEditor {
	return nil
}

// Projections implements sql.ProjectedTable
func (t *DoltTable) Projections() []string {
	// The semantics of nil v. empty are important for this interface, they display differently in explain plans
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
	sch := t.Schema()
	schemaSchema := t.sch
	if t.overriddenSchema != nil {
		schemaSchema = t.overriddenSchema
	}
	cols := schemaSchema.GetAllCols()
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
	if t.overriddenSchema != nil {
		doltSchema, err := sqlutil.FromDoltSchema(t.db.Name(), t.tableName, t.overriddenSchema)
		if err != nil {
			// panic'ing isn't ideal, but this method doesn't allow returning an error.
			// We could log this and return nil, but that will just cause a problem when
			// the caller tries to use the value, so panic'ing seems appropriate.
			panic("error converting to sql schema: " + err.Error())
		}
		return doltSchema
	}

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
	sql.CollationAlterableTable
	fulltext.IndexAlterableTable
}

var _ doltAlterableTableInterface = (*AlterableDoltTable)(nil)
var _ sql.RewritableTable = (*AlterableDoltTable)(nil)

func (t *AlterableDoltTable) WithProjections(colNames []string) sql.Table {
	return &AlterableDoltTable{WritableDoltTable: *t.WritableDoltTable.WithProjections(colNames).(*WritableDoltTable)}
}

// AddColumn implements sql.AlterableTable
func (t *AlterableDoltTable) AddColumn(ctx *sql.Context, column *sql.Column, order *sql.ColumnOrder) error {
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
		return err
	}
	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}

	table, _, err := root.GetTable(ctx, t.TableName())
	if err != nil {
		return err
	}

	ti, err := typeinfo.FromSqlType(column.Type)
	if err != nil {
		return err
	}
	tags, err := doltdb.GenerateTagsForNewColumns(ctx, root, t.TableName(), []string{column.Name}, []types.NomsKind{ti.NomsKind()}, nil)
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
		ait, err := t.db.gs.AutoIncrementTracker(ctx)
		if err != nil {
			return err
		}
		ait.AddNewTable(t.tableName)
	}

	newRoot, err := root.PutTable(ctx, t.TableName(), updatedTable)
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
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
		return nil, err
	}
	err := validateSchemaChange(t.Name(), oldSchema, newSchema, oldColumn, newColumn, idxCols)
	if err != nil {
		return nil, err
	}

	sess := dsess.DSessFromSess(ctx.Session)

	// Begin by creating a new table with the same name and the new schema, then removing all its existing rows
	dbState, ok, err := sess.LookupDbState(ctx, t.db.RevisionQualifiedName())
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, fmt.Errorf("database %s not found in session", t.db.Name())
	}

	ws := dbState.WorkingSet()
	if ws == nil {
		return nil, doltdb.ErrOperationNotSupportedInDetachedHead
	}

	head, err := sess.GetHeadCommit(ctx, t.db.RevisionQualifiedName())
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

	newSch, err := t.createSchemaForColumnChange(ctx, oldColumn, newColumn, oldSch, newSchema, ws.WorkingRoot(), headRoot)
	if err != nil {
		return nil, err
	}
	newSch = schema.CopyChecksConstraints(oldSch, newSch)

	isModifyColumn := newColumn != nil && oldColumn != nil
	if isColumnDrop(oldSchema, newSchema) {
		newSch, err = dropIndexesOnDroppedColumn(newSch, oldSch, oldSchema, newSchema, err)
		if err != nil {
			return nil, err
		}
	} else if isModifyColumn {
		newSch, err = modifyIndexesForTableRewrite(ctx, oldSch, oldColumn, newColumn, newSch)
		if err != nil {
			return nil, err
		}
	} else {
		// we need a temp version of a sql.Table here to get key columns
		newTbl, err := t.db.newDoltTable(t.Name(), newSch, dt)
		if err != nil {
			return nil, err
		}

		keyCols, _, err := fulltext.GetKeyColumns(ctx, newTbl)
		if err != nil {
			return nil, err
		}

		// this copies over all non-full-text indexes in place
		newSch, err = modifyFulltextIndexesForRewrite(ctx, keyCols, oldSch, newSch)
		if err != nil {
			return nil, err
		}
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

	// Grab the next auto_increment value before we call truncate, since truncate will delete the table
	// and clear out the auto_increment tracking for this table.
	var nextAutoIncValue uint64
	if t.autoIncCol.AutoIncrement {
		nextAutoIncValue, err = t.PeekNextAutoIncrementValue(ctx)
		if err != nil {
			return nil, err
		}
	}

	// TODO: test for this when the table is auto increment and exists on another branch
	dt, err = t.truncate(ctx, dt, newSch, sess)
	if err != nil {
		return nil, err
	}

	newRoot, err := ws.WorkingRoot().PutTable(ctx, t.TableName(), dt)
	if err != nil {
		return nil, err
	}

	isPrimaryKeyDrop := len(oldSchema.PkOrdinals) > 0 && len(newSchema.PkOrdinals) == 0
	if isPrimaryKeyDrop {
		newRoot, err = t.adjustForeignKeysForDroppedPk(ctx, t.Name(), newRoot)
		if err != nil {
			return nil, err
		}
	}

	// We can't just call getTableEditor for this operation because it uses the session state, which we can't update
	// until after the rewrite operation
	if newSch.Indexes().ContainsFullTextIndex() {
		return fullTextRewriteEditor(ctx, t, newSch, dt, ws, sess, dbState, newRoot)
	}

	// TODO: figure out locking. Other DBs automatically lock a table during this kind of operation, we should probably
	//  do the same. We're messing with global auto-increment values here and it's not safe.
	ait, err := t.db.gs.AutoIncrementTracker(ctx)
	if err != nil {
		return nil, err
	}

	newWs := ws.WithWorkingRoot(newRoot)

	// Restore the next auto increment value, since it was cleared when we truncated the table
	if t.autoIncCol.AutoIncrement {
		err = t.AutoIncrementSetter(ctx).SetAutoIncrementValue(ctx, nextAutoIncValue)
		if err != nil {
			return nil, err
		}
	}

	if ws := dbState.WriteSession(); ws == nil {
		return nil, fmt.Errorf("cannot rebuild index on a headless branch")
	}

	opts := dbState.WriteSession().GetOptions()
	opts.ForeignKeyChecksDisabled = true
	writeSession := writer.NewWriteSession(dt.Format(), newWs, ait, opts)

	ed, err := writeSession.GetTableWriter(ctx, t.TableName(), t.db.RevisionQualifiedName(), sess.SetWorkingRoot, false)
	if err != nil {
		return nil, err
	}

	return ed, nil
}

func fullTextRewriteEditor(
	ctx *sql.Context,
	t *AlterableDoltTable,
	newSch schema.Schema,
	dt *doltdb.Table,
	ws *doltdb.WorkingSet,
	sess *dsess.DoltSession,
	dbState dsess.SessionState,
	workingRoot doltdb.RootValue,
) (sql.RowInserter, error) {

	newTable, err := t.db.newDoltTable(t.Name(), newSch, dt)
	if err != nil {
		return nil, err
	}

	updatedRoot, configTable, tableSets, err := newTable.(*AlterableDoltTable).tableSetsForRewrite(ctx, workingRoot)
	if err != nil {
		return nil, err
	}

	// TODO: figure out locking. Other DBs automatically lock a table during this kind of operation, we should probably
	//  do the same. We're messing with global auto-increment values here and it's not safe.
	ait, err := t.db.gs.AutoIncrementTracker(ctx)
	if err != nil {
		return nil, err
	}

	newWs := ws.WithWorkingRoot(updatedRoot)

	// We need our own write session for the rewrite operation. The connection's session must continue to return rows of
	// the table as it existed before the rewrite operation began until it completes, at which point we update the
	// session with the rewritten table.
	if ws := dbState.WriteSession(); ws == nil {
		return nil, fmt.Errorf("cannot rebuild index on read only database %s", t.Name())
	}

	opts := dbState.WriteSession().GetOptions()
	opts.ForeignKeyChecksDisabled = true
	writeSession := writer.NewWriteSession(dt.Format(), newWs, ait, opts)

	parentEditor, err := writeSession.GetTableWriter(ctx, t.TableName(), t.db.RevisionQualifiedName(), sess.SetWorkingRoot, false)
	if err != nil {
		return nil, err
	}

	// There's a layer of indirection here: the call to fulltext.CreateEditor is going to in turn ask each of these
	// tables for an Inserter, and we need to return the one we're using to do the rewrite, not a fresh one from the
	// session's data (which still has the tables as they existed before the rewrite began). To get around this,
	// we manually set the writeSession in these tables before passing control back to the engine. Then in Inserter(),
	// we check for a pinned writeSession and return that one, not the session one.
	for i := range tableSets {
		tableSets[i].Position.(*AlterableDoltTable).SetWriteSession(writeSession)
		tableSets[i].DocCount.(*AlterableDoltTable).SetWriteSession(writeSession)
		tableSets[i].GlobalCount.(*AlterableDoltTable).SetWriteSession(writeSession)
		tableSets[i].RowCount.(*AlterableDoltTable).SetWriteSession(writeSession)
	}

	ftEditor, err := fulltext.CreateEditor(ctx, newTable, configTable, tableSets...)
	if err != nil {
		return nil, err
	}

	multiEditor, err := fulltext.CreateMultiTableEditor(ctx, parentEditor, ftEditor)
	if err != nil {
		return nil, err
	}

	return multiEditor, nil
}

// modifyFulltextIndexesForRewrite modifies the fulltext indexes of a table to correspond to the new schema before
// a table rewrite. All non-full-text indexes are copied from the old schema directly.
func modifyFulltextIndexesForRewrite(ctx *sql.Context, keyCols fulltext.KeyColumns, oldSch schema.Schema, newSch schema.Schema) (schema.Schema, error) {
	for _, idx := range oldSch.Indexes().AllIndexes() {
		if !idx.IsFullText() {
			newSch.Indexes().AddIndex(idx)
			continue
		}

		ft := idx.FullTextProperties()
		keyColPositions := make([]uint16, len(keyCols.Positions))
		for i, pos := range keyCols.Positions {
			keyColPositions[i] = uint16(pos)
		}

		ft.KeyPositions = keyColPositions
		ft.KeyType = uint8(keyCols.Type)

		props := schema.IndexProperties{
			IsUnique:           idx.IsUnique(),
			IsSpatial:          idx.IsSpatial(),
			IsFullText:         true,
			IsVector:           false,
			IsUserDefined:      true,
			Comment:            idx.Comment(),
			FullTextProperties: ft,
		}

		newSch.Indexes().AddIndexByColNames(idx.Name(), idx.ColumnNames(), idx.PrefixLengths(), props)
	}

	return newSch, nil
}

// dropIndexesOnDroppedColumn removes from the schema any indexes which contain a dropped column.
func dropIndexesOnDroppedColumn(newSch schema.Schema, oldSch schema.Schema, oldSchema sql.PrimaryKeySchema, newSchema sql.PrimaryKeySchema, err error) (schema.Schema, error) {
	newSch = schema.CopyIndexes(oldSch, newSch)
	droppedCol := getDroppedColumn(oldSchema, newSchema)
	for _, index := range newSch.Indexes().IndexesWithColumn(droppedCol.Name) {
		_, err = newSch.Indexes().RemoveIndex(index.Name())
		if err != nil {
			return nil, err
		}

		// For fulltext indexes, we don't just remove them entirely on a column drop, we modify them to contain only the
		// remaining columns
		if index.IsFullText() {
			modifyFulltextIndexForColumnDrop(index, newSch, droppedCol)
		}
	}

	return newSch, nil
}

// modifyFulltextIndexForColumnDrop modifies a fulltext index to remove a column that was dropped, adding it to the
// schema's indexes only if there are still remaining columns in the index after the drop
func modifyFulltextIndexForColumnDrop(index schema.Index, newSch schema.Schema, droppedCol *sql.Column) {
	if len(index.ColumnNames()) == 1 {
		// if there was only one column left in the index, we remove it entirely
		return
	}

	var i int
	colNames := make([]string, len(index.ColumnNames())-1)
	for _, col := range index.ColumnNames() {
		if col == droppedCol.Name {
			continue
		}
		colNames[i] = col
		i++
	}

	newSch.Indexes().AddIndexByColNames(
		index.Name(),
		colNames,
		index.PrefixLengths(),
		schema.IndexProperties{
			IsUnique:           index.IsUnique(),
			IsSpatial:          false,
			IsFullText:         true,
			IsVector:           false,
			IsUserDefined:      index.IsUserDefined(),
			Comment:            index.Comment(),
			FullTextProperties: index.FullTextProperties(),
		})
}

func modifyIndexesForTableRewrite(ctx *sql.Context, oldSch schema.Schema, oldColumn *sql.Column, newColumn *sql.Column, newSch schema.Schema) (schema.Schema, error) {
	for _, index := range oldSch.Indexes().AllIndexes() {
		if index.IsFullText() {
			err := validateFullTextColumnChange(ctx, index, oldColumn, newColumn)
			if err != nil {
				return nil, err
			}
		}

		var colNames []string
		prefixLengths := index.PrefixLengths()
		for i, colName := range index.ColumnNames() {
			if strings.EqualFold(oldColumn.Name, colName) {
				colNames = append(colNames, newColumn.Name)
				if len(prefixLengths) > 0 {
					if !sqltypes.IsText(newColumn.Type) {
						// drop prefix lengths if column is not a string type
						prefixLengths[i] = 0
					} else if uint32(prefixLengths[i]) > newColumn.Type.MaxTextResponseByteLength(ctx) {
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
				IsUnique:           index.IsUnique(),
				IsSpatial:          index.IsSpatial(),
				IsFullText:         index.IsFullText(),
				IsVector:           index.IsVector(),
				IsUserDefined:      index.IsUserDefined(),
				Comment:            index.Comment(),
				FullTextProperties: index.FullTextProperties(),
				VectorProperties:   index.VectorProperties(),
			})
	}

	return newSch, nil
}

// validateFullTextColumnChange returns an error if the column change given violates this full text index.
func validateFullTextColumnChange(ctx *sql.Context, idx schema.Index, oldColumn *sql.Column, newColumn *sql.Column) error {
	colNames := idx.ColumnNames()
	for _, colName := range colNames {
		if oldColumn.Name != colName && newColumn.Name != colName {
			continue
		}
		if !sqltypes.IsTextOnly(newColumn.Type) {
			return sql.ErrFullTextInvalidColumnType.New()
		}
	}
	return nil
}

// createSchemaForColumnChange creates a new Dolt schema based on the old Dolt schema (|oldSch|) changing to the new
// SQL schema (|newSchema|) from the SQL column |oldColumn| changing to the SQL column |newColumn|. The working root
// is provided in |root| and the branch head root is provided in |headRoot|. If any problems are encountered, a nil
// Dolt schema is returned along with an error.
func (t *AlterableDoltTable) createSchemaForColumnChange(ctx context.Context, oldColumn, newColumn *sql.Column, oldSch schema.Schema, newSchema sql.PrimaryKeySchema, root, headRoot doltdb.RootValue) (schema.Schema, error) {
	// Adding or dropping a column
	if oldColumn == nil || newColumn == nil {
		newSch, err := sqlutil.ToDoltSchema(ctx, root, t.TableName(), newSchema, headRoot, sql.CollationID(oldSch.GetCollation()))
		if err != nil {
			return nil, err
		}
		return newSch, err
	}

	// Modifying a column
	newSch, err := sqlutil.ToDoltSchema(ctx, root, t.TableName(), newSchema, headRoot, sql.CollationID(oldSch.GetCollation()))
	if err != nil {
		return nil, err
	}

	oldDoltCol, ok := oldSch.GetAllCols().GetByName(oldColumn.Name)
	if !ok {
		return nil, fmt.Errorf("expected column %s to exist in the old schema but did not find it", oldColumn.Name)
	}

	newColCollection := replaceColumnTagInCollection(newSch.GetAllCols(), oldDoltCol.Name, oldDoltCol.Tag)
	newPkColCollection := replaceColumnTagInCollection(newSch.GetPKCols(), oldDoltCol.Name, oldDoltCol.Tag)
	return schema.NewSchema(newColCollection, newSch.GetPkOrdinals(), newSch.GetCollation(),
		schema.NewIndexCollection(newColCollection, newPkColCollection), newSch.Checks())
}

// replaceColumnTagInCollection returns a new ColCollection, based on |cc|, with the column named |name| updated
// to have the specified column tag |tag|. If the column is not found, no changes are made, no errors are returned,
// and the returned ColCollection will be identical to |cc|.
func replaceColumnTagInCollection(cc *schema.ColCollection, name string, tag uint64) *schema.ColCollection {
	newColumns := cc.GetColumns()
	for i := range newColumns {
		if newColumns[i].Name == name {
			newColumns[i].Tag = tag
			break
		}
	}
	return schema.NewColCollection(newColumns...)
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
		idx := newSchema.Schema.IndexOfColName(idxCol.Name)
		if idx < 0 { // avoid panics
			return sql.ErrColumnNotFound.New(idxCol.Name)
		}
		col := newSchema.Schema[idx]
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

func (t *AlterableDoltTable) adjustForeignKeysForDroppedPk(ctx *sql.Context, tbl string, root doltdb.RootValue) (doltdb.RootValue, error) {
	err := sql.ValidatePrimaryKeyDrop(ctx, t, t.PrimaryKeySchema())
	if err != nil {
		return nil, err
	}

	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	fkcUpdates, err := backupFkcIndexesForPkDrop(ctx, tbl, t.sch, fkc)
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
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
		return err
	}
	ws, err := t.db.GetWorkingSet(ctx)
	if err != nil {
		return err
	}
	root := ws.WorkingRoot()

	table, _, err := root.GetTable(ctx, t.TableName())
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
		// TODO: delegate this to tracker?
		seq, err := t.getFirstAutoIncrementValue(ctx, columnName, column.Type, updatedTable)
		if err != nil {
			return err
		}

		updatedTable, err = updatedTable.SetAutoIncrementValue(ctx, seq)
		if err != nil {
			return err
		}

		ait, err := t.db.gs.AutoIncrementTracker(ctx)
		if err != nil {
			return err
		}

		// TODO: this isn't transactional, and it should be (but none of the auto increment tracking is)
		ait.AddNewTable(t.tableName)
		// Since this is a new auto increment table, we don't need to exclude the current working set from consideration
		// when computing its new sequence value, hence the empty ref
		_, err = ait.Set(ctx, t.tableName, updatedTable, ref.WorkingSetRef{}, seq)
		if err != nil {
			return err
		}
	}

	// If we're removing an auto inc property, we just need to update global auto increment tracking
	if existingCol.AutoIncrement && !col.AutoIncrement {
		// TODO: this isn't transactional, and it should be
		sess := dsess.DSessFromSess(ctx.Session)
		ddb, _ := sess.GetDoltDB(ctx, t.db.RevisionQualifiedName())
		err = t.db.removeTableFromAutoIncrementTracker(ctx, t.Name(), ddb, ws.Ref())
		if err != nil {
			return err
		}
	}

	newRoot, err := root.PutTable(ctx, t.TableName(), updatedTable)
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
	rowIter, err := partitionRows(ctx, table, t.projectedCols, index.SinglePartition{RowData: rowData})
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

	seq, err := dsess.CoerceAutoIncrementValue(initialValue)
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
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
		return err
	}
	if idx.Constraint != sql.IndexConstraint_None && idx.Constraint != sql.IndexConstraint_Unique && idx.Constraint != sql.IndexConstraint_Spatial && idx.Constraint != sql.IndexConstraint_Vector {
		return fmt.Errorf("only the following types of index constraints are supported: none, unique, spatial")
	}

	var vectorProperties schema.VectorProperties
	if idx.Constraint == sql.IndexConstraint_Vector {
		vectorProperties = schema.VectorProperties{
			DistanceType: vector.DistanceL2Squared{},
		}
	}
	return t.createIndex(ctx, idx, fulltext.KeyColumns{}, fulltext.IndexTableNames{}, vectorProperties)
}

// DropIndex implements sql.IndexAlterableTable
func (t *AlterableDoltTable) DropIndex(ctx *sql.Context, indexName string) error {
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
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
	newRoot, err := root.PutTable(ctx, t.TableName(), newTable)
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
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
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
	newRoot, err := root.PutTable(ctx, t.TableName(), newTable)
	if err != nil {
		return err
	}

	err = t.setRoot(ctx, newRoot)
	if err != nil {
		return err
	}
	return t.updateFromRoot(ctx, newRoot)
}

// CreateFulltextIndex implements fulltext.IndexAlterableTable
func (t *AlterableDoltTable) CreateFulltextIndex(ctx *sql.Context, idx sql.IndexDef, keyCols fulltext.KeyColumns, tableNames fulltext.IndexTableNames) error {
	if !types.IsFormat_DOLT(t.Format()) {
		return fmt.Errorf("FULLTEXT is not supported on storage format %s. Run `dolt migrate` to upgrade to the latest storage format.", t.Format().VersionString())
	}
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
		return err
	}
	if !idx.IsFullText() {
		return fmt.Errorf("attempted to create non-FullText index through FullText interface")
	}

	return t.createIndex(ctx, idx, keyCols, tableNames, schema.VectorProperties{})
}

// createIndex handles the common functionality between CreateIndex and CreateFulltextIndex.
func (t *AlterableDoltTable) createIndex(ctx *sql.Context, idx sql.IndexDef, keyCols fulltext.KeyColumns, tableNames fulltext.IndexTableNames, vectorProperties schema.VectorProperties) error {
	columns := make([]string, len(idx.Columns))
	for i, indexCol := range idx.Columns {
		columns[i] = indexCol.Name
	}

	table, err := t.DoltTable.DoltTable(ctx)
	if err != nil {
		return err
	}

	var keyPositions []uint16
	if len(keyCols.Positions) > 0 {
		keyPositions = make([]uint16, len(keyCols.Positions))
		for i := range keyPositions {
			keyPositions[i] = uint16(keyCols.Positions[i])
		}
	}

	ret, err := creation.CreateIndex(ctx, table, t.Name(), idx.Name, columns, allocatePrefixLengths(idx.Columns), schema.IndexProperties{
		IsUnique:      idx.Constraint == sql.IndexConstraint_Unique,
		IsSpatial:     idx.Constraint == sql.IndexConstraint_Spatial,
		IsFullText:    idx.Constraint == sql.IndexConstraint_Fulltext,
		IsVector:      idx.Constraint == sql.IndexConstraint_Vector,
		IsUserDefined: true,
		Comment:       idx.Comment,
		FullTextProperties: schema.FullTextProperties{
			ConfigTable:      tableNames.Config,
			PositionTable:    tableNames.Position,
			DocCountTable:    tableNames.DocCount,
			GlobalCountTable: tableNames.GlobalCount,
			RowCountTable:    tableNames.RowCount,
			KeyType:          uint8(keyCols.Type),
			KeyName:          keyCols.Name,
			KeyPositions:     keyPositions,
		},
		VectorProperties: vectorProperties,
	}, t.opts)
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
			if t.TableName() == fk.TableName && fk.TableIndex == ret.OldIndex.Name() {
				newFk.TableIndex = ret.NewIndex.Name()
			}
			if t.TableName() == fk.ReferencedTableName && fk.ReferencedTableIndex == ret.OldIndex.Name() {
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
	newRoot, err := root.PutTable(ctx, t.TableName(), ret.NewTable)
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
func (t *WritableDoltTable) createForeignKey(
	ctx *sql.Context,
	root doltdb.RootValue,
	tbl *doltdb.Table,
	sqlFk sql.ForeignKeyConstraint,
	onUpdateRefAction, onDeleteRefAction doltdb.ForeignKeyReferentialAction) (doltdb.ForeignKey, error) {

	if !sqlFk.IsResolved {
		return doltdb.ForeignKey{
			Name:                   sqlFk.Name,
			TableName:              doltdb.TableName{Name: sqlFk.Table, Schema: sqlFk.SchemaName},
			TableIndex:             "",
			TableColumns:           nil,
			ReferencedTableName:    doltdb.TableName{Name: sqlFk.ParentTable, Schema: sqlFk.ParentSchema},
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
		refTbl, _, ok, err = doltdb.GetTableInsensitive(ctx, root, doltdb.TableName{Name: sqlFk.ParentTable, Schema: sqlFk.ParentSchema})
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
	tableIndex, ok, err := FindIndexWithPrefix(t.sch, sqlFk.Columns)
	if err != nil {
		return doltdb.ForeignKey{}, err
	}
	// Use secondary index if found; otherwise it will use empty string, indicating primary key
	if ok {
		tableIndexName = tableIndex.Name()
	}
	refTableIndex, ok, err := FindIndexWithPrefix(refSch, sqlFk.ParentColumns)
	if err != nil {
		return doltdb.ForeignKey{}, err
	}
	// Use secondary index if found; otherwise it will use  empty string, indicating primary key
	if ok {
		refTableIndexName = refTableIndex.Name()
	}

	return doltdb.ForeignKey{
		Name:                   sqlFk.Name,
		TableName:              doltdb.TableName{Name: sqlFk.Table, Schema: t.db.SchemaName()},
		TableIndex:             tableIndexName,
		TableColumns:           colTags,
		ReferencedTableName:    doltdb.TableName{Name: sqlFk.ParentTable, Schema: sqlFk.ParentSchema},
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
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
		return err
	}
	// empty string foreign key names are replaced with a generated name elsewhere
	if sqlFk.Name != "" && !doltdb.IsValidIdentifier(sqlFk.Name) {
		return fmt.Errorf("invalid foreign key name `%s`", sqlFk.Name)
	}
	if !strings.EqualFold(sqlFk.Database, sqlFk.ParentDatabase) || !strings.EqualFold(sqlFk.Database, t.db.Name()) {
		return fmt.Errorf("only foreign keys on the same database are currently supported")
	}
	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}
	tbl, err := t.DoltTable.DoltTable(ctx)
	if err != nil {
		return err
	}
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
		return err
	}
	// empty string foreign key names are replaced with a generated name elsewhere
	if sqlFk.Name != "" && !doltdb.IsValidIdentifier(sqlFk.Name) {
		return fmt.Errorf("invalid foreign key name `%s`", sqlFk.Name)
	}

	if strings.ToLower(sqlFk.Database) != strings.ToLower(sqlFk.ParentDatabase) || strings.ToLower(sqlFk.Database) != strings.ToLower(t.db.Name()) {
		return fmt.Errorf("only foreign keys on the same database are currently supported")
	}

	onUpdateRefAction, err := ParseFkReferentialAction(sqlFk.OnUpdate)
	if err != nil {
		return err
	}
	onDeleteRefAction, err := ParseFkReferentialAction(sqlFk.OnDelete)
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
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
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
// This interface really belongs on AlterableDoltTable (which embeds WritableDoltTable), but it's here because we
// actually have a WritableDoltTable at runtime in some cases when we want to update a foreign key. This happens in the
// case when a foreign key is created without foreign key checks on, which causes its IsResolved flag to become enabled
// the first time it is referenced in a statement with foreign keys enabled. This is kind of terrible, as means that
// an update statement (including a no-op write statement) has the side-effect of causing a schema change.
// TODO: get rid of explicit IsResolved tracking
func (t *WritableDoltTable) UpdateForeignKey(ctx *sql.Context, fkName string, sqlFk sql.ForeignKeyConstraint) error {
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
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
	doltFk.Name = sqlFk.Name

	// TODO: need schema name in foreign key defn
	doltFk.TableName = doltdb.TableName{Name: sqlFk.Table, Schema: t.db.SchemaName()}
	doltFk.ReferencedTableName = doltdb.TableName{Name: sqlFk.ParentTable, Schema: t.db.SchemaName()}
	doltFk.UnresolvedFKDetails.TableColumns = sqlFk.Columns
	doltFk.UnresolvedFKDetails.ReferencedTableColumns = sqlFk.ParentColumns

	if !doltFk.IsResolved() || !sqlFk.IsResolved {
		tbl, _, ok, err := doltdb.GetTableInsensitive(ctx, root, t.TableName())
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
	if idx.Constraint != sql.IndexConstraint_None && idx.Constraint != sql.IndexConstraint_Unique && idx.Constraint != sql.IndexConstraint_Spatial {
		return fmt.Errorf("only the following types of index constraints are supported for foreign keys: none, unique, spatial")
	}
	columns := make([]string, len(idx.Columns))
	for i, indexCol := range idx.Columns {
		columns[i] = indexCol.Name
	}

	table, err := t.DoltTable.DoltTable(ctx)
	if err != nil {
		return err
	}

	ret, err := creation.CreateIndex(ctx, table, t.Name(), idx.Name, columns, allocatePrefixLengths(idx.Columns), schema.IndexProperties{
		IsUnique:      idx.Constraint == sql.IndexConstraint_Unique,
		IsSpatial:     idx.Constraint == sql.IndexConstraint_Spatial,
		IsFullText:    false,
		IsVector:      false,
		IsUserDefined: false,
		Comment:       "",
	}, t.opts)
	if err != nil {
		return err
	}
	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}

	newRoot, err := root.PutTable(ctx, t.TableName(), ret.NewTable)
	if err != nil {
		return err
	}

	err = t.setRoot(ctx, newRoot)

	if err != nil {
		return err
	}
	return t.updateFromRoot(ctx, newRoot)
}

// toForeignKeyConstraint converts a Dolt resolved foreign key to a GMS foreign key. If the key is unresolved, then this
// function should not be used.
func toForeignKeyConstraint(fk doltdb.ForeignKey, dbName string, childSch, parentSch schema.Schema) (cst sql.ForeignKeyConstraint, err error) {
	cst = sql.ForeignKeyConstraint{
		Name:           fk.Name,
		Database:       dbName,
		SchemaName:     fk.TableName.Schema,
		Table:          fk.TableName.Name,
		Columns:        make([]string, len(fk.TableColumns)),
		ParentDatabase: dbName,
		ParentSchema:   fk.ReferencedTableName.Schema,
		ParentTable:    fk.ReferencedTableName.Name,
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

func ParseFkReferentialAction(refOp sql.ForeignKeyReferentialAction) (doltdb.ForeignKeyReferentialAction, error) {
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
		newIdx, ok, err := FindIndexWithPrefix(t.sch, fkParentCols)
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
func (t *WritableDoltTable) updateFromRoot(ctx *sql.Context, root doltdb.RootValue) error {
	updatedTableSql, ok, err := t.db.getTable(ctx, root, t.tableName)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("table `%s` cannot find itself", t.tableName)
	}
	var updatedTable *AlterableDoltTable
	if doltdb.IsSystemTable(t.TableName()) && !doltdb.IsReadOnlySystemTable(t.TableName()) && !doltdb.IsDoltCITable(t.tableName) {
		updatedTable = &AlterableDoltTable{*updatedTableSql.(*WritableDoltTable)}
	} else {
		updatedTable = updatedTableSql.(*AlterableDoltTable)
	}
	t.DoltTable = updatedTable.WritableDoltTable.DoltTable

	// When we update this table we need to also clear any cached versions of the object, since they may now have
	// incorrect schema information
	sess := dsess.DSessFromSess(ctx.Session)
	dbState, ok, err := sess.LookupDbState(ctx, t.db.RevisionQualifiedName())
	if !ok {
		return fmt.Errorf("no db state found for %s", t.db.RevisionQualifiedName())
	}

	dbState.SessionCache().ClearTableCache()

	return nil
}

func (t *AlterableDoltTable) CreateCheck(ctx *sql.Context, check *sql.CheckDefinition) error {
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
		return err
	}
	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}

	updatedTable, _, err := root.GetTable(ctx, t.TableName())
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

	newRoot, err := root.PutTable(ctx, t.TableName(), newTable)
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
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
		return err
	}
	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}

	updatedTable, _, err := root.GetTable(ctx, t.TableName())
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

	newRoot, err := root.PutTable(ctx, t.TableName(), newTable)
	if err != nil {
		return err
	}

	err = t.setRoot(ctx, newRoot)
	if err != nil {
		return err
	}

	return t.updateFromRoot(ctx, newRoot)
}

func (t *AlterableDoltTable) ModifyStoredCollation(ctx *sql.Context, collation sql.CollationID) error {
	return fmt.Errorf("converting the collations of columns is not yet supported")
}

func (t *AlterableDoltTable) ModifyDefaultCollation(ctx *sql.Context, collation sql.CollationID) error {
	if err := dsess.CheckAccessForDb(ctx, t.db, branch_control.Permissions_Write); err != nil {
		return err
	}
	root, err := t.getRoot(ctx)
	if err != nil {
		return err
	}
	currentTable, _, err := root.GetTable(ctx, t.TableName())
	if err != nil {
		return err
	}
	sch, err := currentTable.GetSchema(ctx)
	if err != nil {
		return err
	}

	sch.SetCollation(schema.Collation(collation))

	table, err := t.DoltTable.DoltTable(ctx)
	if err != nil {
		return err
	}
	newTable, err := table.UpdateSchema(ctx, sch)
	if err != nil {
		return err
	}
	newRoot, err := root.PutTable(ctx, t.TableName(), newTable)
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

	hashedName := fmt.Sprintf("%s_chk_%s", t.tableName, hash.String()[:8])
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
		if strings.EqualFold(key.Name, name) {
			return true, nil
		}
	}

	checks, err := t.GetChecks(ctx)
	if err != nil {
		return false, err
	}

	for _, check := range checks {
		if strings.EqualFold(check.Name, name) {
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

func (t *WritableDoltTable) SetWriteSession(session dsess.WriteSession) {
	t.pinnedWriteSession = session
}

func FindIndexWithPrefix(sch schema.Schema, prefixCols []string) (schema.Index, bool, error) {
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
