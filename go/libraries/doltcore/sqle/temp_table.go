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
	"context"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/types"
)

type TempTable struct {
	tableName string
	dbName    string
	pkSch     sql.PrimaryKeySchema

	table *doltdb.Table
	sch   schema.Schema

	lookup sql.IndexLookup

	ed   dsess.TableWriter
	opts editor.Options
}

var _ sql.TemporaryTable = &TempTable{}
var _ sql.Table = &TempTable{}
var _ sql.PrimaryKeyTable = &TempTable{}
var _ sql.IndexedTable = &TempTable{}
var _ sql.IndexAlterableTable = &TempTable{}
var _ sql.ForeignKeyTable = &TempTable{}
var _ sql.CheckTable = &TempTable{}
var _ sql.CheckAlterableTable = &TempTable{}
var _ sql.StatisticsTable = &TempTable{}
var _ sql.AutoIncrementTable = &TempTable{}

func NewTempTable(
	ctx *sql.Context,
	ddb *doltdb.DoltDB,
	pkSch sql.PrimaryKeySchema,
	name, db string,
	opts editor.Options,
	collation sql.CollationID,
) (*TempTable, error) {
	sess := dsess.DSessFromSess(ctx.Session)

	dbState, ok, err := sess.LookupDbState(ctx, db)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, fmt.Errorf("database %s not found in session", db)
	}

	ws := dbState.WorkingSet()
	if ws == nil {
		return nil, doltdb.ErrOperationNotSupportedInDetachedHead
	}

	colNames := make([]string, len(pkSch.Schema))
	colKinds := make([]types.NomsKind, len(pkSch.Schema))
	for i, col := range pkSch.Schema {
		colNames[i] = col.Name
		ti, err := typeinfo.FromSqlType(col.Type)
		if err != nil {
			return nil, err
		}
		colKinds[i] = ti.NomsKind()
	}

	tags, err := doltdb.GenerateTagsForNewColumns(ctx, ws.WorkingRoot(), name, colNames, colKinds, ws.WorkingRoot())
	if err != nil {
		return nil, err
	}

	sch, err := temporaryDoltSchema(ctx, pkSch, tags, collation)
	if err != nil {
		return nil, err
	}
	vrw := ddb.ValueReadWriter()
	ns := ddb.NodeStore()

	idx, err := durable.NewEmptyIndex(ctx, vrw, ns, sch)
	if err != nil {
		return nil, err
	}
	set, err := durable.NewIndexSet(ctx, vrw, ns)
	if err != nil {
		return nil, err
	}

	tbl, err := doltdb.NewTable(ctx, vrw, ns, sch, idx, set, nil)
	if err != nil {
		return nil, err
	}

	newRoot, err := ws.WorkingRoot().PutTable(ctx, doltdb.TableName{Name: name}, tbl)
	if err != nil {
		return nil, err
	}

	newWs := ws.WithWorkingRoot(newRoot)

	ait, err := dsess.NewAutoIncrementTracker(ctx, db, newWs)
	if err != nil {
		return nil, err
	}

	writeSession := writer.NewWriteSession(tbl.Format(), newWs, ait, opts)

	tempTable := &TempTable{
		tableName: name,
		dbName:    db,
		pkSch:     pkSch,
		table:     tbl,
		sch:       sch,
		opts:      opts,
	}

	tempTable.ed, err = writeSession.GetTableWriter(ctx, doltdb.TableName{Name: name}, db, setTempTableRoot(tempTable), false)
	if err != nil {
		return nil, err
	}

	return tempTable, nil
}

func setTempTableRoot(t *TempTable) func(ctx *sql.Context, dbName string, newRoot doltdb.RootValue) error {
	return func(ctx *sql.Context, dbName string, newRoot doltdb.RootValue) error {
		newTable, _, err := newRoot.GetTable(ctx, doltdb.TableName{Name: t.tableName})
		if err != nil {
			return err
		}

		t.table = newTable

		sess := dsess.DSessFromSess(ctx.Session)

		dbState, ok, err := sess.LookupDbState(ctx, t.dbName)
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("database %s not found in session", t.dbName)
		}

		ws := dbState.WorkingSet()
		if ws == nil {
			return doltdb.ErrOperationNotSupportedInDetachedHead
		}
		newWs := ws.WithWorkingRoot(newRoot)

		ait, err := dsess.NewAutoIncrementTracker(ctx, t.dbName, newWs)
		if err != nil {
			return err
		}

		writeSession := writer.NewWriteSession(newTable.Format(), newWs, ait, t.opts)
		t.ed, err = writeSession.GetTableWriter(ctx, doltdb.TableName{Name: t.tableName}, t.dbName, setTempTableRoot(t), false)
		if err != nil {
			return err
		}

		return nil
	}
}

func (t *TempTable) RowCount(ctx *sql.Context) (uint64, bool, error) {
	rows, err := t.table.GetRowData(ctx)
	if err != nil {
		return 0, false, err
	}
	cnt, err := rows.Count()
	return cnt, true, err
}

func (t *TempTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return index.DoltIndexesFromTable(ctx, t.dbName, t.tableName, t.table)
}

func (t *TempTable) PreciseMatch() bool {
	return true
}

func (t *TempTable) Name() string {
	return t.tableName
}

func (t *TempTable) String() string {
	return t.tableName
}

func (t *TempTable) Format() *types.NomsBinFormat {
	return t.table.Format()
}

func (t *TempTable) Schema() sql.Schema {
	return t.pkSch.Schema
}

func (t *TempTable) Collation() sql.CollationID {
	return sql.CollationID(t.sch.GetCollation())
}

func (t *TempTable) sqlSchema() sql.PrimaryKeySchema {
	return t.pkSch
}

func (t *TempTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	rows, err := t.table.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	parts, err := partitionsFromRows(ctx, rows)
	if err != nil {
		return nil, err
	}
	return newDoltTablePartitionIter(rows, parts...), nil
}

func (t *TempTable) IsTemporary() bool {
	return true
}

// DataLength implements the sql.StatisticsTable interface.
func (t *TempTable) DataLength(ctx *sql.Context) (uint64, error) {
	idx, err := t.table.GetRowData(ctx)
	if err != nil {
		return 0, err
	}
	return idx.Count()
}

func (t *TempTable) DoltTable(ctx *sql.Context) (*doltdb.Table, error) {
	return t.table, nil
}

func (t *TempTable) DataCacheKey(ctx *sql.Context) (doltdb.DataCacheKey, bool, error) {
	return doltdb.DataCacheKey{}, false, nil
}

func (t *TempTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	t.lookup = lookup
	return t.Partitions(ctx)
}

func (t *TempTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	if !t.lookup.IsEmpty() {
		return index.RowIterForIndexLookup(ctx, t, t.lookup, t.pkSch, nil)
	} else {
		return partitionRows(ctx, t.table, nil, partition)
	}
}

func (t *TempTable) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	return t
}

func (t *TempTable) CreateIndex(ctx *sql.Context, idx sql.IndexDef) error {
	if idx.Constraint != sql.IndexConstraint_None && idx.Constraint != sql.IndexConstraint_Unique && idx.Constraint != sql.IndexConstraint_Spatial {
		return fmt.Errorf("only the following types of index constraints are supported: none, unique, spatial")
	}
	cols := make([]string, len(idx.Columns))
	for i, c := range idx.Columns {
		cols[i] = c.Name
	}

	ret, err := creation.CreateIndex(ctx, t.table, t.Name(), idx.Name, cols, allocatePrefixLengths(idx.Columns), schema.IndexProperties{
		IsUnique:      idx.Constraint == sql.IndexConstraint_Unique,
		IsSpatial:     idx.Constraint == sql.IndexConstraint_Spatial,
		IsFullText:    idx.Constraint == sql.IndexConstraint_Fulltext,
		IsUserDefined: true,
		Comment:       idx.Comment,
	}, t.opts)
	if err != nil {
		return err
	}

	t.table = ret.NewTable
	return nil
}

func (t *TempTable) DropIndex(ctx *sql.Context, indexName string) error {
	_, err := t.sch.Indexes().RemoveIndex(indexName)
	if err != nil {
		return err
	}

	newTable, err := t.table.UpdateSchema(ctx, t.sch)
	if err != nil {
		return err
	}
	newTable, err = newTable.DeleteIndexRowData(ctx, indexName)
	if err != nil {
		return err
	}
	t.table = newTable

	return nil
}

func (t *TempTable) RenameIndex(ctx *sql.Context, fromIndexName string, toIndexName string) error {
	_, err := t.sch.Indexes().RenameIndex(fromIndexName, toIndexName)
	if err != nil {
		return err
	}

	newTable, err := t.table.UpdateSchema(ctx, t.sch)
	if err != nil {
		return err
	}
	newTable, err = newTable.RenameIndexRowData(ctx, fromIndexName, toIndexName)
	if err != nil {
		return err
	}
	t.table = newTable

	return nil
}

func (t *TempTable) GetDeclaredForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	return nil, nil
}

func (t *TempTable) GetReferencedForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	return nil, nil
}

func (t *TempTable) CreateIndexForForeignKey(ctx *sql.Context, idx sql.IndexDef) error {
	return sql.ErrTemporaryTablesForeignKeySupport.New()
}

func (t *TempTable) AddForeignKey(ctx *sql.Context, fk sql.ForeignKeyConstraint) error {
	return sql.ErrTemporaryTablesForeignKeySupport.New()
}

func (t *TempTable) UpdateForeignKey(ctx *sql.Context, fkName string, fk sql.ForeignKeyConstraint) error {
	return sql.ErrTemporaryTablesForeignKeySupport.New()
}

func (t *TempTable) DropForeignKey(ctx *sql.Context, fkName string) error {
	return sql.ErrTemporaryTablesForeignKeySupport.New()
}

func (t *TempTable) GetForeignKeyEditor(ctx *sql.Context) sql.ForeignKeyEditor {
	return nil
}

func (t *TempTable) Inserter(*sql.Context) sql.RowInserter {
	return t
}

func (t *TempTable) Deleter(*sql.Context) sql.RowDeleter {
	return t
}

func (t *TempTable) Replacer(*sql.Context) sql.RowReplacer {
	return t
}

func (t *TempTable) Updater(*sql.Context) sql.RowUpdater {
	return t
}

func (t *TempTable) GetChecks(*sql.Context) ([]sql.CheckDefinition, error) {
	return checksInSchema(t.sch), nil
}

func (t *TempTable) PrimaryKeySchema() sql.PrimaryKeySchema {
	return t.pkSch
}

func (t *TempTable) CreateCheck(ctx *sql.Context, check *sql.CheckDefinition) error {
	sch, err := t.table.GetSchema(ctx)
	if err != nil {
		return err
	}

	check = &(*check)
	if check.Name == "" {
		check.Name = strconv.Itoa(rand.Int())
	}

	_, err = sch.Checks().AddCheck(check.Name, check.CheckExpression, check.Enforced)
	if err != nil {
		return err
	}
	t.table, err = t.table.UpdateSchema(ctx, sch)

	return err
}

func (t *TempTable) DropCheck(ctx *sql.Context, chName string) error {
	err := t.sch.Checks().DropCheck(chName)
	if err != nil {
		return err
	}
	t.table, err = t.table.UpdateSchema(ctx, t.sch)

	return err
}

func (t *TempTable) Insert(ctx *sql.Context, sqlRow sql.Row) error {
	return t.ed.Insert(ctx, sqlRow)
}

func (t *TempTable) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	return t.ed.Update(ctx, oldRow, newRow)
}

func (t *TempTable) Delete(ctx *sql.Context, sqlRow sql.Row) error {
	return t.ed.Delete(ctx, sqlRow)
}

func (t *TempTable) StatementBegin(ctx *sql.Context) {
	return
}

func (t *TempTable) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	t.lookup = sql.IndexLookup{}
	return nil
}

func (t *TempTable) StatementComplete(ctx *sql.Context) error {
	t.lookup = sql.IndexLookup{}
	return nil
}

func (t *TempTable) Close(ctx *sql.Context) error {
	err := t.ed.Close(ctx)

	t.lookup = sql.IndexLookup{}
	return err
}

func temporaryDoltSchema(ctx context.Context, pkSch sql.PrimaryKeySchema, tags []uint64, collation sql.CollationID) (sch schema.Schema, err error) {
	cols := make([]schema.Column, len(pkSch.Schema))
	for i, col := range pkSch.Schema {
		cols[i], err = sqlutil.ToDoltCol(tags[i], col)
		if err != nil {
			return nil, err
		}
	}

	sch, err = schema.SchemaFromCols(schema.NewColCollection(cols...))
	if err != nil {
		return nil, err
	}

	err = sch.SetPkOrdinals(pkSch.PkOrdinals)
	if err != nil {
		return nil, err
	}
	sch.SetCollation(schema.Collation(collation))

	return sch, nil
}

func (t *TempTable) PeekNextAutoIncrementValue(ctx *sql.Context) (uint64, error) {
	return t.table.GetAutoIncrementValue(ctx)
}

func (t *TempTable) GetNextAutoIncrementValue(ctx *sql.Context, insertVal interface{}) (uint64, error) {
	autoIncEditor, ok := t.ed.(writer.AutoIncrementGetter)
	if !ok {
		return 0, sql.ErrNoAutoIncrementCol
	}
	return autoIncEditor.GetNextAutoIncrementValue(ctx, insertVal)
}

func (t *TempTable) AutoIncrementSetter(ctx *sql.Context) sql.AutoIncrementSetter {
	return t.ed
}
