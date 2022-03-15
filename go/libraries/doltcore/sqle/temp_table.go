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
	"math/rand"
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

func NewTempTable(
	ctx context.Context,
	ddb *doltdb.DoltDB,
	pkSch sql.PrimaryKeySchema,
	name, db string,
	opts editor.Options,
) (*TempTable, error) {
	sch, err := temporaryDoltSchema(ctx, pkSch)
	if err != nil {
		return nil, err
	}
	vrw := ddb.ValueReadWriter()

	idx, err := durable.NewEmptyIndex(ctx, vrw, sch)
	if err != nil {
		return nil, err
	}
	set := durable.NewIndexSet(ctx, vrw)

	tbl, err := doltdb.NewTable(ctx, ddb.ValueReadWriter(), sch, idx, set, nil)
	if err != nil {
		return nil, err
	}

	ed, err := editor.NewTableEditor(ctx, tbl, sch, name, opts)
	if err != nil {
		return nil, err
	}

	return &TempTable{
		tableName: name,
		dbName:    db,
		pkSch:     pkSch,
		table:     tbl,
		sch:       sch,
		ed:        ed,
		opts:      opts,
	}, nil
}

type TempTable struct {
	tableName string
	dbName    string
	pkSch     sql.PrimaryKeySchema

	table *doltdb.Table
	sch   schema.Schema

	lookup sql.IndexLookup

	ed   editor.TableEditor
	opts editor.Options
}

var _ sql.TemporaryTable = &TempTable{}
var _ sql.Table = &TempTable{}
var _ sql.PrimaryKeyTable = &TempTable{}
var _ sql.IndexedTable = &TempTable{}
var _ sql.CheckTable = &TempTable{}
var _ sql.CheckAlterableTable = &TempTable{}
var _ sql.StatisticsTable = &TempTable{}

func (t *TempTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return index.DoltIndexesFromTable(ctx, t.dbName, t.tableName, t.table)
}

func (t *TempTable) Name() string {
	return t.tableName
}

func (t *TempTable) String() string {
	return t.tableName
}

func (t *TempTable) NumRows(ctx *sql.Context) (uint64, error) {
	m, err := t.table.GetRowData(ctx)
	if err != nil {
		return 0, err
	}
	return m.Count(), nil
}

func (t *TempTable) Format() *types.NomsBinFormat {
	return t.table.Format()
}

func (t *TempTable) Schema() sql.Schema {
	return t.pkSch.Schema
}

func (t *TempTable) sqlSchema() sql.PrimaryKeySchema {
	return t.pkSch
}

func (t *TempTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	rows, err := t.table.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	return newDoltTablePartitionIter(rows, partitionsFromRows(ctx, rows)...), nil
}

func (t *TempTable) IsTemporary() bool {
	return true
}

func (t *TempTable) DataLength(ctx *sql.Context) (uint64, error) {
	idx, err := t.table.GetRowData(ctx)
	if err != nil {
		return 0, err
	}
	return idx.Count(), nil
}

func (t *TempTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	if t.lookup != nil {
		return index.RowIterForIndexLookup(ctx, t.lookup, t.pkSch, nil)
	} else {
		return partitionRows(ctx, t.table, nil, partition)
	}
}

func (t *TempTable) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	t.lookup = lookup
	return t
}

func (t *TempTable) Inserter(ctx *sql.Context) sql.RowInserter {
	return TempTableWriter{
		ed:  t.ed,
		sch: t.sch,
		vrw: t.table.ValueReadWriter(),
	}
}

func (t *TempTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	return TempTableWriter{
		ed:  t.ed,
		sch: t.sch,
		vrw: t.table.ValueReadWriter(),
	}
}

func (t *TempTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return TempTableWriter{
		ed:  t.ed,
		sch: t.sch,
		vrw: t.table.ValueReadWriter(),
	}
}

func (t *TempTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return TempTableWriter{
		ed:  t.ed,
		sch: t.sch,
		vrw: t.table.ValueReadWriter(),
	}
}

func (t *TempTable) GetChecks(ctx *sql.Context) ([]sql.CheckDefinition, error) {
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

type TempTableWriter struct {
	ed  editor.TableEditor
	sch schema.Schema
	vrw types.ValueReadWriter
}

func (wr TempTableWriter) Insert(ctx *sql.Context, sqlRow sql.Row) error {
	if !schema.IsKeyless(wr.sch) {
		k, v, tagToVal, err := sqlutil.DoltKeyValueAndMappingFromSqlRow(ctx, wr.vrw, sqlRow, wr.sch)
		if err != nil {
			return err
		}
		return wr.ed.InsertKeyVal(ctx, k, v, tagToVal, wr.duplicateKeyErrFunc)
	}
	dRow, err := sqlutil.SqlRowToDoltRow(ctx, wr.vrw, sqlRow, wr.sch)
	if err != nil {
		return err
	}
	return wr.ed.InsertRow(ctx, dRow, wr.duplicateKeyErrFunc)
}

func (wr TempTableWriter) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	dOldRow, err := sqlutil.SqlRowToDoltRow(ctx, wr.vrw, oldRow, wr.sch)
	if err != nil {
		return err
	}
	dNewRow, err := sqlutil.SqlRowToDoltRow(ctx, wr.vrw, newRow, wr.sch)
	if err != nil {
		return err
	}

	return wr.ed.UpdateRow(ctx, dOldRow, dNewRow, wr.duplicateKeyErrFunc)
}

func (wr TempTableWriter) Delete(ctx *sql.Context, sqlRow sql.Row) error {
	if !schema.IsKeyless(wr.sch) {
		k, tagToVal, err := sqlutil.DoltKeyAndMappingFromSqlRow(ctx, wr.vrw, sqlRow, wr.sch)
		if err != nil {
			return err
		}

		return wr.ed.DeleteByKey(ctx, k, tagToVal)
	} else {
		dRow, err := sqlutil.SqlRowToDoltRow(ctx, wr.vrw, sqlRow, wr.sch)
		if err != nil {
			return err
		}
		return wr.ed.DeleteRow(ctx, dRow)
	}
}

func (wr TempTableWriter) duplicateKeyErrFunc(keyString, indexName string, k, v types.Tuple, isPk bool) error {
	// todo: improve error msg
	return sql.NewUniqueKeyErr(keyString, isPk, nil)
}

func (wr TempTableWriter) StatementBegin(ctx *sql.Context) {
	return
}

func (wr TempTableWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return nil
}

func (wr TempTableWriter) StatementComplete(ctx *sql.Context) error {
	return nil
}

func (wr TempTableWriter) Close(ctx *sql.Context) error {
	return wr.ed.Close(ctx)
}

func temporaryDoltSchema(ctx context.Context, pkSch sql.PrimaryKeySchema) (sch schema.Schema, err error) {
	cols := make([]schema.Column, len(pkSch.Schema))
	for i, col := range pkSch.Schema {
		tag := uint64(i)
		cols[i], err = sqlutil.ToDoltCol(tag, col)
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

	return sch, nil
}
