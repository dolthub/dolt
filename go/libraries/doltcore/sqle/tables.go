package sqle

import (
	"context"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/src-d/go-mysql-server/sql"
	"io"
)

type Database struct {
	name string
	dEnv *env.DoltEnv
}

type DoltTable struct {
	name  string
	table *doltdb.Table
	sch   schema.Schema
}

func (t *DoltTable) Name() string {
	return t.name
}

func (t *DoltTable) String() string {
	return ""
}

func (t *DoltTable) Schema() sql.Schema {
	schema := t.table.GetSchema(context.TODO())
	return doltSchemaToSqlSchema(t.name, schema)
}

type doltTablePartitionIter struct {
	table *DoltTable
	i int
}

func (itr *doltTablePartitionIter) Close() error {
	return nil
}

func (itr *doltTablePartitionIter) Next() (sql.Partition, error) {
	if itr.i > 0 {
		return nil, io.EOF
	}
	itr.i++

	return &doltTablePartition{itr.table}, nil
}

type doltTablePartition struct {
	table *DoltTable
}

func (p doltTablePartition) Key() []byte {
	return []byte(p.table.name)
}

// Returns the partitions for this table. We return a single partition, but could potentially get more performance by
// returning multiple.
func (t *DoltTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return &doltTablePartitionIter{table: t}, nil
}

// Returns the table rows for the partition given.
func (t *DoltTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	return newRowIterator(t, ctx), nil
}

type doltTableRowIter struct {
	table *DoltTable
	rowData types.Map
	ctx *sql.Context
	nomsIter types.MapIterator
}

func newRowIterator(tbl *DoltTable, ctx *sql.Context) *doltTableRowIter {
	rowData := tbl.table.GetRowData(ctx.Context)
	mapIter := rowData.Iterator(ctx.Context)
	return &doltTableRowIter{table: tbl, rowData: rowData, ctx: ctx, nomsIter: mapIter}
}

func (itr *doltTableRowIter) Next() (sql.Row, error) {
	key, val := itr.nomsIter.Next(itr.ctx.Context)
	if key == nil && val == nil {
		return nil, io.EOF
	}

	doltRow := row.FromNoms(itr.table.sch, key.(types.Tuple), val.(types.Tuple))
	return doltRowToSqlRow(doltRow, itr.table.sch), nil
}

func doltRowToSqlRow(doltRow row.Row, sch schema.Schema) sql.Row {
	colVals := make(sql.Row, sch.GetAllCols().Size())

	i := 0
	sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		value, _:= doltRow.GetColVal(tag)
		colVals[i] = doltColValToSqlColVal(value)
		i++
		return false
	})

	return sql.NewRow(colVals)
}

func doltColValToSqlColVal(val types.Value) interface{} {
	if types.IsNull(val) {
		return nil
	}

	return nomsValToSqlVal(val)
}

func (itr *doltTableRowIter) Close() error {
	return nil
}

func NewDatabase(name string, dEnv *env.DoltEnv) sql.Database {
	return &Database{
		name: name,
		dEnv: dEnv,
	}
}

func (db *Database) Name() string {
	return db.name
}

func (db *Database) Tables() map[string]sql.Table {
	ctx := context.TODO()
	root, err := db.dEnv.WorkingRoot(ctx)
	if err != nil {
		panic(err)
	}

	tables := make(map[string]sql.Table)
	tableNames := root.GetTableNames(ctx)
	for _, name := range tableNames {
		table, ok := root.GetTable(ctx, name)
		if !ok {
			panic("Error loading table " + name)
		}
		tables[name] = &DoltTable{name: name, table: table, sch: table.GetSchema(context.TODO())}
	}

	return tables
}

