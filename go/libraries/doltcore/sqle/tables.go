package sqle

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/src-d/go-mysql-server/sql"
)

type Database struct {
	name string
	dEnv *env.DoltEnv
}

type DoltTable struct {
	name string
	table *doltdb.Table
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

func (*DoltTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	panic("implement me")
}

func (*DoltTable) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	panic("implement me")
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
		tables[name] = &DoltTable{name: name, table: table}
	}

	return tables
}

