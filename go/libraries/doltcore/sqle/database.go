package sqle

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/src-d/go-mysql-server/sql"
)

// Database implements sql.Database for a dolt DB.
type Database struct {
	sql.Database
	name string
	dEnv *env.DoltEnv
}

// NewDatabase returns a new dolt databae to use in queries.
func NewDatabase(name string, dEnv *env.DoltEnv) sql.Database {
	return &Database{
		name: name,
		dEnv: dEnv,
	}
}

// Name returns the name of this database, set at creation time.
func (db *Database) Name() string {
	return db.name
}

// Tables returns the tables in this database, currently exactly the same tables as in the current working root.
func (db *Database) Tables() map[string]sql.Table {
	ctx := context.Background()
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
		tables[name] = &DoltTable{name: name, table: table, sch: table.GetSchema(ctx)}
	}

	return tables
}