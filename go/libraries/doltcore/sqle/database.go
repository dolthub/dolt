package sqle

import (
	"context"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
)

// Database implements sql.Database for a dolt DB.
type Database struct {
	sql.Database
	name string
	root *doltdb.RootValue
}

// NewDatabase returns a new dolt databae to use in queries.
func NewDatabase(name string, root *doltdb.RootValue) *Database {
	return &Database{
		name: name,
		root: root,
	}
}

// Name returns the name of this database, set at creation time.
func (db *Database) Name() string {
	return db.name
}

// Tables returns the tables in this database, currently exactly the same tables as in the current working root.
func (db *Database) Tables() map[string]sql.Table {
	ctx := context.Background()

	tables := make(map[string]sql.Table)
	tableNames := db.root.GetTableNames(ctx)
	for _, name := range tableNames {
		table, ok := db.root.GetTable(ctx, name)
		if !ok {
			panic("Error loading table " + name)
		}
		tables[name] = &DoltTable{name: name, table: table, sch: table.GetSchema(ctx)}
	}

	return tables
}
