package adapters

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

// TableAdapter provides a hook for extensions to customize or wrap table implementations. For example, this allows
// libraries like Doltgres to intercept system table creation and apply type conversions, schema modifications, or other
// customizations without modifying the core Dolt implementation for their compatibility.
type TableAdapter interface {
	// CreateTable creates or wraps a system table. The function receives all necessary parameters to construct the
	// table and can either build it from scratch or call the default Dolt constructor and wrap it.
	CreateTable(ctx *sql.Context, tableName string, dDb *doltdb.DoltDB, workingSet *doltdb.WorkingSet, rootsProvider env.RootsProvider[*sql.Context]) sql.Table

	// TableName returns the preferred name for the adapter's table. This allows extensions to rename tables while
	// preserving the underlying implementation. For example, Doltgres uses "status" while Dolt uses "dolt_status",
	// enabling cleaner Postgres-style naming.
	TableName() string
}

// TableAdapters is a registry for TableAdapter implementations, keyed by table name. It is populated during package
// initialization and intended to be read-only thereafter.
var TableAdapters = make(map[string]TableAdapter)
