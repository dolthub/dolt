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

var DoltTableAdapterRegistry = newDoltTableAdapterRegistry()

// doltTableAdapterRegistry is a TableAdapter registry for Dolt tables, keyed by table name. It is populated during
// package initialization (in the Dolt table source) and intended to be read-only thereafter.
type doltTableAdapterRegistry struct {
	Adapters       map[string]TableAdapter
	adapterAliases map[string]string
}

// newDoltTableAdapterRegistry constructs Dolt table adapter registry with empty alias and adapter maps.
func newDoltTableAdapterRegistry() *doltTableAdapterRegistry {
	return &doltTableAdapterRegistry{
		Adapters:       make(map[string]TableAdapter),
		adapterAliases: make(map[string]string),
	}
}

// AddAdapter adds a TableAdapter to the Dolt table adapter registry with optional |aliases| (alternative table name
// keys). An alias cannot exist as both an adapter and alias, if you provide an alias that is already in the adapter
// map, it will be dropped. This overriding behavior is typically used by integrators, i.e. Doltgres, to replace the
// original Dolt table.
func (as *doltTableAdapterRegistry) AddAdapter(tableName string, adapter TableAdapter, aliases ...string) {
	for _, alias := range aliases {
		as.adapterAliases[alias] = tableName
		if _, ok := as.Adapters[alias]; ok {
			delete(as.Adapters, alias) // We don't want this to show up in the catalog.
		}
	}
	as.adapterAliases[tableName] = tableName
	as.Adapters[tableName] = adapter
}

// GetAdapter gets a Dolt TableAdapter mapped to |name|, which can be an alias or the table name.
func (as *doltTableAdapterRegistry) GetAdapter(name string) TableAdapter {
	name = as.adapterAliases[name]
	return as.Adapters[name]
}

// GetTableName gets the Dolt table name mapped to |name|, which can be an alias or the table name.
func (as *doltTableAdapterRegistry) GetTableName(name string) string {
	return as.adapterAliases[name]
}
