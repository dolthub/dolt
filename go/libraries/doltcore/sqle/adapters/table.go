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
	// NewTable creates or wraps a system table. The function receives all necessary parameters to construct the table
	// and can either build it from scratch or call the default Dolt constructor and wrap it.
	NewTable(ctx *sql.Context, tableName string, dDb *doltdb.DoltDB, workingSet *doltdb.WorkingSet, rootsProvider env.RootsProvider[*sql.Context]) sql.Table

	// TableName returns the preferred name for the adapter's table. This allows extensions to rename tables while
	// preserving the underlying implementation. For example, Doltgres uses "status" while Dolt uses "dolt_status",
	// enabling cleaner Postgres-style naming.
	TableName() string
}

var DoltTableAdapterRegistry = newDoltTableAdapterRegistry()

// doltTableAdapterRegistry is a Dolt table name to TableAdapter map. Integrators populate this registry during package
// initialization, and it's intended to be read-only thereafter. The registry links with existing Dolt system tables to
// allow them to be resolved and evaluated to integrator's version and internal aliases (integrators' Dolt table name
// keys).
type doltTableAdapterRegistry struct {
	Adapters        map[string]TableAdapter
	internalAliases map[string]string
}

// newDoltTableAdapterRegistry constructs Dolt table adapter registry with empty internal alias and adapter maps.
func newDoltTableAdapterRegistry() *doltTableAdapterRegistry {
	return &doltTableAdapterRegistry{
		Adapters:        make(map[string]TableAdapter),
		internalAliases: make(map[string]string),
	}
}

// AddAdapter maps |doltTableName| to an |adapter| in the Dolt table adapter registry, with optional |internalAliases|.
func (as *doltTableAdapterRegistry) AddAdapter(doltTableName string, adapter TableAdapter, internalAliases ...string) {
	for _, alias := range internalAliases {
		as.internalAliases[alias] = doltTableName
	}
	as.Adapters[doltTableName] = adapter
}

// GetAdapter gets a Dolt TableAdapter mapped to |name|, which can be the dolt table name or internal alias.
func (as *doltTableAdapterRegistry) GetAdapter(name string) (TableAdapter, bool) {
	adapter, ok := as.Adapters[name]
	if !ok {
		name = as.internalAliases[name]
		adapter, ok = as.Adapters[name]
	}

	return adapter, ok
}

// NormalizeName normalizes |name| if it's an internal alias of the underlying Dolt table name. If no match is found,
// |name| is returned as-is.
func (as *doltTableAdapterRegistry) NormalizeName(name string) string {
	doltTableName, ok := as.internalAliases[name]
	if !ok {
		return name
	}

	return doltTableName
}
