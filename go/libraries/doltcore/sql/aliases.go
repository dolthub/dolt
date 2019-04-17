package sql

// Aliases tracks the aliased identifiers in a query
type Aliases struct {
	// Map from table alias to table name
	TablesByAlias map[string]string
	// Map from table alias to table name
	AliasesByTable map[string]string
	// Map from column alias to column name in original schema
	ColumnsByAlias map[string]string
	// Map from column name to column alias
	AliasesByColumn map[string]string
}

func NewAliases() *Aliases {
	return &Aliases{
		TablesByAlias: make(map[string]string),
		AliasesByTable: make(map[string]string),
		ColumnsByAlias: make(map[string]string),
		AliasesByColumn: make(map[string]string),
	}
}

// Adds a column alias as specified. Silently overwrites existing entries.
func (a *Aliases) AddColumnAlias(colName, alias string) {
	a.AliasesByColumn[colName] = alias
	a.ColumnsByAlias[alias] = colName
}

// Adds a table alias as specified. Silently overwrites existing entries.
func (a *Aliases) AddTableAlias(tableName, alias string) {
	a.AliasesByTable[tableName] = alias
	a.TablesByAlias[alias] = tableName
}