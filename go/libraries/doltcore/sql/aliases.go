package sql

// Aliases tracks the aliased identifiers in a query
type Aliases struct {
	// DestRef from table alias to table name
	TablesByAlias map[string]string
	// DestRef from table alias to table name
	AliasesByTable map[string]string
	// DestRef from column alias to column name in original schema
	ColumnsByAlias map[string]QualifiedColumn
	// DestRef from column name to column alias
	AliasesByColumn map[QualifiedColumn]string
}

// A single result set can have many columns that share the same name
type QualifiedColumn struct {
	TableName  string
	ColumnName string
}

func NewAliases() *Aliases {
	return &Aliases{
		TablesByAlias:   make(map[string]string),
		AliasesByTable:  make(map[string]string),
		ColumnsByAlias:  make(map[string]QualifiedColumn),
		AliasesByColumn: make(map[QualifiedColumn]string),
	}
}

// Adds a column alias as specified. Silently overwrites existing entries.
func (a *Aliases) AddColumnAlias(tableName, colName, alias string) {
	qc := QualifiedColumn{TableName: tableName, ColumnName: colName}
	a.AliasesByColumn[qc] = alias
	a.ColumnsByAlias[alias] = qc
}

// Adds a table alias as specified. Silently overwrites existing entries.
func (a *Aliases) AddTableAlias(tableName, alias string) {
	a.AliasesByTable[tableName] = alias
	a.TablesByAlias[alias] = tableName
}

// Returns whether the two columns given are equal
func AreColumnsEqual(c1, c2 QualifiedColumn) bool {
	return c1.TableName == c2.TableName && c1.ColumnName == c2.ColumnName
}
