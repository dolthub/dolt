package sql

// Aliases tracks the aliased identifiers in a query
// TODO: these should be case insensitive
type Aliases struct {
	// Map from table alias to table name
	TablesByAlias map[string]string
	// Map from table name to aliases
	AliasesByTable map[string][]string
	// Column aliases by their name. Multiple columns can share the same alias.
	ColumnAliases map[string][]*RowValGetter
}

// A qualified column names its table and its column.
// Both names are case-sensitive and match the name as defined in the schema.
type QualifiedColumn struct {
	TableName  string
	ColumnName string
}

func NewAliases() *Aliases {
	return &Aliases{
		TablesByAlias:  make(map[string]string),
		AliasesByTable: make(map[string][]string),
	}
}

// Returns a copy of the aliases with only table aliases filled in.`
func (a *Aliases) TableAliasesOnly() *Aliases {
	return &Aliases{
		TablesByAlias:  a.TablesByAlias,
		AliasesByTable: a.AliasesByTable,
		ColumnAliases:  make(map[string][]*RowValGetter),
	}
}

// Adds a table alias as specified. Returns an error if the alias already exists
func (a *Aliases) AddTableAlias(tableName, alias string) error {
	if _, ok := a.TablesByAlias[alias]; ok {
		return errFmt("Duplicate table alias: '%v'", alias)
	}
	a.TablesByAlias[alias] = tableName
	a.AliasesByTable[tableName] = append(a.AliasesByTable[tableName], alias)
	return nil
}

// Returns whether the two columns given are equal
func ColumnsEqual(c1, c2 QualifiedColumn) bool {
	return c1.TableName == c2.TableName && c1.ColumnName == c2.ColumnName
}
