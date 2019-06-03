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

func NewAliases() *Aliases {
	return &Aliases{
		TablesByAlias:  make(map[string]string),
		AliasesByTable: make(map[string][]string),
		ColumnAliases:  make(map[string][]*RowValGetter),
	}
}

// Returns a copy of the aliases with only table aliases filled in.
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

func (a *Aliases) AddColumnAlias(alias string, getter *RowValGetter) {
	a.ColumnAliases[alias] = append(a.ColumnAliases[alias], getter)
}