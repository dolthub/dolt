package sql

import (
	"strings"
)

// Aliases tracks the aliased identifiers in a query
type Aliases struct {
	// Table aliases by their name. Table aliases must be distinct and are case-insensitive
	tableAliases map[string]string
	// Column aliases by their name. Multiple columns can share the same alias.
	columnAliases map[string][]*RowValGetter
}

func NewAliases() *Aliases {
	return &Aliases{
		tableAliases:  make(map[string]string),
		columnAliases: make(map[string][]*RowValGetter),
	}
}

// Returns a copy of the aliases with only table aliases filled in.
func (a *Aliases) TableAliasesOnly() *Aliases {
	return &Aliases{
		tableAliases:  a.tableAliases,
		columnAliases: make(map[string][]*RowValGetter),
	}
}

// Adds a table alias as specified. Returns an error if the alias already exists
func (a *Aliases) AddTableAlias(tableName, alias string) error {
	lowerAlias := strings.ToLower(alias)
	if _, ok := a.tableAliases[lowerAlias]; ok {
		return errFmt("Duplicate table alias: '%v'", alias)
	}
	a.tableAliases[lowerAlias] = tableName

	return nil
}

// GetTableByAlias returns the table name with the case-insensitive alias given, and a boolean ok value.
func (a *Aliases) GetTableByAlias(alias string) (string, bool) {
	tableName, ok := a.tableAliases[strings.ToLower(alias)]
	return tableName, ok
}

// AddColumnAlias adds a column alias as specified. Multiple column aliases can share the same name
func (a *Aliases) AddColumnAlias(alias string, getter *RowValGetter) {
	lowerAlias := strings.ToLower(alias)
	a.columnAliases[lowerAlias] = append(a.columnAliases[lowerAlias], getter)
}

// GetColumn returns the slice of RowValGetters for the case-insensitive column alias named.
func (a *Aliases) GetColumnByAlias(alias string) []*RowValGetter {
	return a.columnAliases[strings.ToLower(alias)]
}