package sql

// Aliases tracks the aliased identifiers in a query
type Aliases struct {
	// Map from table alias to table name
	TablesByAlias map[string]string
	// Map from table name to aliases
	AliasesByTable map[string][]string
	// Map from column name to column alias
	columnAliases map[QualifiedColumn]string
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
		columnAliases:  make(map[QualifiedColumn]string),
	}
}

// Returns a copy of the aliases with only table aliases filled in.`
func (a *Aliases) TableAliasesOnly() *Aliases {
	return &Aliases{
		TablesByAlias:  a.TablesByAlias,
		AliasesByTable: a.AliasesByTable,
		columnAliases:  make(map[QualifiedColumn]string),
	}
}

// Adds a column alias as specified. Silently overwrites existing entries.
func (a *Aliases) AddColumnAlias(qc QualifiedColumn, alias string) {
	a.columnAliases[qc] = alias
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

// Returns the single column matching the alias given. If no column matches, returns NoQualifiedColumn. If
// multiple match, returns an ambiguous column error.
func (a *Aliases) GetColumnForAlias(alias string) (QualifiedColumn, error) {
	found := QualifiedColumn{}
	for qc, colAlias := range a.columnAliases {
		if colAlias == alias {
			if !ColumnsEqual(found, QualifiedColumn{}) {
				return QualifiedColumn{}, errFmt(AmbiguousColumnErrFmt, alias)
			}
			found = qc
		}
	}
	return found, nil
}

// Returns the alias for the column given and whether the alias exists.
func (a *Aliases) GetColumnAlias(qc QualifiedColumn) (string, bool) {
	alias, ok := a.columnAliases[qc]
	return alias, ok
}

// Returns whether the two columns given are equal
func ColumnsEqual(c1, c2 QualifiedColumn) bool {
	return c1.TableName == c2.TableName && c1.ColumnName == c2.ColumnName
}
