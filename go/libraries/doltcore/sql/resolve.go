// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"strings"
	"vitess.io/vitess/go/vt/sqlparser"
)

// A qualified column names its table and its column.
// Both names are case-sensitive and match the name as defined in the schema.
type QualifiedColumn struct {
	TableName  string
	ColumnName string
}

// Returns whether the two columns given are equal
func ColumnsEqual(c1, c2 QualifiedColumn) bool {
	return c1.TableName == c2.TableName && c1.ColumnName == c2.ColumnName
}

// Resolves the table name expression given by returning the canonical name of the table, or an error if no such table
// exists in the given root. The table name returned will always match the expression given except for case sensitivity.
// If an exact case-sensitive match for table name exists, it will be returned. Otherwise, a case-insensitive match will
// be returned if one exists. If no case-insensitive match exists, or if multiple exist, an error will be returned.
func resolveTable(tableNameExpr string, allTableNames []string, aliases *Aliases) (canonicalTableName string, err error) {
	if tableName, ok := aliases.GetTableByAlias(tableNameExpr); ok {
		return tableName, nil
	}

	// First look for an exact case-sensitive match
	for _, tableName := range allTableNames {
		if tableNameExpr == tableName {
			return tableName, nil
		}
	}

	// Then look for case-insensitive matches, watching for ambiguity
	var foundTableName string
	for _, tableName := range allTableNames {
		if strings.ToLower(tableNameExpr) == strings.ToLower(tableName) {
			if foundTableName != "" {
				return "", errFmt(AmbiguousTableErrFmt, tableNameExpr)
			}
			foundTableName = tableName
		}
	}

	if foundTableName != "" {
		return foundTableName, nil
	}

	return "", errFmt(UnknownTableErrFmt, tableNameExpr)
}

// Finds the schema that contains the column name given among the tables given, and returns the fully qualified column,
// with the full (unaliased) name of the table and column being referenced.  Returns an error if no schema contains such
// a column name, or if multiple do. Columns returned by this method are verified to exist.
//
// colName is the string column selection statement, e.g. "col" or "table.column". See getColumnNameString
func resolveColumn(colNameExpr string, schemas map[string]schema.Schema, aliases *Aliases) (QualifiedColumn, error) {
	// Try getting the table from the column name string itself, eg. "t.col"
	qc := parseQualifiedColumnString(colNameExpr)
	if qc.TableName != "" {
		tableName, err := resolveTable(qc.TableName, keys(schemas), aliases)
		if err != nil {
			return QualifiedColumn{}, err
		}

		if sch, ok := schemas[tableName]; ok {
			return resolveColumnInTables(qc.ColumnName, map[string]schema.Schema{tableName: sch})
		} else {
			panic("resolveTable returned an unknown table")
		}
	}

	// Finally, look through all input schemas to see if there's an exact match and dying if there's any ambiguity
	return resolveColumnInTables(colNameExpr, schemas)
}

// Finds the column name given in the aliases given. Returns nil if it can't be found, or an error if there's any
// ambiguity (column aliases can be non-distinct)
//
// colName is the string column selection statement, e.g. "col" or "table.column". See getColumnNameString
func resolveColumnAlias(colNameExpr string, aliases *Aliases) (*RowValGetter, error) {
	getters := aliases.GetColumnByAlias(colNameExpr)
	if len(getters) > 1 {
		return nil, errFmt(AmbiguousColumnErrFmt, colNameExpr)
	}
	if len(getters) == 1 {
		return getters[0], nil
	}
	return nil, nil
}

// Returns the best match column for the name given in the schemas given. An exact case-sensitive match is preferred.
// If that isn't found, try a case-insensitive match instead. If the name is ambiguous or cannot be found in any schema,
// returns an error.
func resolveColumnInTables(colName string, schemas map[string]schema.Schema) (QualifiedColumn, error) {
	var canonicalColumnName string
	var tableName string
	foundMatch := false
	ambiguous := false
	foundCaseSensitiveMatch := false

	for tbl, sch := range schemas {
		if col, ok := sch.GetAllCols().GetByName(colName); ok {
			if foundCaseSensitiveMatch {
				ambiguous = true
				break
			}
			tableName = tbl
			canonicalColumnName = col.Name
			foundCaseSensitiveMatch, foundMatch = true, true
		}

		if _, ok := sch.GetAllCols().GetByNameCaseInsensitive(colName); ok && !foundCaseSensitiveMatch {
			sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
				if strings.ToLower(colName) == strings.ToLower(col.Name) {
					if foundMatch {
						ambiguous = true
						return true
					}
					canonicalColumnName = col.Name
					tableName = tbl
					foundMatch = true
				}

				return false
			})
		}
	}

	if ambiguous {
		return QualifiedColumn{}, errFmt(AmbiguousColumnErrFmt, colName)
	}

	if !foundMatch {
		return QualifiedColumn{}, errFmt(UnknownColumnErrFmt, colName)
	}

	return QualifiedColumn{TableName: tableName, ColumnName: canonicalColumnName}, nil
}

// Returns a slice of the keys of the map given.
func keys(m map[string]schema.Schema) []string {
	ks := make([]string, len(m))
	i := 0
	for k, _ := range m {
		ks[i] = k
		i++
	}
	return ks
}

// Parses a qualified column string (e.g.: "a.id") into a qualified column name, where either the table name or the
// column name may be an alias. If there is no table qualifier, the returned QualifiedColumn will have an empty
// TableName. The ColumnName field will always be set for any non-empty string.
func parseQualifiedColumnString(colName string) QualifiedColumn {
	if len(colName) == 0 {
		panic("cannot parse empty column string")
	}

	if idx := strings.Index(colName, "."); idx > 0 {
		return QualifiedColumn{colName[:idx], colName[idx+1:]}
	}
	return QualifiedColumn{"", colName}
}

// Returns a canonicalized name string for the (potentially qualified) column name given, e.g. "a.id"
// This is the inverse of parseQualifiedColumnString().
func getColumnNameString(e *sqlparser.ColName) string {
	var b strings.Builder
	if !e.Qualifier.Name.IsEmpty() {
		b.WriteString(e.Qualifier.Name.String())
		b.WriteString(".")
	}
	b.WriteString(e.Name.String())
	return b.String()
}

// resolveColumnsInWhereClause returns the qualified columns referenced by the where clause.
func resolveColumnsInWhereClause(whereClause *sqlparser.Where, inputSchemas map[string]schema.Schema, aliases *Aliases) ([]QualifiedColumn, error) {
	if whereClause != nil && whereClause.Type != sqlparser.WhereStr {
		return nil, errFmt("Having clause not supported")
	}

	if whereClause == nil {
		return nil, nil
	}

	return resolveColumnsInExpr(whereClause.Expr, inputSchemas, aliases.TableAliasesOnly())
}

// resolveColumnsInSelectClause returns the qualified columns referenced by the select clause
func resolveColumnsInSelectClause(selectExprs sqlparser.SelectExprs, tableNames []string, schemas map[string]schema.Schema, aliases *Aliases) ([]QualifiedColumn, error) {
	if selectExprs == nil {
		return nil, nil
	}

	cols := make([]QualifiedColumn, 0)
	for _, colSelection := range selectExprs {
		switch selectExpr := colSelection.(type) {
		case *sqlparser.StarExpr:
			if qcs, err := resolveColumnsInStarExpr(selectExpr, tableNames, schemas, aliases); err != nil {
				return nil, err
			} else {
				cols = append(cols, qcs...)
			}
		case *sqlparser.AliasedExpr:
			if qcs, err := resolveColumnsInExpr(selectExpr.Expr, schemas, aliases.TableAliasesOnly()); err != nil {
				return nil, err
			} else {
				cols = append(cols, qcs...)
			}
		default:
			// do nothing
		}
	}

	return cols, nil
}

func resolveColumnsInStarExpr(selectExpr *sqlparser.StarExpr, tableNames []string, schemas map[string]schema.Schema, aliases *Aliases) ([]QualifiedColumn, error) {
	columns := make([]QualifiedColumn, 0)
	if !selectExpr.TableName.IsEmpty() {
		var targetTable string
		var err error
		if targetTable, err = resolveTable(selectExpr.TableName.Name.String(), tableNames, aliases); err != nil {
			return nil, err
		}

		tableSch := schemas[targetTable]
		tableSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
			columns = append(columns, QualifiedColumn{targetTable, col.Name})
			return false
		})
	} else {
		for _, tableName := range tableNames {
			tableSch := schemas[tableName]
			tableSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
				columns = append(columns, QualifiedColumn{tableName, col.Name})
				return false
			})
		}
	}

	return columns, nil
}

// resolveColumnsInOrderBy returns the qualified columns referenced in the order by clause.
func resolveColumnsInOrderBy(orderBy sqlparser.OrderBy, inputSchemas map[string]schema.Schema, aliases *Aliases) ([]QualifiedColumn, error) {
	if orderBy == nil {
		return nil, nil
	}

	cols := make([]QualifiedColumn, 0)
	for _, o := range orderBy {
		if obCols, err := resolveColumnsInExpr(o.Expr, inputSchemas, aliases); err != nil {
			return nil, err
		} else {
			cols = append(cols, obCols...)
		}
	}

	return cols, nil
}

// resolveColumnsInJoins returns the qualified columns referenced by the join expressions given
func resolveColumnsInJoins(joinExprs []*sqlparser.JoinTableExpr, inputSchemas map[string]schema.Schema, aliases *Aliases) ([]QualifiedColumn, error) {
	cols := make([]QualifiedColumn, 0)
	for _, je := range joinExprs {
		if joinCols, err := resolveColumnsInJoin(je, inputSchemas, aliases); err != nil {
			return nil, err
		} else {
			cols = append(cols, joinCols...)
		}
	}

	return cols, nil
}

// resolveColumnsInJoin returns the qualified columsn referenced by the join expression given
func resolveColumnsInJoin(expr *sqlparser.JoinTableExpr, schemas map[string]schema.Schema, aliases *Aliases) ([]QualifiedColumn, error) {
	if expr.Condition.Using != nil {
		return nil, errFmt("Using expression not supported: %v", nodeToString(expr.Condition.Using))
	}

	if expr.Condition.On == nil {
		return nil, nil
	}

	// This may not work in all cases -- not sure if there are expressions that are valid in where clauses but not in
	// join conditions or vice versa.
	return resolveColumnsInExpr(expr.Condition.On, schemas, aliases)
}

// resolveColumnsInExpr returns the set of columns referenced by the expression given.
// Supported parser types here must be kept in sync with createFilterForWhereExpr
func resolveColumnsInExpr(colExpr sqlparser.Expr, inputSchemas map[string]schema.Schema, aliases *Aliases) ([]QualifiedColumn, error) {

	cols := make([]QualifiedColumn, 0)
	switch expr := colExpr.(type) {
	case *sqlparser.ComparisonExpr:
		leftCols, err := resolveColumnsInExpr(expr.Left, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}
		rightCols, err := resolveColumnsInExpr(expr.Right, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}
		cols = append(cols, leftCols...)
		cols = append(cols, rightCols...)
	case *sqlparser.ColName:
		colNameStr := getColumnNameString(expr)

		// if the colName given is actually an alias, there are no columns to resolve (only a getter that may or may not
		// refer to an expression involving a column)
		if len(aliases.GetColumnByAlias(colNameStr)) > 0 {
			return nil, nil
		}

		qc, err := resolveColumn(colNameStr, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}
		cols = append(cols, qc)
	case *sqlparser.IsExpr:
		isCols, err := resolveColumnsInExpr(expr.Expr, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}
		cols = append(cols, isCols...)
	case *sqlparser.AndExpr:
		leftCols, err := resolveColumnsInExpr(expr.Left, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}
		rightCols, err := resolveColumnsInExpr(expr.Right, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}
		cols = append(cols, leftCols...)
		cols = append(cols, rightCols...)
	case *sqlparser.OrExpr:
		leftCols, err := resolveColumnsInExpr(expr.Left, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}
		rightCols, err := resolveColumnsInExpr(expr.Right, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}
		cols = append(cols, leftCols...)
		cols = append(cols, rightCols...)
	case *sqlparser.BinaryExpr:
		leftCols, err := resolveColumnsInExpr(expr.Left, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}
		rightCols, err := resolveColumnsInExpr(expr.Right, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}
		cols = append(cols, leftCols...)
		cols = append(cols, rightCols...)
	case *sqlparser.UnaryExpr:
		unaryCols, err := resolveColumnsInExpr(expr.Expr, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}

		cols = append(cols, unaryCols...)
	case *sqlparser.SQLVal, sqlparser.BoolVal, sqlparser.ValTuple:
		// No columns, just a SQL literal
	case *sqlparser.NotExpr:
		return nil, errFmt("Not expressions not supported: %v", nodeToString(expr))
	case *sqlparser.ParenExpr:
		return nil, errFmt("Parenthetical expressions not supported: %v", nodeToString(expr))
	case *sqlparser.RangeCond:
		return nil, errFmt("Range expressions not supported: %v", nodeToString(expr))
	case *sqlparser.ExistsExpr:
		return nil, errFmt("Exists expressions not supported: %v", nodeToString(expr))
	case *sqlparser.NullVal:
		return nil, errFmt("NULL expressions not supported: %v", nodeToString(expr))
	case *sqlparser.ValTuple:
		return nil, errFmt("Tuple expressions not supported: %v", nodeToString(expr))
	case *sqlparser.Subquery:
		return nil, errFmt("Subquery expressions not supported: %v", nodeToString(expr))
	case *sqlparser.ListArg:
		return nil, errFmt("List expressions not supported: %v", nodeToString(expr))
	case *sqlparser.IntervalExpr:
		return nil, errFmt("Interval expressions not supported: %v", nodeToString(expr))
	case *sqlparser.CollateExpr:
		return nil, errFmt("Collate expressions not supported: %v", nodeToString(expr))
	case *sqlparser.FuncExpr:
		return nil, errFmt("Function expressions not supported: %v", nodeToString(expr))
	case *sqlparser.CaseExpr:
		return nil, errFmt("Case expressions not supported: %v", nodeToString(expr))
	case *sqlparser.ValuesFuncExpr:
		return nil, errFmt("Values func expressions not supported: %v", nodeToString(expr))
	case *sqlparser.ConvertExpr:
		return nil, errFmt("Conversion expressions not supported: %v", nodeToString(expr))
	case *sqlparser.SubstrExpr:
		return nil, errFmt("Substr expressions not supported: %v", nodeToString(expr))
	case *sqlparser.ConvertUsingExpr:
		return nil, errFmt("Convert expressions not supported: %v", nodeToString(expr))
	case *sqlparser.MatchExpr:
		return nil, errFmt("Match expressions not supported: %v", nodeToString(expr))
	case *sqlparser.GroupConcatExpr:
		return nil, errFmt("Group concat expressions not supported: %v", nodeToString(expr))
	default:
		return nil, errFmt("Unrecognized expression resolving columns: %v", nodeToString(expr))
	}

	return cols, nil
}
