package sql

import (
	"context"
	"errors"
	"fmt"
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/resultset"
	"github.com/xwb1989/sqlparser"
	"strconv"
	"strings"
)

// SQL keyword constants for use in switches and comparisons
const (
	ADD                = "add"
	AGAINST            = "against"
	ALL                = "all"
	ALTER              = "alter"
	ANALYZE            = "analyze"
	AND                = "and"
	AS                 = "as"
	ASC                = "asc"
	AUTO_INCREMENT     = "auto_increment"
	BEGIN              = "begin"
	BETWEEN            = "between"
	BIGINT             = "bigint"
	BINARY             = "binary"
	BIT                = "bit"
	BLOB               = "blob"
	BOOL               = "bool"
	BOOLEAN            = "boolean"
	BY                 = "by"
	CASE               = "case"
	CAST               = "cast"
	CHAR               = "char"
	CHARACTER          = "character"
	CHARSET            = "charset"
	COLLATE            = "collate"
	COLUMN             = "column"
	COMMENT            = "comment"
	COMMIT             = "commit"
	COMMITTED          = "committed"
	CONSTRAINT         = "constraint"
	CONVERT            = "convert"
	CREATE             = "create"
	CROSS              = "cross"
	CURRENT_DATE       = "current_date"
	CURRENT_TIME       = "current_time"
	CURRENT_TIMESTAMP  = "current_timestamp"
	DATABASE           = "database"
	DATABASES          = "databases"
	DATE               = "date"
	DATETIME           = "datetime"
	DECIMAL            = "decimal"
	DEFAULT            = "default"
	DELETE             = "delete"
	DESC               = "desc"
	DESCRIBE           = "describe"
	DISTINCT           = "distinct"
	DIV                = "div"
	DOUBLE             = "double"
	DROP               = "drop"
	DUAL               = "dual"
	DUPLICATE          = "duplicate"
	ELSE               = "else"
	END                = "end"
	ENUM               = "enum"
	ESCAPE             = "escape"
	EXISTS             = "exists"
	EXPANSION          = "expansion"
	EXPLAIN            = "explain"
	EXTENDED           = "extended"
	FALSE              = "false"
	FLOAT_TYPE         = "float"
	FOR                = "for"
	FORCE              = "force"
	FOREIGN            = "foreign"
	FROM               = "from"
	FULL               = "full"
	FULLTEXT           = "fulltext"
	GEOMETRY           = "geometry"
	GEOMETRYCOLLECTION = "geometrycollection"
	GLOBAL             = "global"
	GROUP              = "group"
	GROUP_CONCAT       = "group_concat"
	HAVING             = "having"
	IF                 = "if"
	IGNORE             = "ignore"
	IN                 = "in"
	INDEX              = "index"
	INNER              = "inner"
	INSERT             = "insert"
	INT                = "int"
	INTEGER            = "integer"
	INTERVAL           = "interval"
	INTO               = "into"
	IS                 = "is"
	ISOLATION          = "isolation"
	JOIN               = "join"
	JSON               = "json"
	KEY                = "key"
	KEYS               = "keys"
	KEY_BLOCK_SIZE     = "key_block_size"
	LANGUAGE           = "language"
	LAST_INSERT_ID     = "last_insert_id"
	LEFT               = "left"
	LESS               = "less"
	LEVEL              = "level"
	LIKE               = "like"
	LIMIT              = "limit"
	LINESTRING         = "linestring"
	LOCALTIME          = "localtime"
	LOCALTIMESTAMP     = "localtimestamp"
	LOCK               = "lock"
	LONGBLOB           = "longblob"
	LONGTEXT           = "longtext"
	MATCH              = "match"
	MAXVALUE           = "maxvalue"
	MEDIUMBLOB         = "mediumblob"
	MEDIUMINT          = "mediumint"
	MEDIUMTEXT         = "mediumtext"
	MOD                = "mod"
	MODE               = "mode"
	MULTILINESTRING    = "multilinestring"
	MULTIPOINT         = "multipoint"
	MULTIPOLYGON       = "multipolygon"
	NAMES              = "names"
	NATURAL            = "natural"
	NCHAR              = "nchar"
	NEXT               = "next"
	NOT                = "not"
	NULL               = "null"
	NUMERIC            = "numeric"
	OFFSET             = "offset"
	ON                 = "on"
	ONLY               = "only"
	OPTIMIZE           = "optimize"
	OR                 = "or"
	ORDER              = "order"
	OUTER              = "outer"
	PARTITION          = "partition"
	POINT              = "point"
	POLYGON            = "polygon"
	PRIMARY            = "primary"
	PROCEDURE          = "procedure"
	PROCESSLIST        = "processlist"
	QUERY              = "query"
	READ               = "read"
	REAL               = "real"
	REGEXP             = "regexp"
	RLIKE              = "rlike"
	RENAME             = "rename"
	REORGANIZE         = "reorganize"
	REPAIR             = "repair"
	REPEATABLE         = "repeatable"
	REPLACE            = "replace"
	RIGHT              = "right"
	ROLLBACK           = "rollback"
	SCHEMA             = "schema"
	SELECT             = "select"
	SEPARATOR          = "separator"
	SERIALIZABLE       = "serializable"
	SESSION            = "session"
	SET                = "set"
	SHARE              = "share"
	SHOW               = "show"
	SIGNED             = "signed"
	SMALLINT           = "smallint"
	SPATIAL            = "spatial"
	SQL_CACHE          = "sql_cache"
	SQL_NO_CACHE       = "sql_no_cache"
	START              = "start"
	STATUS             = "status"
	STRAIGHT_JOIN      = "straight_join"
	STREAM             = "stream"
	SUBSTR             = "substr"
	SUBSTRING          = "substring"
	TABLE              = "table"
	TABLES             = "tables"
	TEXT               = "text"
	THAN               = "than"
	THEN               = "then"
	TIME               = "time"
	TIMESTAMP          = "timestamp"
	TINYBLOB           = "tinyblob"
	TINYINT            = "tinyint"
	TINYTEXT           = "tinytext"
	TO                 = "to"
	TRANSACTION        = "transaction"
	TRIGGER            = "trigger"
	TRUE               = "true"
	TRUNCATE           = "truncate"
	UNCOMMITTED        = "uncommitted"
	UNDERSCORE_BINARY  = "_binary"
	UNION              = "union"
	UNIQUE             = "unique"
	UNSIGNED           = "unsigned"
	UPDATE             = "update"
	USE                = "use"
	USING              = "using"
	UTC_DATE           = "utc_date"
	UTC_TIME           = "utc_time"
	UTC_TIMESTAMP      = "utc_timestamp"
	UUID               = "uuid"
	VALUES             = "values"
	VARBINARY          = "varbinary"
	VARCHAR            = "varchar"
	VARIABLES          = "variables"
	VIEW               = "view"
	VINDEX             = "vindex"
	VINDEXES           = "vindexes"
	VITESS_KEYSPACES   = "vitess_keyspaces"
	VITESS_SHARDS      = "vitess_shards"
	VITESS_TABLETS     = "vitess_tablets"
	VSCHEMA_TABLES     = "vschema_tables"
	WHEN               = "when"
	WHERE              = "where"
	WITH               = "with"
	WRITE              = "write"
	YEAR               = "year"
	ZEROFILL           = "zerofill"
)

// Boolean predicate func type to filter rows in result sets
type rowFilterFn = func(r row.Row) (matchesFilter bool)

// Boolean lesser function for rows. Returns whether rLeft < rRight
type rowLesserFn func(rLeft row.Row, rRight row.Row) bool

const UnknownTableErrFmt = "Unknown table: '%v'"
const AmbiguousTableErrFmt = "Ambiguous table: '%v'"
const UnknownColumnErrFmt = "Unknown column: '%v'"
const AmbiguousColumnErrFmt = "Ambiguous column: '%v'"

// Turns a node to a string
func nodeToString(node sqlparser.SQLNode) string {
	buffer := sqlparser.NewTrackedBuffer(nil)
	node.Format(buffer)
	return buffer.String()
}

// Resolves the table name expression given by returning the canonical name of the table, or an error if no such table
// exists in the given root. The table name returned will always match the expression given except for case sensitivity.
// If an exact case-sensitive match for table name exists, it will be returned. Otherwise, a case-insensitive match will
// be returned if one exists. If no case-insensitive match exists, or if multiple exist, an error will be returned.
func resolveTable(tableNameExpr string, allTableNames []string, aliases *Aliases) (canonicalTableName string, err error) {
	if tableName, ok := aliases.TablesByAlias[tableNameExpr]; ok {
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
	// First try matching any known aliases directly. This is only appropriate in some contexts, e.g. in the ORDER BY but
	// not the WHERE clause. Callers should use Aliases.TableAliasesOnly for cases when the column aliases shouldn't be
	// considered.
	if qc, err := aliases.GetColumnForAlias(colNameExpr); err != nil {
		return QualifiedColumn{}, err
	} else if !ColumnsEqual(qc, QualifiedColumn{}) {
		return qc, nil
	}

	// Then try getting the table from the column name string itself, eg. "t.col"
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

// binaryNomsOperation knows how to combine two noms values into a single one, e.g. addition
type binaryNomsOperation func(left, right types.Value) types.Value
type unaryNomsOperation func(val types.Value) types.Value

type valGetterKind uint8

const (
	COLNAME valGetterKind = iota
	SQL_VAL
	BOOL_VAL
)

// valGetter is a convenience object used for comparing the right and left side of an expression
type valGetter struct {
	// The kind of this val getter
	Kind valGetterKind
	// The value type returned by this getter
	NomsKind types.NomsKind
	// The kind of the value that this getter's result will be compared against, filled in elsewhere
	CmpKind types.NomsKind
	// Init() performs error checking and does any labor-saving pre-calculation that doesn't need to be done for every
	// row in the result set
	Init func() error
	// Get() returns the value for this getter for the row given
	Get func(r row.Row) types.Value
	// CachedVal is a handy place to put a pre-computed value for getters that deal with constants or literals
	CachedVal types.Value
}

// Returns a comparison value getter for the expression given, which could be a column value or a literal
func getterFor(expr sqlparser.Expr, inputSchemas map[string]schema.Schema, aliases *Aliases, rss *resultset.ResultSetSchema) (*valGetter, error) {
	switch e := expr.(type) {
	case *sqlparser.NullVal:
		getter := valGetter{Kind: SQL_VAL}
		getter.Init = func() error { return nil }
		getter.Get = func(r row.Row) types.Value { return nil }
		return &getter, nil
	case *sqlparser.ColName:
		colNameStr := getColumnNameString(e)

		qc, err := resolveColumn(colNameStr, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}
		tableSch, ok := inputSchemas[qc.TableName]
		if !ok {
			return nil, errFmt("Unresolved table %v", qc.TableName)
		}

		column, ok := tableSch.GetAllCols().GetByName(qc.ColumnName)
		if !ok {
			return nil, errFmt(UnknownColumnErrFmt, colNameStr)
		}
		resultSetTag := rss.Mapping(tableSch).SrcToDest[column.Tag]

		getter := valGetter{Kind: COLNAME, NomsKind: column.Kind}
		getter.Init = func() error {
			return nil
		}
		getter.Get = func(r row.Row) types.Value {
			value, _ := r.GetColVal(resultSetTag)
			return value
		}

		return &getter, nil
	case *sqlparser.SQLVal:
		getter := valGetter{Kind: SQL_VAL}

		getter.Init = func() error {
			val, err := extractNomsValueFromSQLVal(e, getter.CmpKind)
			if err != nil {
				return err
			}
			getter.CachedVal = val
			return nil
		}
		getter.Get = func(r row.Row) types.Value {
			return getter.CachedVal
		}

		return &getter, nil
	case sqlparser.BoolVal:
		val := types.Bool(bool(e))
		getter := valGetter{Kind: BOOL_VAL, NomsKind: types.BoolKind}

		getter.Init = func() error {
			switch getter.CmpKind {
			case types.BoolKind:
				return nil
			default:
				return errFmt("Type mismatch: boolean value but non-numeric column: %v", nodeToString(e))
			}
		}
		getter.Get = func(r row.Row) types.Value {
			return val
		}

		return &getter, nil
	case sqlparser.ValTuple:
		getter := valGetter{Kind: SQL_VAL}

		getter.Init = func() error {
			vals := make([]types.Value, len(e))
			for i, item := range e {
				switch v := item.(type) {
				case *sqlparser.SQLVal:
					if val, err := extractNomsValueFromSQLVal(v, getter.CmpKind); err != nil {
						return err
					} else {
						vals[i] = val
					}
				default:
					return errFmt("Unsupported list literal: %v", nodeToString(v))
				}
			}

			// TODO: surely there is a better way to do this without resorting to interface{}
			ts := &chunks.TestStorage{}
			vs := types.NewValueStore(ts.NewView())
			set := types.NewSet(context.Background(), vs, vals...)

			getter.CachedVal = set
			return nil
		}
		getter.Get = func(r row.Row) types.Value {
			return getter.CachedVal
		}

		return &getter, nil
	case *sqlparser.BinaryExpr:
		getter, err := getterForBinaryExpr(e, inputSchemas, aliases, rss)
		if err != nil {
			return nil, err
		}
		return getter, nil
	case *sqlparser.UnaryExpr:
		getter, err := getterForUnaryExpr(e, inputSchemas, aliases, rss)
		if err != nil {
			return nil, err
		}
		return getter, nil
	default:
		return nil, errFmt("Unsupported type %v", nodeToString(e))
	}
}

// getterForUnaryExpr returns a getter for the given unary expression, where calls to Get() evaluates the full
// expression for the row given
func getterForUnaryExpr(e *sqlparser.UnaryExpr, inputSchemas map[string]schema.Schema, aliases *Aliases, rss *resultset.ResultSetSchema) (*valGetter, error) {
	getter, err := getterFor(e.Expr, inputSchemas, aliases, rss)
	if err != nil {
		return nil, err
	}

	var opFn unaryNomsOperation
	switch e.Operator {
	case sqlparser.UPlusStr:
		switch getter.NomsKind {
		case types.IntKind, types.FloatKind:
			// fine, nothing to do
		default:
			return nil, errFmt("Unsupported type for unary + operation: %v", types.KindToString[getter.NomsKind])
		}
		opFn = func(val types.Value) types.Value {
			return val
		}
	case sqlparser.UMinusStr:
		switch getter.NomsKind {
		case types.IntKind:
			opFn = func(val types.Value) types.Value {
				if types.IsNull(val) {
					return nil
				}
				return types.Int(-1 * val.(types.Int))
			}
		case types.FloatKind:
			opFn = func(val types.Value) types.Value {
				if types.IsNull(val) {
					return nil
				}
				return types.Float(-1 * val.(types.Float))
			}
		case types.UintKind:
			// TODO: this alters the type of the expression returned relative to the column's.
			//  This probably causes some problems.
			opFn = func(val types.Value) types.Value {
				if types.IsNull(val) {
					return nil
				}
				return types.Int(-1 * int64(val.(types.Uint)))
			}
		default:
			return nil, errFmt("Unsupported type for unary - operation: %v", types.KindToString[getter.NomsKind])
		}
	case sqlparser.BangStr:
		switch getter.NomsKind {
		case types.BoolKind:
			opFn = func(val types.Value) types.Value {
				return types.Bool(!val.(types.Bool))
			}
		default:
			return nil, errFmt("Unsupported type for unary ! operation: %v", types.KindToString[getter.NomsKind])
		}
	default:
		return nil, errFmt("Unsupported unary operation: %v", e.Operator)
	}

	unaryGetter := valGetter{}

	unaryGetter.Init = func() error {
		// Already did type checking explicitly
		return nil
	}

	unaryGetter.Get = func(r row.Row) types.Value {
		return opFn(getter.Get(r))
	}

	return &unaryGetter, nil
}

// getterForBinaryExpr returns a getter for the given binary expression, where calls to Get() evaluates the full
// expression for the row given
func getterForBinaryExpr(e *sqlparser.BinaryExpr, inputSchemas map[string]schema.Schema, aliases *Aliases, rss *resultset.ResultSetSchema) (*valGetter, error) {
	leftGetter, err := getterFor(e.Left, inputSchemas, aliases, rss)
	if err != nil {
		return nil, err
	}
	rightGetter, err := getterFor(e.Right, inputSchemas, aliases, rss)
	if err != nil {
		return nil, err
	}

	// Fill in target noms kinds for SQL_VAL fields if possible
	if leftGetter.Kind == SQL_VAL && rightGetter.Kind != SQL_VAL {
		leftGetter.NomsKind = rightGetter.NomsKind
	}
	if rightGetter.Kind == SQL_VAL && leftGetter.Kind != SQL_VAL {
		rightGetter.NomsKind = leftGetter.NomsKind
	}

	if rightGetter.NomsKind != leftGetter.NomsKind {
		return nil, errFmt("Unsupported binary operation types: %v, %v", types.KindToString[leftGetter.NomsKind], types.KindToString[rightGetter.NomsKind])
	}

	// Fill in comparison kinds before doing error checking
	rightGetter.CmpKind, leftGetter.CmpKind = leftGetter.NomsKind, rightGetter.NomsKind

	// Initialize the getters. This uses the type hints from above to enforce type constraints between columns and
	// literals.
	if err := leftGetter.Init(); err != nil {
		return nil, err
	}
	if err := rightGetter.Init(); err != nil {
		return nil, err
	}

	getter := valGetter{Kind: SQL_VAL, NomsKind: leftGetter.NomsKind, CmpKind: rightGetter.NomsKind}

	// All the operations differ only in their filter logic
	var opFn binaryNomsOperation
	switch e.Operator {
	case sqlparser.PlusStr:
		switch getter.NomsKind {
		case types.UintKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Uint(uint64(left.(types.Int)) + uint64(right.(types.Int)))
			}
		case types.IntKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Int(int64(left.(types.Int)) + int64(right.(types.Int)))
			}
		case types.FloatKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Float(float64(left.(types.Float)) + float64(right.(types.Float)))
			}
		default:
			return nil, errFmt("Unsupported type for + operation: %v", types.KindToString[getter.NomsKind])
		}
	case sqlparser.MinusStr:
		switch getter.NomsKind {
		case types.UintKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Uint(uint64(left.(types.Int)) - uint64(right.(types.Int)))
			}
		case types.IntKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Int(int64(left.(types.Int)) - int64(right.(types.Int)))
			}
		case types.FloatKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Float(float64(left.(types.Float)) - float64(right.(types.Float)))
			}
		default:
			return nil, errFmt("Unsupported type for - operation: %v", types.KindToString[getter.NomsKind])
		}
	case sqlparser.MultStr:
		switch getter.NomsKind {
		case types.UintKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Uint(uint64(left.(types.Int)) * uint64(right.(types.Int)))
			}
		case types.IntKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Int(int64(left.(types.Int)) * int64(right.(types.Int)))
			}
		case types.FloatKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Float(float64(left.(types.Float)) * float64(right.(types.Float)))
			}
		default:
			return nil, errFmt("Unsupported type for * operation: %v", types.KindToString[getter.NomsKind])
		}
	case sqlparser.DivStr:
		switch getter.NomsKind {
		case types.UintKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Uint(uint64(left.(types.Int)) / uint64(right.(types.Int)))
			}
		case types.IntKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Int(int64(left.(types.Int)) / int64(right.(types.Int)))
			}
		case types.FloatKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Float(float64(left.(types.Float)) / float64(right.(types.Float)))
			}
		default:
			return nil, errFmt("Unsupported type for / operation: %v", types.KindToString[getter.NomsKind])
		}
	case sqlparser.ModStr:
		switch getter.NomsKind {
		case types.UintKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Uint(uint64(left.(types.Int)) % uint64(right.(types.Int)))
			}
		case types.IntKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Int(int64(left.(types.Int)) % int64(right.(types.Int)))
			}
		default:
			return nil, errFmt("Unsupported type for %% operation: %v", types.KindToString[getter.NomsKind])
		}
	default:
		return nil, errFmt("Unsupported binary operation: %v", e.Operator)
	}

	getter.Init = func() error {
		// Already did type checking explicitly
		return nil
	}

	getter.Get = func(r row.Row) types.Value {
		leftVal := leftGetter.Get(r)
		rightVal := rightGetter.Get(r)
		if types.IsNull(leftVal) || types.IsNull(rightVal) {
			return nil
		}
		return opFn(leftVal, rightVal)
	}

	return &getter, nil
}

func getColumnNameString(e *sqlparser.ColName) string {
	var b strings.Builder
	if !e.Qualifier.Name.IsEmpty() {
		b.WriteString(e.Qualifier.Name.String())
		b.WriteString(".")
	}
	b.WriteString(e.Name.String())
	return b.String()
}

// resolveColumnsInWhereClause returns the qualified columns referenced by the where clause
func resolveColumnsInWhereClause(whereClause *sqlparser.Where, inputSchemas map[string]schema.Schema, aliases *Aliases) ([]QualifiedColumn, error) {
	if whereClause != nil && whereClause.Type != sqlparser.WhereStr {
		return nil, errFmt("Having clause not supported")
	}

	if whereClause == nil {
		return nil, nil
	}

	return resolveColumnsInExpr(whereClause.Expr, inputSchemas, aliases)
}

// resolveColumnsInOrderBy returns the qualified columns referenced in the order by clause
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

// resolveColumnsInExpr is the helper function for resolveColumnsInExpr, which can be used recursively on sub
// expressions. Supported parser types here must be kept in sync with createFilterForWhereExpr
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

// createFilterForWhere creates a filter function from the where clause given, or returns an error if it cannot
func createFilterForWhere(whereClause *sqlparser.Where, inputSchemas map[string]schema.Schema, aliases *Aliases, rss *resultset.ResultSetSchema) (rowFilterFn, error) {
	if whereClause != nil && whereClause.Type != sqlparser.WhereStr {
		return nil, errFmt("Having clause not supported")
	}

	if whereClause == nil {
		return func(r row.Row) bool {
			return true
		}, nil
	} else {
		return createFilterForWhereExpr(whereClause.Expr, inputSchemas, aliases.TableAliasesOnly(), rss)
	}
}

// createFilterForWhere creates a filter function from the joins given
func createFilterForJoins(joins []*sqlparser.JoinTableExpr, inputSchemas map[string]schema.Schema, aliases *Aliases, rss *resultset.ResultSetSchema) (rowFilterFn, error) {
	filterFns := make([]rowFilterFn, 0)
	for _, je := range joins {
		if filterFn, err := createFilterForJoin(je, inputSchemas, aliases.TableAliasesOnly(), rss); err != nil {
			return nil, err
		} else if filterFn != nil {
			filterFns = append(filterFns, filterFn)
		}
	}

	return func(r row.Row) (matchesFilter bool) {
		for _, fn := range filterFns {
			if !fn(r) {
				return false
			}
		}
		return true
	}, nil
}

// createFilterForJoin creates a row filter function for the join expression given
func createFilterForJoin(expr *sqlparser.JoinTableExpr, schemas map[string]schema.Schema, aliases *Aliases, rss *resultset.ResultSetSchema) (rowFilterFn, error) {
	if expr.Condition.Using != nil {
		return nil, errFmt("Using expression not supported: %v", nodeToString(expr.Condition.Using))
	}

	if expr.Condition.On == nil {
		return nil, nil
	}

	// This may not work in all cases -- not sure if there are expressions that are valid in where clauses but not in
	// join conditions or vice versa.
	return createFilterForWhereExpr(expr.Condition.On, schemas, aliases, rss)
}

// createFilterForWhereExpr is the helper function for createFilterForWhere, which can be used recursively on sub
// expressions. Supported parser types here must be kept in sync with resolveColumnsInExpr
func createFilterForWhereExpr(whereExpr sqlparser.Expr, inputSchemas map[string]schema.Schema, aliases *Aliases, rss *resultset.ResultSetSchema) (rowFilterFn, error) {
	var filter rowFilterFn
	switch expr := whereExpr.(type) {
	case *sqlparser.ComparisonExpr:

		leftGetter, err := getterFor(expr.Left, inputSchemas, aliases, rss)
		if err != nil {
			return nil, err
		}
		rightGetter, err := getterFor(expr.Right, inputSchemas, aliases, rss)
		if err != nil {
			return nil, err
		}

		// Fill in target noms kinds for SQL_VAL fields if possible
		if leftGetter.Kind == SQL_VAL && rightGetter.Kind != SQL_VAL {
			leftGetter.NomsKind = rightGetter.NomsKind
		}
		if rightGetter.Kind == SQL_VAL && leftGetter.Kind != SQL_VAL {
			rightGetter.NomsKind = leftGetter.NomsKind
		}

		// Fill in comparison kinds before doing error checking
		rightGetter.CmpKind, leftGetter.CmpKind = leftGetter.NomsKind, rightGetter.NomsKind

		// Initialize the getters. This uses the type hints from above to enforce type constraints between columns and
		// literals.
		if err := leftGetter.Init(); err != nil {
			return nil, err
		}
		if err := rightGetter.Init(); err != nil {
			return nil, err
		}

		// All the operations differ only in their filter logic
		switch expr.Operator {
		case sqlparser.EqualStr:
			filter = func(r row.Row) bool {
				leftVal := leftGetter.Get(r)
				rightVal := rightGetter.Get(r)
				if types.IsNull(leftVal) || types.IsNull(rightVal) {
					return false
				}
				return leftVal.Equals(rightVal)
			}
		case sqlparser.LessThanStr:
			filter = func(r row.Row) bool {
				leftVal := leftGetter.Get(r)
				rightVal := rightGetter.Get(r)
				if types.IsNull(leftVal) || types.IsNull(rightVal) {
					return false
				}
				return leftVal.Less(rightVal)
			}
		case sqlparser.GreaterThanStr:
			filter = func(r row.Row) bool {
				leftVal := leftGetter.Get(r)
				rightVal := rightGetter.Get(r)
				if types.IsNull(leftVal) || types.IsNull(rightVal) {
					return false
				}
				return rightVal.Less(leftVal)
			}
		case sqlparser.LessEqualStr:
			filter = func(r row.Row) bool {
				leftVal := leftGetter.Get(r)
				rightVal := rightGetter.Get(r)
				if types.IsNull(leftVal) || types.IsNull(rightVal) {
					return false
				}
				return leftVal.Less(rightVal) || leftVal.Equals(rightVal)
			}
		case sqlparser.GreaterEqualStr:
			filter = func(r row.Row) bool {
				leftVal := leftGetter.Get(r)
				rightVal := rightGetter.Get(r)
				if types.IsNull(leftVal) || types.IsNull(rightVal) {
					return false
				}
				return rightVal.Less(leftVal) || rightVal.Equals(leftVal)
			}
		case sqlparser.NotEqualStr:
			filter = func(r row.Row) bool {
				leftVal := leftGetter.Get(r)
				rightVal := rightGetter.Get(r)
				if types.IsNull(leftVal) || types.IsNull(rightVal) {
					return false
				}
				return !leftVal.Equals(rightVal)
			}
		case sqlparser.InStr:
			filter = func(r row.Row) bool {
				leftVal := leftGetter.Get(r)
				rightVal := rightGetter.Get(r)
				if types.IsNull(leftVal) || types.IsNull(rightVal) {
					return false
				}
				set := rightVal.(types.Set)
				return set.Has(context.Background(), leftVal)
			}
		case sqlparser.NotInStr:
			filter = func(r row.Row) bool {
				leftVal := leftGetter.Get(r)
				rightVal := rightGetter.Get(r)
				if types.IsNull(leftVal) || types.IsNull(rightVal) {
					return false
				}
				set := rightVal.(types.Set)
				return !set.Has(context.Background(), leftVal)
			}
		case sqlparser.NullSafeEqualStr:
			return nil, errFmt("null safe equal operation not supported")
		case sqlparser.LikeStr:
			return nil, errFmt("like keyword not supported")
		case sqlparser.NotLikeStr:
			return nil, errFmt("like keyword not supported")
		case sqlparser.RegexpStr:
			return nil, errFmt("regular expressions not supported")
		case sqlparser.NotRegexpStr:
			return nil, errFmt("regular expressions not supported")
		case sqlparser.JSONExtractOp:
			return nil, errFmt("json not supported")
		case sqlparser.JSONUnquoteExtractOp:
			return nil, errFmt("json not supported")
		}
	case *sqlparser.ColName:
		getter, err := getterFor(expr, inputSchemas, aliases, rss)
		if err != nil {
			return nil, err
		}

		if getter.NomsKind != types.BoolKind {
			return nil, errFmt("Type mismatch: cannot use column %v as boolean expression", nodeToString(expr))
		}

		filter = func(r row.Row) bool {
			colVal := getter.Get(r)
			if types.IsNull(colVal) {
				return false
			}
			return colVal.Equals(types.Bool(true))
		}

	case *sqlparser.AndExpr:
		var leftFilter, rightFilter rowFilterFn
		var err error
		if leftFilter, err = createFilterForWhereExpr(expr.Left, inputSchemas, aliases, rss); err != nil {
			return nil, err
		}
		if rightFilter, err = createFilterForWhereExpr(expr.Right, inputSchemas, aliases, rss); err != nil {
			return nil, err
		}
		filter = func(r row.Row) (matchesFilter bool) {
			return leftFilter(r) && rightFilter(r)
		}
	case *sqlparser.OrExpr:
		var leftFilter, rightFilter rowFilterFn
		var err error
		if leftFilter, err = createFilterForWhereExpr(expr.Left, inputSchemas, aliases, rss); err != nil {
			return nil, err
		}
		if rightFilter, err = createFilterForWhereExpr(expr.Right, inputSchemas, aliases, rss); err != nil {
			return nil, err
		}
		filter = func(r row.Row) (matchesFilter bool) {
			return leftFilter(r) || rightFilter(r)
		}
	case *sqlparser.IsExpr:
		op := expr.Operator
		switch op {
		case sqlparser.IsNullStr, sqlparser.IsNotNullStr:
			getter, err := getterFor(expr.Expr, inputSchemas, aliases, rss)
			if err != nil {
				return nil, err
			}

			if err := getter.Init(); err != nil {
				return nil, err
			}

			filter = func(r row.Row) (matchesFilter bool) {
				colVal := getter.Get(r)
				if (types.IsNull(colVal) && op == sqlparser.IsNullStr) || (!types.IsNull(colVal) && op == sqlparser.IsNotNullStr) {
					return true
				}
				return false
			}
		case sqlparser.IsTrueStr, sqlparser.IsNotTrueStr, sqlparser.IsFalseStr, sqlparser.IsNotFalseStr:
			getter, err := getterFor(expr.Expr, inputSchemas, aliases, rss)
			if err != nil {
				return nil, err
			}

			if getter.NomsKind != types.BoolKind {
				return nil, errFmt("Type mismatch: cannot use column %v as boolean expression", nodeToString(expr))
			}

			filter = func(r row.Row) (matchesFilter bool) {
				colVal := getter.Get(r)
				if types.IsNull(colVal) {
					return false
				}
				// TODO: this may not be the correct nullness semantics for "is not" comparisons
				if colVal.Equals(types.Bool(true)) {
					return op == sqlparser.IsTrueStr || op == sqlparser.IsNotFalseStr
				} else {
					return op == sqlparser.IsFalseStr || op == sqlparser.IsNotTrueStr
				}
			}
		default:
			return nil, errFmt("Unrecognized is comparison: %v", expr.Operator)
		}

	// Unary and Binary operators are supported in getGetter(), but not as top-level nodes here.
	case *sqlparser.BinaryExpr:
		return nil, errFmt("Binary expressions not supported: %v", nodeToString(expr))
	case *sqlparser.UnaryExpr:
		return nil, errFmt("Unary expressions not supported: %v", nodeToString(expr))

	// Full listing of the unsupported types for informative error messages
	case *sqlparser.NotExpr:
		return nil, errFmt("Not expressions not supported: %v", nodeToString(expr))
	case *sqlparser.ParenExpr:
		return nil, errFmt("Parenthetical expressions not supported: %v", nodeToString(expr))
	case *sqlparser.RangeCond:
		return nil, errFmt("Range expressions not supported: %v", nodeToString(expr))
	case *sqlparser.ExistsExpr:
		return nil, errFmt("Exists expressions not supported: %v", nodeToString(expr))
	case *sqlparser.SQLVal:
		return nil, errFmt("Literal expressions not supported: %v", nodeToString(expr))
	case *sqlparser.NullVal:
		return nil, errFmt("NULL expressions not supported: %v", nodeToString(expr))
	case *sqlparser.BoolVal:
		return nil, errFmt("Bool expressions not supported: %v", nodeToString(expr))
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
	case *sqlparser.Default:
		return nil, errFmt("Unrecognized expression: %v", nodeToString(expr))
	default:
		return nil, errFmt("Unrecognized expression: %v", nodeToString(expr))
	}

	return filter, nil
}

// extractNomsValueFromSQLVal extracts a noms value from the given SQLVal, using type info in the dolt column given as
// a hint and for type-checking
func extractNomsValueFromSQLVal(val *sqlparser.SQLVal, kind types.NomsKind) (types.Value, error) {
	switch val.Type {
	// Integer-like values
	case sqlparser.HexVal, sqlparser.HexNum, sqlparser.IntVal, sqlparser.BitVal:
		intVal, err := strconv.ParseInt(string(val.Val), 0, 64)
		if err != nil {
			return nil, err
		}
		switch kind {
		case types.IntKind:
			return types.Int(intVal), nil
		case types.FloatKind:
			return types.Float(intVal), nil
		case types.UintKind:
			return types.Uint(intVal), nil
		default:
			return nil, errFmt("Type mismatch: numeric value but non-numeric column: %v", nodeToString(val))
		}
	// Float values
	case sqlparser.FloatVal:
		floatVal, err := strconv.ParseFloat(string(val.Val), 64)
		if err != nil {
			return nil, err
		}
		switch kind {
		case types.FloatKind:
			return types.Float(floatVal), nil
		default:
			return nil, errFmt("Type mismatch: float value but non-float column: %v", nodeToString(val))
		}
	// Strings, which can be coerced into lots of other types
	case sqlparser.StrVal:
		strVal := string(val.Val)
		switch kind {
		case types.StringKind:
			return types.String(strVal), nil
		case types.UUIDKind:
			id, err := uuid.Parse(strVal)
			if err != nil {
				return nil, errFmt("Type mismatch: string value but non-string column: %v", nodeToString(val))
			}
			return types.UUID(id), nil
		default:
			return nil, errFmt("Type mismatch: string value but non-string column: %v", nodeToString(val))
		}
	case sqlparser.ValArg:
		return nil, errFmt("Value args not supported")
	default:
		return nil, errFmt("Unrecognized SQLVal type %v", val.Type)
	}
}

// extractNomsValueFromUnaryExpr extracts a noms value from the given expression, using the type info given as
// a hint and for type-checking. The underlying expression must be a SQLVal
func extractNomsValueFromUnaryExpr(expr *sqlparser.UnaryExpr, kind types.NomsKind) (types.Value, error) {
	sqlVal, ok := expr.Expr.(*sqlparser.SQLVal)
	if !ok {
		return nil, errFmt("Only SQL values are supported in unary expressions: %v", nodeToString(expr))
	}

	val, err := extractNomsValueFromSQLVal(sqlVal, kind)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.UPlusStr:
		switch kind {
		case types.UintKind, types.IntKind, types.FloatKind:
			return val, nil
		default:
			return nil, errFmt("Unsupported type for unary + operator: %v", nodeToString(expr))
		}
	case sqlparser.UMinusStr:
		switch kind {
		case types.UintKind:
			return nil, errFmt("Cannot use unary - with for an unsigned value: %v", nodeToString(expr))
		case types.IntKind:
			return types.Int(-1 * val.(types.Int)), nil
		case types.FloatKind:
			return types.Float(-1 * val.(types.Float)), nil
		default:
			return nil, errFmt("Unsupported type for unary - operator: %v", nodeToString(expr))
		}
	case sqlparser.BangStr:
		switch kind {
		case types.BoolKind:
			return types.Bool(!val.(types.Bool)), nil
		default:
			return nil, errFmt("Unsupported type for unary ! operator: '%v'", nodeToString(expr))
		}
	default:
		return nil, errFmt("Unsupported unary operator %v in expression: '%v'", expr.Operator, nodeToString(expr))
	}
}


func errFmt(fmtMsg string, args ...interface{}) error {
	return errors.New(fmt.Sprintf(fmtMsg, args...))
}
