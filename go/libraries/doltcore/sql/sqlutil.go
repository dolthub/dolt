package sql

import (
	"errors"
	"fmt"
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

const PRINTED_NULL = "<NULL>"

// Boolean predicate func type to filter rows in result sets
type rowFilterFn = func(r row.Row) (matchesFilter bool)

// Turns a node to a string
func nodeToString(node sqlparser.SQLNode) string {
	buffer := sqlparser.NewTrackedBuffer(nil)
	node.Format(buffer)
	return buffer.String()
}

// Finds the schema that contains the column name given among the tables given, and returns the fully qualified column,
// with the full (unaliased) name of the table and column being referenced.  Returns an error if no schema contains such
// a column name, or if multiple do.
func resolveColumn(colName string, schemas map[string]schema.Schema, aliases *Aliases) (QualifiedColumn, error) {
	// First try matching any known aliases directly
	if qc, ok := aliases.ColumnsByAlias[colName]; ok {
		return qc, nil
	}

	// Then try getting the table from the column name string itself, eg. "t.col"
	qc := parseColumnAlias(colName)
	if qc.TableName != "" {
		tableName := aliases.TablesByAlias[qc.TableName]
		if resolvedName, ok := aliases.ColumnsByAlias[qc.ColumnName]; ok {
			return resolvedName, nil
		}
		if _, ok := schemas[tableName]; ok {
			return QualifiedColumn{TableName: tableName, ColumnName: qc.ColumnName}, nil
		} else {
			return QualifiedColumn{}, errFmt("Unrecognized table name: '%v'", tableName)
		}
	}

	// Finally, look through all input schemas to see if there's an exact match and dying if there's any ambiguity
	var colSchema schema.Schema
	var tableName string
	for tbl, sch := range schemas {
		if _, ok := sch.GetAllCols().GetByName(colName); ok {
			if colSchema != nil {
				return QualifiedColumn{}, errFmt("Ambiguous column: %v", colName)
			}
			colSchema = sch
			tableName = tbl
		}
	}

	if colSchema == nil {
		return QualifiedColumn{}, errFmt("Unknown column: '%v'", colName)
	}

	return QualifiedColumn{TableName: tableName, ColumnName: colName}, nil
}

// Parses a column alias (e.g.: "a.id") into a qualified column name, where either the table name or the column name may
// be an alias. If there is no table qualifier, the returned QualifiedColumn will have an empty TableName
func parseColumnAlias(colName string) QualifiedColumn {
	if idx := strings.Index(colName, "."); idx > 0 {
		return QualifiedColumn{colName[:idx], colName[idx+1:]}
	}
	return QualifiedColumn{"", colName}
}

type valGetterKind uint8
const (
	COLNAME valGetterKind = iota
	SQL_VAL
	BOOL_VAL
)

// valGetter is a convenience object used for comparing the right and left side of an expression
type valGetter struct {
	// The kind of this val getter
	Kind      valGetterKind
	// The value type returned by this getter
	NomsKind  types.NomsKind
	// The kind of the value that this getter's result will be compared against, filled in elsewhere
	CmpKind   types.NomsKind
	// Init() performs error checking and does any labor-saving pre-calculation that doens't need to be done for every
	// row in the result set
	Init      func() error
	// Get() returns the value for this getter for the row given
	Get       func(r row.Row) types.Value
	// CachedVal is a handy place to put a pre-computed value for getters that deal with constants or literals
	CachedVal types.Value
}

// Returns a comparison value getter for the expression given, which could be a column value or a literal
func getComparisonValueGetter(expr sqlparser.Expr, inputSchemas map[string]schema.Schema, aliases *Aliases, rss *resultset.ResultSetSchema) (*valGetter, error) {
	switch e := expr.(type) {
	case *sqlparser.ColName:
		colNameStr := getColumnNameString(e)

		qc, err := resolveColumn(colNameStr, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}
		tableSch := inputSchemas[qc.TableName]

		column, ok := tableSch.GetAllCols().GetByName(qc.ColumnName)
		if !ok {
			return nil, errFmt("Unknown column %v", colNameStr)
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
	default:
		return nil, errFmt("Unsupported comparison %v", nodeToString(e))
	}
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

func createFilterForWhere(whereClause *sqlparser.Where, inputSchemas map[string]schema.Schema, aliases *Aliases, rss *resultset.ResultSetSchema) (rowFilterFn, error) {
	if whereClause != nil && whereClause.Type != sqlparser.WhereStr {
		return nil, errFmt("Having clause not supported")
	}

	if whereClause == nil {
		return func(r row.Row) bool {
			return true
		}, nil
	} else {
		return createFilterForWhereExpr(whereClause.Expr, inputSchemas, aliases, rss)
	}
}

// createFilter creates a filter function from the where clause given, or returns an error if it cannot
func createFilterForWhereExpr(whereExpr sqlparser.Expr, inputSchemas map[string]schema.Schema, aliases *Aliases, rss *resultset.ResultSetSchema) (rowFilterFn, error) {

	var filter rowFilterFn
	switch expr := whereExpr.(type) {
	case *sqlparser.ComparisonExpr:

		leftGetter, err := getComparisonValueGetter(expr.Left, inputSchemas, aliases, rss)
		if err != nil {
			return nil, err
		}
		rightGetter, err := getComparisonValueGetter(expr.Right, inputSchemas, aliases, rss)
		if err != nil {
			return nil, err
		}

		// Fill in noms kinds for SQL_VAL fields if possible
		if leftGetter.Kind == SQL_VAL && rightGetter.Kind != SQL_VAL {
			leftGetter.NomsKind = rightGetter.NomsKind
		}
		if rightGetter.Kind == SQL_VAL && leftGetter.Kind != SQL_VAL {
			rightGetter.NomsKind = leftGetter.NomsKind
		}

		// Fill in comparison kinds before doing error checking
		rightGetter.CmpKind, leftGetter.CmpKind = leftGetter.NomsKind, rightGetter.NomsKind

		// Initialize the getters, mostly so that literal vals can do type error checking and cache results
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
		case sqlparser.NullSafeEqualStr:
			return nil, errFmt("null safe equal operation not supported")
		case sqlparser.InStr:
			return nil, errFmt("in keyword not supported")
		case sqlparser.NotInStr:
			return nil, errFmt("in keyword not supported")
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
		getter, err := getComparisonValueGetter(expr, inputSchemas, aliases, rss)
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
	case *sqlparser.NotExpr:
		return nil, errFmt("Not expressions not supported: %v", nodeToString(expr))
	case *sqlparser.ParenExpr:
		return nil, errFmt("Parenthetical expressions not supported: %v", nodeToString(expr))
	case *sqlparser.RangeCond:
		return nil, errFmt("Range expressions not supported: %v", nodeToString(expr))
	case *sqlparser.IsExpr:
		return nil, errFmt("Is expressions not supported: %v", nodeToString(expr))
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
	case *sqlparser.BinaryExpr:
		return nil, errFmt("Binary expressions not supported: %v", nodeToString(expr))
	case *sqlparser.UnaryExpr:
		return nil, errFmt("Unary expressions not supported: %v", nodeToString(expr))
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

func errFmt(fmtMsg string, args ...interface{}) error {
	return errors.New(fmt.Sprintf(fmtMsg, args...))
}
