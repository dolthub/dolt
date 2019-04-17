package sql

import (
	"errors"
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/xwb1989/sqlparser"
	"strconv"
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

// createFilter creates a filter function from the where clause given, or returns an error if it cannot
func createFilterForWhere(whereClause *sqlparser.Where, tableSch schema.Schema, aliases *Aliases) (rowFilterFn, error) {
	if whereClause != nil && whereClause.Type != sqlparser.WhereStr {
		return nil, errFmt("Having clause not supported")
	}

	var filter rowFilterFn
	if whereClause == nil {
		filter = func(r row.Row) bool {
			return true
		}
	} else {
		switch expr := whereClause.Expr.(type) {
		case *sqlparser.ComparisonExpr:
			left := expr.Left
			right := expr.Right
			op := expr.Operator

			colValOnLeft := true
			colExpr := left
			valExpr := right

			// Swap the column and value expr as necessary
			colName, ok := colExpr.(*sqlparser.ColName)
			if !ok {
				colValOnLeft = false
				colExpr = right
				valExpr = left
			}

			colName, ok = colExpr.(*sqlparser.ColName)
			if !ok {
				return nil, errFmt("Only column names and value literals are supported")
			}

			colNameStr := colName.Name.String()
			if colName, ok := aliases.ColumnsByAlias[colNameStr]; ok {
				colNameStr = colName
			}
			column, ok := tableSch.GetAllCols().GetByName(colNameStr)
			if !ok {
				return nil, errFmt("Unknown column: '%v'", colNameStr)
			}

			var comparisonVal types.Value
			switch val := valExpr.(type) {
			case *sqlparser.SQLVal:
				var err error
				comparisonVal, err = extractNomsValueFromSQLVal(val, column)
				if err != nil {
					return nil, err
				}
			case sqlparser.BoolVal:
				switch column.Kind {
				case types.BoolKind:
					comparisonVal = types.Bool(bool(val))
				default:
					return nil, errFmt("Type mismatch: boolean value but non-numeric column: %v", nodeToString(val))
				}

			default:
				return nil, errFmt("Only SQL literal values are supported in comparisons: %v", nodeToString(val))
			}

			// All the operations differ only in their filter logic
			switch op {
			case sqlparser.EqualStr:
				filter = func(r row.Row) bool {
					colVal, ok := r.GetColVal(column.Tag)
					if !ok {
						return false
					}
					return comparisonVal.Equals(colVal)
				}
			case sqlparser.LessThanStr:
				filter = func(r row.Row) bool {
					colVal, ok := r.GetColVal(column.Tag)
					if !ok {
						return false
					}

					leftVal := colVal
					rightVal := comparisonVal
					if !colValOnLeft {
						swap(&leftVal, &rightVal)
					}

					return leftVal.Less(rightVal)
				}
			case sqlparser.GreaterThanStr:
				filter = func(r row.Row) bool {
					colVal, ok := r.GetColVal(column.Tag)
					if !ok {
						return false
					}

					leftVal := colVal
					rightVal := comparisonVal
					if !colValOnLeft {
						swap(&leftVal, &rightVal)
					}

					return rightVal.Less(leftVal)
				}
			case sqlparser.LessEqualStr:
				filter = func(r row.Row) bool {
					colVal, ok := r.GetColVal(column.Tag)
					if !ok {
						return false
					}

					leftVal := colVal
					rightVal := comparisonVal
					if !colValOnLeft {
						swap(&leftVal, &rightVal)
					}

					return leftVal.Less(rightVal) || leftVal.Equals(rightVal)
				}
			case sqlparser.GreaterEqualStr:
				filter = func(r row.Row) bool {
					colVal, ok := r.GetColVal(column.Tag)
					if !ok {
						return false
					}

					leftVal := colVal
					rightVal := comparisonVal
					if !colValOnLeft {
						swap(&leftVal, &rightVal)
					}

					return rightVal.Less(leftVal) || rightVal.Equals(leftVal)
				}
			case sqlparser.NotEqualStr:
				filter = func(r row.Row) bool {
					colVal, ok := r.GetColVal(column.Tag)
					if !ok {
						return false
					}
					return !comparisonVal.Equals(colVal)
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
			colNameStr := expr.Name.String()
			if colName, ok := aliases.ColumnsByAlias[colNameStr]; ok {
				colNameStr = colName
			}
			column, ok := tableSch.GetAllCols().GetByName(colNameStr)
			if !ok {
				return nil, errFmt("Unknown column: '%v'", colNameStr)
			}
			if column.Kind != types.BoolKind {
				return nil, errFmt("Type mismatch: cannot use column %v as boolean expression", colNameStr)
			}

			filter = func(r row.Row) bool {
				colVal, ok := r.GetColVal(column.Tag)
				if !ok {
					return false
				}
				return colVal.Equals(types.Bool(true))
			}

		case *sqlparser.AndExpr:
			return nil, errFmt("And expressions not supported: %v", nodeToString(expr))
		case *sqlparser.OrExpr:
			return nil, errFmt("Or expressions not supported: %v", nodeToString(expr))
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
	}

	return filter, nil
}

func swap(left, right *types.Value) {
	temp := *right
	*right = *left
	*left = temp
}

// extractNomsValueFromSQLVal extracts a noms value from the given SQLVal, using type info in the dolt column given as
// a hint and for type-checking
func extractNomsValueFromSQLVal(val *sqlparser.SQLVal, column schema.Column) (types.Value, error) {
	switch val.Type {
	// Integer-like values
	case sqlparser.HexVal, sqlparser.HexNum, sqlparser.IntVal, sqlparser.BitVal:
		intVal, err := strconv.ParseInt(string(val.Val), 0, 64)
		if err != nil {
			return nil, err
		}
		switch column.Kind {
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
		switch column.Kind {
		case types.FloatKind:
			return types.Float(floatVal), nil
		default:
			return nil, errFmt("Type mismatch: float value but non-float column: %v", nodeToString(val))
		}
	// Strings, which can be coerced into lots of other types
	case sqlparser.StrVal:
		strVal := string(val.Val)
		switch column.Kind {
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
