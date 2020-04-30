// Copyright 2020 Liquidata, Inc.
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

package sqle

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
	WHEN               = "when"
	WHERE              = "where"
	WITH               = "with"
	WRITE              = "write"
	YEAR               = "year"
	ZEROFILL           = "zerofill"
)

// All SQL keywords that we treat as reserved words
var AllKeywords = []string{
	ADD,
	AGAINST,
	ALL,
	ALTER,
	ANALYZE,
	AND,
	AS,
	ASC,
	AUTO_INCREMENT,
	BEGIN,
	BETWEEN,
	BIGINT,
	BINARY,
	BIT,
	BLOB,
	BOOL,
	BOOLEAN,
	BY,
	CASE,
	CAST,
	CHAR,
	CHARACTER,
	CHARSET,
	COLLATE,
	COLUMN,
	COMMENT,
	COMMIT,
	COMMITTED,
	CONSTRAINT,
	CONVERT,
	CREATE,
	CROSS,
	CURRENT_DATE,
	CURRENT_TIME,
	CURRENT_TIMESTAMP,
	DATABASE,
	DATABASES,
	DATE,
	DATETIME,
	DECIMAL,
	DEFAULT,
	DELETE,
	DESC,
	DESCRIBE,
	DISTINCT,
	DIV,
	DOUBLE,
	DROP,
	DUAL,
	DUPLICATE,
	ELSE,
	END,
	ENUM,
	ESCAPE,
	EXISTS,
	EXPANSION,
	EXPLAIN,
	EXTENDED,
	FALSE,
	FLOAT_TYPE,
	FOR,
	FORCE,
	FOREIGN,
	FROM,
	FULL,
	FULLTEXT,
	GEOMETRY,
	GEOMETRYCOLLECTION,
	GLOBAL,
	GROUP,
	GROUP_CONCAT,
	HAVING,
	IF,
	IGNORE,
	IN,
	INDEX,
	INNER,
	INSERT,
	INT,
	INTEGER,
	INTERVAL,
	INTO,
	IS,
	ISOLATION,
	JOIN,
	JSON,
	KEY,
	KEYS,
	KEY_BLOCK_SIZE,
	LANGUAGE,
	LAST_INSERT_ID,
	LEFT,
	LESS,
	LEVEL,
	LIKE,
	LIMIT,
	LINESTRING,
	LOCALTIME,
	LOCALTIMESTAMP,
	LOCK,
	LONGBLOB,
	LONGTEXT,
	MATCH,
	MAXVALUE,
	MEDIUMBLOB,
	MEDIUMINT,
	MEDIUMTEXT,
	MOD,
	MODE,
	MULTILINESTRING,
	MULTIPOINT,
	MULTIPOLYGON,
	NAMES,
	NATURAL,
	NCHAR,
	NEXT,
	NOT,
	NULL,
	NUMERIC,
	OFFSET,
	ON,
	ONLY,
	OPTIMIZE,
	OR,
	ORDER,
	OUTER,
	PARTITION,
	POINT,
	POLYGON,
	PRIMARY,
	PROCEDURE,
	PROCESSLIST,
	QUERY,
	READ,
	REAL,
	REGEXP,
	RLIKE,
	RENAME,
	REORGANIZE,
	REPAIR,
	REPEATABLE,
	REPLACE,
	RIGHT,
	ROLLBACK,
	SCHEMA,
	SELECT,
	SEPARATOR,
	SERIALIZABLE,
	SESSION,
	SET,
	SHARE,
	SHOW,
	SIGNED,
	SMALLINT,
	SPATIAL,
	SQL_CACHE,
	SQL_NO_CACHE,
	START,
	STATUS,
	STRAIGHT_JOIN,
	STREAM,
	SUBSTR,
	SUBSTRING,
	TABLE,
	TABLES,
	TEXT,
	THAN,
	THEN,
	TIME,
	TIMESTAMP,
	TINYBLOB,
	TINYINT,
	TINYTEXT,
	TO,
	TRANSACTION,
	TRIGGER,
	TRUE,
	TRUNCATE,
	UNCOMMITTED,
	UNDERSCORE_BINARY,
	UNION,
	UNIQUE,
	UNSIGNED,
	UPDATE,
	USE,
	USING,
	UTC_DATE,
	UTC_TIME,
	UTC_TIMESTAMP,
	UUID,
	VALUES,
	VARBINARY,
	VARCHAR,
	VARIABLES,
	VIEW,
	WHEN,
	WHERE,
	WITH,
	WRITE,
	YEAR,
	ZEROFILL,
}

// Well-supported keywords that are likely to be useful in e.g. autocompletion
var CommonKeywords = []string{
	ADD,
	ALTER,
	AND,
	AS,
	ASC,
	BIGINT,
	BINARY,
	BIT,
	BLOB,
	BOOL,
	BOOLEAN,
	BY,
	CHAR,
	CHARACTER,
	COLUMN,
	COMMENT,
	CREATE,
	DECIMAL,
	DEFAULT,
	DELETE,
	DESC,
	DESCRIBE,
	DISTINCT,
	DOUBLE,
	DROP,
	EXISTS,
	FALSE,
	FLOAT_TYPE,
	FROM,
	FULL,
	IGNORE,
	IN,
	INSERT,
	INT,
	INTEGER,
	INTO,
	IS,
	JOIN,
	KEY,
	KEYS,
	LIMIT,
	LONGBLOB,
	LONGTEXT,
	MEDIUMBLOB,
	MEDIUMINT,
	MEDIUMTEXT,
	NOT,
	NULL,
	NUMERIC,
	OFFSET,
	ON,
	OR,
	ORDER,
	PRIMARY,
	RENAME,
	SCHEMA,
	SELECT,
	SET,
	SHOW,
	SIGNED,
	SMALLINT,
	TABLE,
	TABLES,
	TEXT,
	TINYINT,
	TINYTEXT,
	TRUE,
	UNSIGNED,
	UPDATE,
	UUID,
	VALUES,
	VARCHAR,
	WHERE,
}
