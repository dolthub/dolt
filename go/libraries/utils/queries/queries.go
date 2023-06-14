package queries

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"strconv"
)

// Queryist is generic interface for executing queries. Commands will be provided a Queryist to perform any work using
// SQL. The Queryist can be obtained from the CliContext passed into the Exec method by calling the QueryEngine method.
type Queryist interface {
	Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, error)
}

// GetTinyIntColAsBool returns the value of a tinyint column as a bool
// This is necessary because Queryist may return a tinyint column as a bool (when using SQLEngine)
// or as a string (when using ConnectionQueryist).
func GetTinyIntColAsBool(col interface{}) (bool, error) {
	switch v := col.(type) {
	case bool:
		return v, nil
	case int:
		return v == 1, nil
	case string:
		return v == "1", nil
	default:
		return false, fmt.Errorf("unexpected type %T, was expecting bool, int, or string", v)
	}
}

// GetJsonDocumentColAsString returns the value of a JSONDocument column as a string
// This is necessary because Queryist may return a tinyint column as a bool (when using SQLEngine)
// or as a string (when using ConnectionQueryist).
func GetJsonDocumentColAsString(sqlCtx *sql.Context, col interface{}) (string, error) {
	switch v := col.(type) {
	case string:
		return v, nil
	case types.JSONDocument:
		text, err := v.ToString(sqlCtx)
		if err != nil {
			return "", err
		}
		return text, nil
	default:
		return "", fmt.Errorf("unexpected type %T, was expecting JSONDocument or string", v)
	}
}

// GetInt64ColAsInt64 returns the value of an int64 column as an int64
// This is necessary because Queryist may return an int64 column as an int64 (when using SQLEngine)
// or as a string (when using ConnectionQueryist).
func GetInt64ColAsInt64(col interface{}) (int64, error) {
	switch v := col.(type) {
	case uint64:
		return int64(v), nil
	case int64:
		return v, nil
	case string:
		iv, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, err
		}
		return iv, nil
	default:
		return 0, fmt.Errorf("unexpected type %T, was expecting int64, uint64 or string", v)
	}
}

// GetRowsForSql returns the resultant rows for a given SQL query
func GetRowsForSql(queryist Queryist, sqlCtx *sql.Context, q string) ([]sql.Row, error) {
	schema, ri, err := queryist.Query(sqlCtx, q)
	if err != nil {
		return nil, err
	}
	rows, err := sql.RowIterToRows(sqlCtx, schema, ri)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

var doltSystemTables = []string{
	"dolt_procedures",
	"dolt_schemas",
}

func GetTableNamesAtRef(queryist Queryist, sqlCtx *sql.Context, ref string) (map[string]bool, error) {
	// query for user-created tables
	q := fmt.Sprintf("SHOW FULL TABLES AS OF '%s'", ref)
	rows, err := GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return nil, err
	}

	tableNames := make(map[string]bool)
	for _, row := range rows {
		tableName := row[0].(string)
		tableType := row[1].(string)
		isTable := tableType == "BASE TABLE"
		if isTable {
			tableNames[tableName] = true
		}
	}

	// add system tables, if they exist at this ref
	for _, sysTable := range doltSystemTables {
		q = fmt.Sprintf("show create table %s as of '%s'", sysTable, ref)
		_, err = GetRowsForSql(queryist, sqlCtx, q)
		if err == nil {
			tableNames[sysTable] = true
		}
	}

	return tableNames, nil
}
