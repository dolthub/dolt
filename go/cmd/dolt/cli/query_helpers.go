// Copyright 2025 Dolthub, Inc.
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

package cli

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// SetSystemVar sets the @@dolt_show_system_tables variable if necessary, and returns a function
// resetting the variable for after the commands completion, if necessary.
func SetSystemVar(queryist Queryist, sqlCtx *sql.Context, newVal bool) (func() error, error) {
	_, rowIter, _, err := queryist.Query(sqlCtx, "SHOW VARIABLES WHERE VARIABLE_NAME='dolt_show_system_tables'")
	if err != nil {
		return nil, err
	}

	row, err := sql.RowIterToRows(sqlCtx, rowIter)
	if err != nil {
		return nil, err
	}
	prevVal, err := QueryValueAsBool(row[0][1])
	if err != nil {
		return nil, err
	}

	var update func() error
	if newVal != prevVal {
		query := fmt.Sprintf("SET @@dolt_show_system_tables = %t", newVal)
		_, _, _, err = queryist.Query(sqlCtx, query)
		update = func() error {
			query := fmt.Sprintf("SET @@dolt_show_system_tables = %t", prevVal)
			_, _, _, err := queryist.Query(sqlCtx, query)
			return err
		}
	}

	return update, err
}

func GetRowsForSql(queryist Queryist, sqlCtx *sql.Context, query string) ([]sql.Row, error) {
	_, rowIter, _, err := queryist.Query(sqlCtx, query)
	if err != nil {
		return nil, err
	}
	rows, err := sql.RowIterToRows(sqlCtx, rowIter)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// QueryValueAsString converts a single value from a query result to a string. Use this when reading string-like
// columns from Queryist results, since the type can differ in-process [engine.SQLEngine] versus over the wire
// [sqlserver.ConnectionQueryist].
func QueryValueAsString(value any) (str string, err error) {
	if value == nil {
		return "", nil
	}

	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case fmt.Stringer:
		return v.String(), nil
	default:
		return "", fmt.Errorf("unexpected type %T, expected string-like column value", value)
	}
}

// QueryValueAsBool interprets a query result cell as a bool. Strings are normalized and matched as "true"/"1"/"ON"
// (true) or the opposite for false; matching is case-insensitive. [Queryist] may return a tinyint column as a bool
// when utilizing the [engine.SQLEngine] or as string when using [sqlserver.ConnectionQueryist].
func QueryValueAsBool(col interface{}) (bool, error) {
	switch v := col.(type) {
	case bool:
		return v, nil
	case byte:
		return v == 1, nil
	case int:
		return v == 1, nil
	case int8:
		return v == 1, nil
	case string:
		s := strings.TrimSpace(v)
		switch {
		case s == "1" || strings.EqualFold(s, "true") || strings.EqualFold(s, "ON"):
			return true, nil
		case s == "0" || strings.EqualFold(s, "false") || strings.EqualFold(s, "OFF"):
			return false, nil
		default:
			return false, fmt.Errorf("unexpected value for string for bool: %v", v)
		}
	default:
		return false, fmt.Errorf("unexpected type %T, was expecting bool, int, or string", v)
	}
}

// WithQueryWarningsLocked runs queries with a preserved warning buffer. Internal shell queries run on the same SQL
// session as user queries. Without warning locks, these housekeeping queries can clear or overwrite warnings that users
// expect.
func WithQueryWarningsLocked(sqlCtx *sql.Context, queryist Queryist, fn func() error) error {
	_, _, _, err := queryist.Query(sqlCtx, "set lock_warnings = 1")
	if err != nil {
		return err
	}

	runErr := fn()

	_, _, _, err = queryist.Query(sqlCtx, "set lock_warnings = 0")
	if err != nil {
		return err
	}
	return runErr
}
