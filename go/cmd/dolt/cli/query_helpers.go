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

// GetInt8ColAsBool returns the value of an int8 column as a bool
// This is necessary because Queryist may return an int8 column as a bool (when using SQLEngine)
// or as a string (when using ConnectionQueryist).
func GetInt8ColAsBool(col interface{}) (bool, error) {
	switch v := col.(type) {
	case int8:
		return v != 0, nil
	case string:
		if v == "ON" || v == "1" {
			return true, nil
		} else if v == "OFF" || v == "0" {
			return false, nil
		} else {
			return false, fmt.Errorf("unexpected value for boolean var: %v", v)
		}
	default:
		return false, fmt.Errorf("unexpected type %T, was expecting int8", v)
	}
}

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
	prevVal, err := GetInt8ColAsBool(row[0][1])
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

// GetStringColumnValue returns column values from [sql.Row] as a string.
func GetStringColumnValue(value any) (str string, err error) {
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

// GetBoolColumnValue returns the value of the input as a bool. This is required because depending on if we go over the
// wire or not we may get a string or a bool when we expect a bool.
func GetBoolColumnValue(col interface{}) (bool, error) {
	switch v := col.(type) {
	case bool:
		return col.(bool), nil
	case string:
		return strings.EqualFold(col.(string), "true") || strings.EqualFold(col.(string), "1"), nil
	default:
		return false, fmt.Errorf("unexpected type %T, was expecting bool or string", v)
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
