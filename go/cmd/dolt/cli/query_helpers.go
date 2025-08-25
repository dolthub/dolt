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
