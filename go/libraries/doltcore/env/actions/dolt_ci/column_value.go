// Copyright 2024 Dolthub, Inc.
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

package dolt_ci

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
)

type columnValue struct {
	ColumnName string
	Value      string
}

const utf8RuneError = string(utf8.RuneError)

type columnValues []*columnValue

func toUtf8StringValue(col *sql.Column, val interface{}) (string, error) {
	if val == nil {
		return "", nil
	}

	ti, err := typeinfo.FromSqlType(col.Type)
	if err != nil {
		return "", err
	}

	if ti.ToSqlType().Type() == sqltypes.Blob {
		return "", fmt.Errorf("binary types not supported in dolt ci configuration")
	} else {
		formattedVal, err := sqlutil.SqlColToStr(ti.ToSqlType(), val)
		if err != nil {
			return "", err
		}

		if utf8.ValidString(formattedVal) {
			return formattedVal, nil
		} else {
			return strings.ToValidUTF8(formattedVal, utf8RuneError), nil
		}
	}
}

func newColumnValue(col *sql.Column, val interface{}) (*columnValue, error) {
	utf8Value, err := toUtf8StringValue(col, val)
	if err != nil {
		return nil, err
	}

	if utf8Value == "" {
		return nil, nil
	}

	return &columnValue{
		ColumnName: col.Name,
		Value:      utf8Value,
	}, nil
}
