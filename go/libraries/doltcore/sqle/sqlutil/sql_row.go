// Copyright 2020 Dolthub, Inc.
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

package sqlutil

import (
	"fmt"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	// Necessary for the empty context used by some functions to be initialized with system vars
	_ "github.com/dolthub/go-mysql-server/sql/variables"
	"github.com/dolthub/vitess/go/sqltypes"
)

// DoltRowToSqlRow constructs a go-mysql-server sql.Row from a Dolt row.Row.
func DoltRowToSqlRow(doltRow row.Row, sch schema.Schema) (sql.Row, error) {
	if doltRow == nil {
		return nil, nil
	}

	colVals := make(sql.Row, sch.GetAllCols().Size())
	i := 0

	_, err := doltRow.IterSchema(sch, func(tag uint64, val types.Value) (stop bool, err error) {
		col, _ := sch.GetAllCols().GetByTag(tag)
		colVals[i], err = col.TypeInfo.ConvertNomsValueToValue(val)
		i++

		stop = err != nil
		return
	})
	if err != nil {
		return nil, err
	}

	return sql.NewRow(colVals...), nil
}

// BinaryAsHexDisplayValue is a wrapper for binary values that should be displayed as hex strings.
type BinaryAsHexDisplayValue string

// SqlColToStr is a utility function for converting a sql column of type interface{} to a string.
// NULL values are treated as empty strings. Handle nil separately if you require other behavior.
func SqlColToStr(ctx *sql.Context, sqlType sql.Type, col interface{}) (string, error) {
	if col != nil {
		if hexVal, ok := col.(BinaryAsHexDisplayValue); ok {
			return string(hexVal), nil
		}

		switch typedCol := col.(type) {
		case bool:
			if typedCol {
				return "true", nil
			} else {
				return "false", nil
			}
		case sql.SpatialColumnType:
			res, err := sqlType.SQL(ctx, nil, col)
			hexRes := fmt.Sprintf("0x%X", res.Raw())
			if err != nil {
				return "", err
			}
			return hexRes, nil
		default:
			res, err := sqlType.SQL(ctx, nil, col)
			if err != nil {
				return "", err
			}
			return res.ToString(), nil
		}
	}

	return "", nil
}

// DatabaseTypeNameToSqlType converts a MySQL wire protocol database type name
// to a go-mysql-server sql.Type. This uses the same type mapping logic as the existing
// Dolt type system for consistency.
// TODO: Add support for BLOB types (TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB) and BIT type
// as confirmed by testing MySQL 8.4+ binary-as-hex behavior
func DatabaseTypeNameToSqlType(databaseTypeName string) sql.Type {
	typeName := strings.ToLower(databaseTypeName)
	switch typeName {
	case "binary":
		return gmstypes.MustCreateBinary(sqltypes.Binary, 255)
	case "varbinary":
		return gmstypes.MustCreateBinary(sqltypes.VarBinary, 255)
	default:
		// Default to LongText for all other types (as was done before)
		return gmstypes.LongText
	}
}
