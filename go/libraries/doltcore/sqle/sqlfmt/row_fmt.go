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

package sqlfmt

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
)

const singleQuote = `'`

// Quotes the identifier given with backticks.
func QuoteIdentifier(s string) string {
	return "`" + s + "`"
}

// QuoteComment quotes the given string with apostrophes, and escapes any contained within the string.
func QuoteComment(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `\'`) + `'`
}

func RowAsInsertStmt(r row.Row, tableName string, tableSch schema.Schema) (string, error) {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(QuoteIdentifier(tableName))
	b.WriteString(" ")

	b.WriteString("(")
	seenOne := false
	err := tableSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if seenOne {
			b.WriteRune(',')
		}
		b.WriteString(QuoteIdentifier(col.Name))
		seenOne = true
		return false, nil
	})

	if err != nil {
		return "", err
	}

	b.WriteString(")")

	b.WriteString(" VALUES (")
	seenOne = false
	_, err = r.IterSchema(tableSch, func(tag uint64, val types.Value) (stop bool, err error) {
		if seenOne {
			b.WriteRune(',')
		}
		col, _ := tableSch.GetAllCols().GetByTag(tag)
		sqlString, err := valueAsSqlString(col.TypeInfo, val)
		if err != nil {
			return true, err
		}
		b.WriteString(sqlString)
		seenOne = true
		return false, nil
	})

	if err != nil {
		return "", err
	}

	b.WriteString(");")

	return b.String(), nil
}

func SqlRowAsInsertStmt(ctx context.Context, r sql.Row, tableName string, tableSch schema.Schema) (string, error) {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(QuoteIdentifier(tableName))
	b.WriteString(" ")

	b.WriteString("(")
	seenOne := false
	err := tableSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if seenOne {
			b.WriteRune(',')
		}
		b.WriteString(QuoteIdentifier(col.Name))
		seenOne = true
		return false, nil
	})

	if err != nil {
		return "", err
	}

	b.WriteString(")")

	b.WriteString(" VALUES (")
	seenOne = false

	for i, val := range r {
		if seenOne {
			b.WriteRune(',')
		}
		col := tableSch.GetAllCols().GetAtIndex(i)
		str := "NULL"
		if val != nil {
			str, err = interfaceValueAsSqlString(ctx, col.TypeInfo, val)
			if err != nil {
				return "", err
			}
		}

		b.WriteString(str)
		seenOne = true
	}

	b.WriteString(");")

	return b.String(), nil
}

func RowAsDeleteStmt(r row.Row, tableName string, tableSch schema.Schema) (string, error) {
	var b strings.Builder
	b.WriteString("DELETE FROM ")
	b.WriteString(QuoteIdentifier(tableName))

	b.WriteString(" WHERE (")
	seenOne := false
	isKeyless := tableSch.GetPKCols().Size() == 0
	_, err := r.IterSchema(tableSch, func(tag uint64, val types.Value) (stop bool, err error) {
		col, _ := tableSch.GetAllCols().GetByTag(tag)
		if col.IsPartOfPK || isKeyless {
			if seenOne {
				b.WriteString(" AND ")
			}
			sqlString, err := valueAsSqlString(col.TypeInfo, val)
			if err != nil {
				return true, err
			}
			b.WriteString(QuoteIdentifier(col.Name))
			b.WriteRune('=')
			b.WriteString(sqlString)
			seenOne = true
		}
		return false, nil
	})

	if err != nil {
		return "", err
	}

	b.WriteString(");")
	return b.String(), nil
}

func RowAsUpdateStmt(r row.Row, tableName string, tableSch schema.Schema, colDiffs *set.StrSet) (string, error) {
	var b strings.Builder
	b.WriteString("UPDATE ")
	b.WriteString(QuoteIdentifier(tableName))
	b.WriteString(" ")

	b.WriteString("SET ")
	seenOne := false
	_, err := r.IterSchema(tableSch, func(tag uint64, val types.Value) (stop bool, err error) {
		col, _ := tableSch.GetAllCols().GetByTag(tag)
		exists := colDiffs.Contains(col.Name)
		if !col.IsPartOfPK && exists {
			if seenOne {
				b.WriteRune(',')
			}
			sqlString, err := valueAsSqlString(col.TypeInfo, val)
			if err != nil {
				return true, err
			}
			b.WriteString(QuoteIdentifier(col.Name))
			b.WriteRune('=')
			b.WriteString(sqlString)
			seenOne = true
		}
		return false, nil
	})

	if err != nil {
		return "", err
	}

	b.WriteString(" WHERE (")
	seenOne = false
	_, err = r.IterSchema(tableSch, func(tag uint64, val types.Value) (stop bool, err error) {
		col, _ := tableSch.GetAllCols().GetByTag(tag)
		if col.IsPartOfPK {
			if seenOne {
				b.WriteString(" AND ")
			}
			sqlString, err := valueAsSqlString(col.TypeInfo, val)
			if err != nil {
				return true, err
			}
			b.WriteString(QuoteIdentifier(col.Name))
			b.WriteRune('=')
			b.WriteString(sqlString)
			seenOne = true
		}
		return false, nil
	})

	if err != nil {
		return "", err
	}

	b.WriteString(");")
	return b.String(), nil
}

func valueAsSqlString(ti typeinfo.TypeInfo, value types.Value) (string, error) {
	if types.IsNull(value) {
		return "NULL", nil
	}

	str, err := ti.FormatValue(value)

	if err != nil {
		return "", err
	}

	switch ti.GetTypeIdentifier() {
	case typeinfo.BoolTypeIdentifier:
		// todo: unclear if we want this to output with "TRUE/FALSE" or 1/0
		if value.(types.Bool) {
			return "TRUE", nil
		}
		return "FALSE", nil
	case typeinfo.UuidTypeIdentifier, typeinfo.TimeTypeIdentifier, typeinfo.YearTypeIdentifier, typeinfo.DatetimeTypeIdentifier:
		return singleQuote + *str + singleQuote, nil
	case typeinfo.BlobStringTypeIdentifier, typeinfo.VarBinaryTypeIdentifier, typeinfo.InlineBlobTypeIdentifier, typeinfo.JSONTypeIdentifier, typeinfo.EnumTypeIdentifier, typeinfo.SetTypeIdentifier:
		return quoteAndEscapeString(*str), nil
	case typeinfo.VarStringTypeIdentifier:
		s, ok := value.(types.String)
		if !ok {
			return "", fmt.Errorf("typeinfo.VarStringTypeIdentifier is not types.String")
		}
		return quoteAndEscapeString(string(s)), nil
	default:
		return *str, nil
	}
}

func interfaceValueAsSqlString(ctx context.Context, ti typeinfo.TypeInfo, value interface{}) (string, error) {
	str := sqlutil.SqlColToStr(ctx, value)

	switch ti.GetTypeIdentifier() {
	case typeinfo.BoolTypeIdentifier:
		// todo: unclear if we want this to output with "TRUE/FALSE" or 1/0
		if value.(bool) {
			return "TRUE", nil
		}
		return "FALSE", nil
	case typeinfo.UuidTypeIdentifier, typeinfo.TimeTypeIdentifier, typeinfo.YearTypeIdentifier:
		return singleQuote + str + singleQuote, nil
	case typeinfo.DatetimeTypeIdentifier:
		reparsed, err := typeinfo.StringDefaultType.ConvertToType(ctx, nil, ti, types.String(str))
		if err != nil {
			return "", err
		}

		strp, err := ti.FormatValue(reparsed)
		if err != nil {
			return "", err
		}

		return singleQuote + *strp + singleQuote, nil
	case typeinfo.BlobStringTypeIdentifier, typeinfo.VarBinaryTypeIdentifier, typeinfo.InlineBlobTypeIdentifier, typeinfo.JSONTypeIdentifier, typeinfo.EnumTypeIdentifier, typeinfo.SetTypeIdentifier:
		return quoteAndEscapeString(str), nil
	case typeinfo.VarStringTypeIdentifier:
		s, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("typeinfo.VarStringTypeIdentifier is not types.String")
		}
		return quoteAndEscapeString(string(s)), nil
	default:
		return str, nil
	}
}

func quoteAndEscapeString(s string) string {
	buf := &bytes.Buffer{}
	v, err := sqltypes.NewValue(sqltypes.VarChar, []byte(s))
	if err != nil {
		panic(err)
	}
	v.EncodeSQL(buf)
	return buf.String()
}
