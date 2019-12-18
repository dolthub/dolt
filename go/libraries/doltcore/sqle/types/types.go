// Copyright 2019 Liquidata, Inc.
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

package types

import (
	"fmt"

	"github.com/src-d/go-mysql-server/sql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"

	dtypes "github.com/liquidata-inc/dolt/go/store/types"
)

type ValueToSql func(dtypes.Value) (interface{}, error)
type SqlToValue func(interface{}) (dtypes.Value, error)

type SqlType interface {
	// NomsKind is the underlying NomsKind that this initialization structure represents.
	NomsKind() dtypes.NomsKind
	// SqlType is the sql.Type that will be returned for Values of the NomsKind returned by NomsKind().
	// In other words, this is the SQL type that will be used as the default type for all Values of this NomsKind.
	SqlType() sql.Type
	// SqlTypes are the SQL types that will be directly processed to represent the underlying NomsKind of Value.
	SqlTypes() []sql.Type
	// GetValueToSql returns a function that accepts a Value (same type as returned by Value()) and returns the SQL representation.
	GetValueToSql() ValueToSql
	// GetSqlToValue returns a function that accepts any variable and returns a Value if applicable.
	GetSqlToValue() SqlToValue
	fmt.Stringer
}

var SqlTypeInitializers = []SqlType{
	//boolType{},
	datetimeType{},
	floatType{},
	intType{},
	stringType{},
	uintType{},
	uuidType{},
}

func init() {
	for _, sqlTypeInit := range SqlTypeInitializers {
		kind := sqlTypeInit.NomsKind()
		nomsKindToSqlType[kind] = sqlTypeInit.SqlType()
		nomsValToSqlValFunc[kind] = sqlTypeInit.GetValueToSql()
		nomsKindToValFunc[kind] = sqlTypeInit.GetSqlToValue()
		nomsKindToSqlTypeStr[kind] = sqlTypeInit.SqlType().String()
		for _, st := range sqlTypeInit.SqlTypes() {
			if _, ok := sqlTypeToNomsKind[st]; ok {
				panic(fmt.Errorf("SQL type %v already has a representation", st))
			}
			sqlTypeToNomsKind[st] = kind
		}
	}
}

var (
	nomsKindToSqlType      = map[dtypes.NomsKind]sql.Type{dtypes.BoolKind: sql.Int8}
	nomsKindToSqlTypeStr   = make(map[dtypes.NomsKind]string)
	nomsKindToValFunc      = make(map[dtypes.NomsKind]SqlToValue)
	nomsValToSqlValFunc    = make(map[dtypes.NomsKind]ValueToSql)
	sqlTypeToNomsKind      = make(map[sql.Type]dtypes.NomsKind)
	baseSqlTypesToNomsKind = map[query.Type]dtypes.NomsKind{
		sqltypes.Binary:    dtypes.StringKind,
		sqltypes.Bit:       dtypes.UintKind,
		sqltypes.Blob:      dtypes.StringKind,
		sqltypes.Char:      dtypes.StringKind,
		sqltypes.Date:      dtypes.TimestampKind,
		sqltypes.Datetime:  dtypes.TimestampKind,
		sqltypes.Decimal:   dtypes.FloatKind,
		sqltypes.Float32:   dtypes.FloatKind,
		sqltypes.Float64:   dtypes.FloatKind,
		sqltypes.Int16:     dtypes.IntKind,
		sqltypes.Int24:     dtypes.IntKind,
		sqltypes.Int32:     dtypes.IntKind,
		sqltypes.Int64:     dtypes.IntKind,
		sqltypes.Int8:      dtypes.IntKind,
		sqltypes.Null:      dtypes.NullKind,
		sqltypes.Text:      dtypes.StringKind,
		sqltypes.Time:      dtypes.InlineBlobKind,
		sqltypes.Timestamp: dtypes.TimestampKind,
		sqltypes.Uint16:    dtypes.UintKind,
		sqltypes.Uint24:    dtypes.UintKind,
		sqltypes.Uint32:    dtypes.UintKind,
		sqltypes.Uint64:    dtypes.UintKind,
		sqltypes.Uint8:     dtypes.UintKind,
		sqltypes.VarBinary: dtypes.StringKind,
		sqltypes.VarChar:   dtypes.StringKind,
		sqltypes.Year:      dtypes.IntKind,
	}
)

func NomsKindToSqlType(nomsKind dtypes.NomsKind) (sql.Type, error) {
	if st, ok := nomsKindToSqlType[nomsKind]; ok {
		return st, nil
	}
	if nomsKind == dtypes.BoolKind {
		return sql.Boolean, nil
	}
	return nil, fmt.Errorf("no corresponding SQL type found for %v", nomsKind)
}

func NomsKindToSqlTypeString(nomsKind dtypes.NomsKind) (string, error) {
	if str, ok := nomsKindToSqlTypeStr[nomsKind]; ok {
		return str, nil
	}
	if nomsKind == dtypes.BoolKind {
		return "BOOLEAN", nil
	}
	return "", fmt.Errorf("no corresponding SQL type found for %v", nomsKind)
}

func NomsValToSqlVal(val dtypes.Value) (interface{}, error) {
	if dtypes.IsNull(val) {
		return nil, nil
	}
	if valueToSQL, ok := nomsValToSqlValFunc[val.Kind()]; ok {
		return valueToSQL(val)
	}
	if boolVal, ok := val.(dtypes.Bool); ok {
		return bool(boolVal), nil
	}
	return nil, fmt.Errorf("Value of %v is unsupported in SQL", val.Kind())
}

func SqlTypeToNomsKind(t sql.Type) (dtypes.NomsKind, error) {
	if kind, ok := sqlTypeToNomsKind[t]; ok {
		return kind, nil
	}
	if kind, ok := baseSqlTypesToNomsKind[t.Type()]; ok {
		return kind, nil
	}
	return dtypes.UnknownKind, fmt.Errorf("unknown SQL type %v", t)
}

func SqlTypeToString(t sql.Type) (string, error) {
	return t.String(), nil
}

func SqlValToNomsVal(val interface{}, kind dtypes.NomsKind) (dtypes.Value, error) {
	if val == nil {
		return nil, nil
	}
	if varToVal, ok := nomsKindToValFunc[kind]; ok {
		return varToVal(val)
	}
	if kind == dtypes.BoolKind {
		return boolType{}.GetSqlToValue()(val)
	}
	return nil, fmt.Errorf("Value of %v is unsupported in SQL", kind)
}
