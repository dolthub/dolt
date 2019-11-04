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

	dtypes "github.com/liquidata-inc/dolt/go/store/types"
)

type floatType struct{}

func (floatType) NomsKind() dtypes.NomsKind {
	return dtypes.FloatKind
}

func (floatType) SqlType() sql.Type {
	return sql.Float64
}

func (floatType) SqlTypes() []sql.Type {
	return []sql.Type{sql.Float32, sql.Float64}
}

func (floatType) GetValueToSql() ValueToSql {
	return func(val dtypes.Value) (interface{}, error) {
		if v, ok := val.(dtypes.Float); ok {
			return float64(v), nil
		}
		return nil, fmt.Errorf("expected Float, recevied %v", val.Kind())
	}
}

func (floatType) GetSqlToValue() SqlToValue {
	return func(val interface{}) (dtypes.Value, error) {
		switch e := val.(type) {
		case int:
			return dtypes.Float(e), nil
		case int8:
			return dtypes.Float(e), nil
		case int16:
			return dtypes.Float(e), nil
		case int32:
			return dtypes.Float(e), nil
		case int64:
			return dtypes.Float(e), nil
		case uint:
			return dtypes.Float(e), nil
		case uint8:
			return dtypes.Float(e), nil
		case uint16:
			return dtypes.Float(e), nil
		case uint32:
			return dtypes.Float(e), nil
		case uint64:
			return dtypes.Float(e), nil
		case float32:
			return dtypes.Float(e), nil
		case float64:
			return dtypes.Float(e), nil
		default:
			return nil, fmt.Errorf("cannot convert SQL type <%T> val <%v> to float", val, val)
		}
	}
}

func (floatType) SqlTypeString() string {
	return "DOUBLE"
}

func (floatType) String() string {
	return "floatType"
}
