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
	"math"

	"github.com/src-d/go-mysql-server/sql"

	dtypes "github.com/liquidata-inc/dolt/go/store/types"
)

type boolType struct{}

func (boolType) NomsKind() dtypes.NomsKind {
	return dtypes.BoolKind
}

func (boolType) SqlType() sql.Type {
	return sql.Boolean
}

func (boolType) SqlTypes() []sql.Type {
	return []sql.Type{sql.Boolean}
}

func (boolType) GetValueToSql() ValueToSql {
	return func(val dtypes.Value) (interface{}, error) {
		if v, ok := val.(dtypes.Bool); ok {
			return bool(v), nil
		}
		return nil, fmt.Errorf("expected Bool, recevied %v", val.Kind())
	}
}

func (boolType) GetSqlToValue() SqlToValue {
	return func(val interface{}) (dtypes.Value, error) {
		switch e := val.(type) {
		case bool:
			return dtypes.Bool(e), nil
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return dtypes.Bool(e != 0), nil
		case float32, float64:
			return dtypes.Bool(int(math.Round(e.(float64))) != 0), nil
		case string:
			return dtypes.Bool(false), nil
		default:
			return nil, fmt.Errorf("cannot convert SQL type <%T> val <%v> to Bool", val, val)
		}
	}
}

func (boolType) SqlTypeString() string {
	return "BOOLEAN"
}

func (boolType) String() string {
	return "boolType"
}
