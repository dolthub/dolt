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

type uintType struct{}

func (uintType) NomsKind() dtypes.NomsKind {
	return dtypes.UintKind
}

func (uintType) SqlType() sql.Type {
	return sql.Uint64
}

func (uintType) SqlTypes() []sql.Type {
	return []sql.Type{sql.Uint8, sql.Uint16, sql.Uint24, sql.Uint32, sql.Uint64}
}

func (uintType) GetValueToSql() ValueToSql {
	return func(val dtypes.Value) (interface{}, error) {
		if v, ok := val.(dtypes.Uint); ok {
			return uint64(v), nil
		}
		return nil, fmt.Errorf("expected Uint, recevied %v", val.Kind())
	}
}

func (uintType) GetSqlToValue() SqlToValue {
	return func(val interface{}) (dtypes.Value, error) {
		switch e := val.(type) {
		case int:
			if e < 0 {
				return nil, fmt.Errorf("cannot convert negative SQL val <%v> to uint", e)
			}
			return dtypes.Uint(e), nil
		case int8:
			if e < 0 {
				return nil, fmt.Errorf("cannot convert negative SQL val <%v> to uint", e)
			}
			return dtypes.Uint(e), nil
		case int16:
			if e < 0 {
				return nil, fmt.Errorf("cannot convert negative SQL val <%v> to uint", e)
			}
			return dtypes.Uint(e), nil
		case int32:
			if e < 0 {
				return nil, fmt.Errorf("cannot convert negative SQL val <%v> to uint", e)
			}
			return dtypes.Uint(e), nil
		case int64:
			if e < 0 {
				return nil, fmt.Errorf("cannot convert negative SQL val <%v> to uint", e)
			}
			return dtypes.Uint(e), nil
		case uint:
			return dtypes.Uint(e), nil
		case uint8:
			return dtypes.Uint(e), nil
		case uint16:
			return dtypes.Uint(e), nil
		case uint32:
			return dtypes.Uint(e), nil
		case uint64:
			return dtypes.Uint(e), nil
		default:
			return nil, fmt.Errorf("cannot convert SQL type <%T> val <%v> to uint", val, val)
		}
	}
}

func (uintType) String() string {
	return "uintType"
}
