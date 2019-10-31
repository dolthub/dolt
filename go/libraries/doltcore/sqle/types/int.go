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

type intType struct{}

func (intType) NomsKind() dtypes.NomsKind {
	return dtypes.IntKind
}

func (intType) SqlType() sql.Type {
	return sql.Int64
}

func (intType) SqlTypes() []sql.Type {
	return []sql.Type{sql.Int8, sql.Int16, sql.Int24, sql.Int32, sql.Int64}
}

func (intType) GetValueToSql() ValueToSql {
	return func(val dtypes.Value) (interface{}, error) {
		if v, ok := val.(dtypes.Int); ok {
			return int64(v), nil
		}
		return nil, fmt.Errorf("expected Int, recevied %v", val.Kind())
	}
}

func (intType) GetSqlToValue() SqlToValue {
	return func(val interface{}) (dtypes.Value, error) {
		switch e := val.(type) {
		case int:
			return dtypes.Int(e), nil
		case int8:
			return dtypes.Int(e), nil
		case int16:
			return dtypes.Int(e), nil
		case int32:
			return dtypes.Int(e), nil
		case int64:
			return dtypes.Int(e), nil
		case uint:
			r := dtypes.Int(e)
			if r < 0 {
				return nil, fmt.Errorf("cannot convert SQL val <%v> to Int", e)
			}
			return r, nil
		case uint8:
			return dtypes.Int(e), nil
		case uint16:
			return dtypes.Int(e), nil
		case uint32:
			return dtypes.Int(e), nil
		case uint64:
			if e > math.MaxInt64 {
				return nil, fmt.Errorf("cannot convert SQL val <%v> to Int", e)
			}
			return dtypes.Int(e), nil
		default:
			return nil, fmt.Errorf("cannot convert SQL type <%T> val <%v> to Int", val, val)
		}
	}
}

func (intType) String() string {
	return "intType"
}
