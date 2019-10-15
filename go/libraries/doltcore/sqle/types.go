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

package sqle

import (
	"errors"
	"fmt"
	"math"

	"github.com/google/uuid"
	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/store/types"
)

func nomsTypeToSqlType(kind types.NomsKind) sql.Type {
	switch kind {
	case types.BoolKind:
		return sql.Boolean
	case types.FloatKind:
		return sql.Float64
	case types.StringKind:
		return sql.Text
	case types.UUIDKind:
		// TODO: make an actual uuid
		return sql.Text
	case types.IntKind:
		return sql.Int64
	case types.UintKind:
		return sql.Uint64
	default:
		panic(fmt.Sprintf("Unexpected kind %v", kind))
	}
}

func SqlTypeToNomsKind(t sql.Type) types.NomsKind {
	switch t {
	case sql.Boolean:
		return types.BoolKind
	case sql.Float32, sql.Float64:
		return types.FloatKind
	case sql.Text:
		// TODO: handle UUIDs
		return types.StringKind
	case sql.Int8, sql.Int16, sql.Int24, sql.Int32, sql.Int64:
		return types.IntKind
	case sql.Uint8, sql.Uint16, sql.Uint24, sql.Uint32, sql.Uint64:
		return types.UintKind
	default:
		panic(fmt.Sprintf("Unexpected type %v", t))
	}
}

func nomsValToSqlVal(val types.Value) interface{} {
	switch val.Kind() {
	case types.BoolKind:
		return convertBool(val.(types.Bool))
	case types.FloatKind:
		return convertFloat(val.(types.Float))
	case types.StringKind:
		return convertString(val.(types.String))
	case types.UUIDKind:
		return convertUUID(val.(types.UUID))
	case types.IntKind:
		return convertInt(val.(types.Int))
	case types.UintKind:
		return convertUint(val.(types.Uint))
	default:
		panic(fmt.Sprintf("Unexpected kind %v", val.Kind()))
	}
}

func SqlValToNomsVal(val interface{}, kind types.NomsKind) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	switch kind {
	case types.BoolKind:
		switch e := val.(type) {
		case bool:
			return types.Bool(e), nil
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return types.Bool(e != 0), nil
		case float32, float64:
			return types.Bool(int(math.Round(e.(float64))) != 0), nil
		case string:
			return types.Bool(false), nil
		default:
			return nil, errors.New(fmt.Sprintf("Cannot convert SQL type <%T> val <%v> to bool", val, val))
		}
	case types.IntKind:
		switch e := val.(type) {
		case int:
			return types.Int(e), nil
		case int8:
			return types.Int(e), nil
		case int16:
			return types.Int(e), nil
		case int32:
			return types.Int(e), nil
		case int64:
			return types.Int(e), nil
		case uint:
			r := types.Int(e)
			if r < 0 {
				return nil, errors.New(fmt.Sprintf("Cannot convert SQL val <%v> to int", e))
			}
			return r, nil
		case uint8:
			return types.Int(e), nil
		case uint16:
			return types.Int(e), nil
		case uint32:
			return types.Int(e), nil
		case uint64:
			if e > math.MaxInt64 {
				return nil, errors.New(fmt.Sprintf("Cannot convert SQL val <%v> to int", e))
			}
			return types.Int(e), nil
		default:
			return nil, errors.New(fmt.Sprintf("Cannot convert SQL type <%T> val <%v> to int", val, val))
		}
	case types.UintKind:
		switch e := val.(type) {
		case int:
			if e < 0 {
				return nil, errors.New(fmt.Sprintf("Cannot convert negative SQL val <%v> to uint", e))
			}
			return types.Uint(e), nil
		case int8:
			if e < 0 {
				return nil, errors.New(fmt.Sprintf("Cannot convert negative SQL val <%v> to uint", e))
			}
			return types.Uint(e), nil
		case int16:
			if e < 0 {
				return nil, errors.New(fmt.Sprintf("Cannot convert negative SQL val <%v> to uint", e))
			}
			return types.Uint(e), nil
		case int32:
			if e < 0 {
				return nil, errors.New(fmt.Sprintf("Cannot convert negative SQL val <%v> to uint", e))
			}
			return types.Uint(e), nil
		case int64:
			if e < 0 {
				return nil, errors.New(fmt.Sprintf("Cannot convert negative SQL val <%v> to uint", e))
			}
			return types.Uint(e), nil
		case uint:
			return types.Uint(e), nil
		case uint8:
			return types.Uint(e), nil
		case uint16:
			return types.Uint(e), nil
		case uint32:
			return types.Uint(e), nil
		case uint64:
			return types.Uint(e), nil
		default:
			return nil, errors.New(fmt.Sprintf("Cannot convert SQL type <%T> val <%v> to uint", val, val))
		}
	case types.FloatKind:
		switch e := val.(type) {
		case int:
			return types.Float(e), nil
		case int8:
			return types.Float(e), nil
		case int16:
			return types.Float(e), nil
		case int32:
			return types.Float(e), nil
		case int64:
			return types.Float(e), nil
		case uint:
			return types.Float(e), nil
		case uint8:
			return types.Float(e), nil
		case uint16:
			return types.Float(e), nil
		case uint32:
			return types.Float(e), nil
		case uint64:
			return types.Float(e), nil
		case float32:
			return types.Float(e), nil
		case float64:
			return types.Float(e), nil
		default:
			return nil, errors.New(fmt.Sprintf("Cannot convert SQL type <%T> val <%v> to float", val, val))
		}
	case types.StringKind:
		e, ok := val.(string)
		if !ok {
			return nil, errors.New(fmt.Sprintf("Cannot convert SQL type <%T> val <%v> to string", val, val))
		}
		return types.String(e), nil
	case types.UUIDKind:
		e, ok := val.(string)
		if !ok {
			return nil, errors.New(fmt.Sprintf("Cannot convert SQL type <%T> val <%v> to uuid", val, val))
		}
		if u, err := uuid.Parse(e); err == nil {
			return types.UUID(u), nil
		}
		return nil, errors.New(fmt.Sprintf("Cannot convert SQL val <%v> to uuid", e))
	default:
		return nil, errors.New("invalid Kind to convert SQL type to")
	}
}

func convertUUID(u types.UUID) interface{} {
	return u.String()
}

func convertUint(i types.Uint) interface{} {
	return uint64(i)
}

func convertInt(i types.Int) interface{} {
	return int64(i)
}

func convertString(i types.String) interface{} {
	return string(i)
}

func convertFloat(f types.Float) interface{} {
	return float64(f)
}

func convertBool(b types.Bool) interface{} {
	return bool(b)
}
