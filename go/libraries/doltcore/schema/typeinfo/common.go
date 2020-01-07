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

package typeinfo

import (
	"fmt"

	"github.com/liquidata-inc/dolt/go/store/types"
)

func floatConvertValueToNomsValue(ti TypeInfo, v interface{}) (types.Value, error) {
	if ti.IsValid(v) {
		switch val := v.(type) {
		case int:
			return types.Float(val), nil
		case int8:
			return types.Float(val), nil
		case int16:
			return types.Float(val), nil
		case int32:
			return types.Float(val), nil
		case int64:
			return types.Float(val), nil
		case uint:
			return types.Float(val), nil
		case uint8:
			return types.Float(val), nil
		case uint16:
			return types.Float(val), nil
		case uint32:
			return types.Float(val), nil
		case uint64:
			return types.Float(val), nil
		case float32:
			return types.Float(val), nil
		case float64:
			return types.Float(val), nil
		default:
			return nil, fmt.Errorf(`"%v" has falsely evaluated value "%v" of type "%T" as valid`, ti.String(), val, val)
		}
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

func intConvertValueToNomsValue(ti TypeInfo, v interface{}) (types.Value, error) {
	if ti.IsValid(v) {
		switch val := v.(type) {
		case bool:
			if val {
				return types.Int(1), nil
			}
			return types.Int(0), nil
		case int:
			return types.Int(val), nil
		case int8:
			return types.Int(val), nil
		case int16:
			return types.Int(val), nil
		case int32:
			return types.Int(val), nil
		case int64:
			return types.Int(val), nil
		case uint:
			return types.Int(val), nil
		case uint8:
			return types.Int(val), nil
		case uint16:
			return types.Int(val), nil
		case uint32:
			return types.Int(val), nil
		case uint64:
			return types.Int(val), nil
		default:
			return nil, fmt.Errorf(`"%v" has falsely evaluated value "%v" of type "%T" as valid`, ti.String(), val, val)
		}
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

func uintConvertValueToNomsValue(ti TypeInfo, v interface{}) (types.Value, error) {
	if ti.IsValid(v) {
		switch val := v.(type) {
		case bool:
			if val {
				return types.Uint(1), nil
			}
			return types.Uint(0), nil
		case int:
			return types.Uint(val), nil
		case int8:
			return types.Uint(val), nil
		case int16:
			return types.Uint(val), nil
		case int32:
			return types.Uint(val), nil
		case int64:
			return types.Uint(val), nil
		case uint:
			return types.Uint(val), nil
		case uint8:
			return types.Uint(val), nil
		case uint16:
			return types.Uint(val), nil
		case uint32:
			return types.Uint(val), nil
		case uint64:
			return types.Uint(val), nil
		default:
			return nil, fmt.Errorf(`"%v" has falsely evaluated value "%v" of type "%T" as valid`, ti.String(), val, val)
		}
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}
