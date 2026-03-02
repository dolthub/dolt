// Copyright 2021 Dolthub, Inc.
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

package json

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/types"
)

var ErrUnexpectedJSONTypeIn = errors.New("unexpected type during JSON marshalling")
var ErrUnexpectedJSONTypeOut = errors.New("unexpected type during JSON unmarshalling")

const (
	JSONNull = "null"

	// Struct name and field names for JSON array/object encoding.
	// List and Map types were removed; we use Struct+Tuple instead.
	jsonStructName   = "json"
	jsonArrayField  = "a"
	jsonObjectField = "o"
)

// NomsJSON is a type alias for types.JSON. The alias allows MySQL-specific
// logic to be kept separate from the storage-layer code in pkg types.
type NomsJSON types.JSON

var _ sql.JSONWrapper = NomsJSON{}

// NomsJSONFromJSONValue converts a sql.JSONValue to a NomsJSON value.
func NomsJSONFromJSONValue(ctx context.Context, vrw types.ValueReadWriter, val sql.JSONWrapper) (NomsJSON, error) {
	if noms, ok := val.(NomsJSON); ok {
		return noms, nil
	}

	sqlVal, err := val.ToInterface(ctx)
	if err != nil {
		return NomsJSON{}, err
	}

	v, err := marshalJSON(ctx, vrw, sqlVal)
	if err != nil {
		return NomsJSON{}, err
	}

	doc, err := types.NewJSONDoc(vrw.Format(), vrw, v)
	if err != nil {
		return NomsJSON{}, err
	}

	return NomsJSON(doc), nil
}

func (v NomsJSON) Clone(_ context.Context) sql.JSONWrapper {
	return v
}

func marshalJSON(ctx context.Context, vrw types.ValueReadWriter, val interface{}) (types.Value, error) {
	if val == nil {
		return types.NullValue, nil
	}

	switch val := val.(type) {
	case []interface{}:
		return marshalJSONArray(ctx, vrw, val)
	case map[string]interface{}:
		return marshalJSONObject(ctx, vrw, val)
	case bool:
		return types.Bool(val), nil
	case string:
		return types.String(val), nil
	case float64:
		return types.Float(val), nil

	// TODO(andy): unclear how to handle these
	case float32:
		return types.Float(val), nil
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
	default:
		return nil, ErrUnexpectedJSONTypeIn
	}
}

func marshalJSONArray(ctx context.Context, vrw types.ValueReadWriter, arr []interface{}) (types.Value, error) {
	vals := make([]types.Value, len(arr))
	for i, elem := range arr {
		v, err := marshalJSON(ctx, vrw, elem)
		if err != nil {
			return nil, err
		}
		vals[i] = v
	}
	tup, err := types.NewTuple(vrw.Format(), vals...)
	if err != nil {
		return nil, err
	}
	return types.NewStruct(vrw.Format(), jsonStructName, types.StructData{jsonArrayField: tup})
}

func marshalJSONObject(ctx context.Context, vrw types.ValueReadWriter, obj map[string]interface{}) (types.Value, error) {
	// Sort keys for deterministic output (by length then alphabetically, per JSON spec)
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if len(keys[i]) != len(keys[j]) {
			return len(keys[i]) < len(keys[j])
		}
		return keys[i] < keys[j]
	})

	vals := make([]types.Value, len(keys)*2)
	for i, k := range keys {
		v, err := marshalJSON(ctx, vrw, obj[k])
		if err != nil {
			return nil, err
		}
		vals[i*2] = types.String(k)
		vals[i*2+1] = v
	}
	tup, err := types.NewTuple(vrw.Format(), vals...)
	if err != nil {
		return nil, err
	}
	return types.NewStruct(vrw.Format(), jsonStructName, types.StructData{jsonObjectField: tup})
}

func (v NomsJSON) ToInterface(ctx context.Context) (interface{}, error) {
	nomsVal, err := types.JSON(v).Inner()
	if err != nil {
		return nil, err
	}

	val, err := unmarshalJSON(ctx, nomsVal)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// Unmarshall implements the sql.JSONValue interface.
func (v NomsJSON) Unmarshall(ctx *sql.Context) (doc gmstypes.JSONDocument, err error) {
	nomsVal, err := types.JSON(v).Inner()
	if err != nil {
		return gmstypes.JSONDocument{}, err
	}

	val, err := unmarshalJSON(ctx, nomsVal)
	if err != nil {
		return gmstypes.JSONDocument{}, err
	}

	return gmstypes.JSONDocument{Val: val}, nil
}

func unmarshalJSON(ctx context.Context, val types.Value) (interface{}, error) {
	switch val := val.(type) {
	case types.Null:
		return nil, nil
	case types.Bool:
		return bool(val), nil
	case types.String:
		return string(val), nil
	case types.Float:
		return float64(val), nil
	case types.Struct:
		return unmarshalJSONStruct(ctx, val)
	default:
		return nil, ErrUnexpectedJSONTypeIn
	}
}

func unmarshalJSONStruct(ctx context.Context, s types.Struct) (interface{}, error) {
	if s.Name() != jsonStructName {
		return nil, ErrUnexpectedJSONTypeIn
	}
	if v, ok, err := s.MaybeGet(jsonArrayField); err != nil {
		return nil, err
	} else if ok {
		tup, ok := v.(types.Tuple)
		if !ok {
			return nil, ErrUnexpectedJSONTypeIn
		}
		return unmarshalJSONArray(ctx, tup)
	}
	if v, ok, err := s.MaybeGet(jsonObjectField); err != nil {
		return nil, err
	} else if ok {
		tup, ok := v.(types.Tuple)
		if !ok {
			return nil, ErrUnexpectedJSONTypeIn
		}
		return unmarshalJSONObject(ctx, tup)
	}
	return nil, ErrUnexpectedJSONTypeIn
}

func unmarshalJSONArray(ctx context.Context, tup types.Tuple) (arr []interface{}, err error) {
	n := tup.Len()
	arr = make([]interface{}, n)
	for i := uint64(0); i < n; i++ {
		v, err := tup.Get(i)
		if err != nil {
			return nil, err
		}
		arr[i], err = unmarshalJSON(ctx, v)
		if err != nil {
			return nil, err
		}
	}
	return arr, nil
}

func unmarshalJSONObject(ctx context.Context, tup types.Tuple) (obj map[string]interface{}, err error) {
	n := tup.Len()
	if n%2 != 0 {
		return nil, ErrUnexpectedJSONTypeOut
	}
	obj = make(map[string]interface{}, n/2)
	for i := uint64(0); i < n; i += 2 {
		keyVal, err := tup.Get(i)
		if err != nil {
			return nil, err
		}
		ks, ok := keyVal.(types.String)
		if !ok {
			return nil, ErrUnexpectedJSONTypeOut
		}
		valVal, err := tup.Get(i + 1)
		if err != nil {
			return nil, err
		}
		obj[string(ks)], err = unmarshalJSON(ctx, valVal)
		if err != nil {
			return nil, err
		}
	}
	return obj, nil
}

// JSONString implements the sql.JSONWrapper interface.
func (v NomsJSON) JSONString() (string, error) {
	return NomsJSONToString(context.Background(), v)
}

func NomsJSONToString(ctx context.Context, js NomsJSON) (string, error) {
	jd, err := types.JSON(js).Inner()
	if err != nil {
		return "", err
	}

	sb := &strings.Builder{}
	if err = marshalToString(ctx, sb, jd); err != nil {
		return "", err
	}

	return sb.String(), nil
}

func marshalToString(ctx context.Context, sb *strings.Builder, val types.Value) (err error) {
	switch val := val.(type) {
	case types.Null:
		sb.WriteString(JSONNull)

	case types.Bool:
		sb.WriteString(val.HumanReadableString())

	case types.String:
		sb.WriteString(val.HumanReadableString())

	case types.Float:
		sb.WriteString(val.HumanReadableString())

	case types.Struct:
		if val.Name() != jsonStructName {
			return ErrUnexpectedJSONTypeOut
		}
		if tupVal, ok, err := val.MaybeGet(jsonArrayField); err != nil {
			return err
		} else if ok {
			tup := tupVal.(types.Tuple)
			sb.WriteRune('[')
			for i := uint64(0); i < tup.Len(); i++ {
				if i > 0 {
					sb.WriteString(", ")
				}
				v, err := tup.Get(i)
				if err != nil {
					return err
				}
				if err = marshalToString(ctx, sb, v); err != nil {
					return err
				}
			}
			sb.WriteRune(']')
			return nil
		}
		if tupVal, ok, err := val.MaybeGet(jsonObjectField); err != nil {
			return err
		} else if ok {
			tup := tupVal.(types.Tuple)
			obj := make(map[string]types.Value, tup.Len()/2)
			var keys []string
			for i := uint64(0); i < tup.Len(); i += 2 {
				k, err := tup.Get(i)
				if err != nil {
					return err
				}
				ks, ok := k.(types.String)
				if !ok {
					return ErrUnexpectedJSONTypeOut
				}
				v, err := tup.Get(i + 1)
				if err != nil {
					return err
				}
				obj[string(ks)] = v
				keys = append(keys, string(ks))
			}
			// JSON map keys are sorted by length then alphabetically
			sort.Slice(keys, func(i, j int) bool {
				if len(keys[i]) != len(keys[j]) {
					return len(keys[i]) < len(keys[j])
				}
				return keys[i] < keys[j]
			})
			sb.WriteRune('{')
			for i, k := range keys {
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(fmt.Sprintf("\"%s\": ", k))
				if err = marshalToString(ctx, sb, obj[k]); err != nil {
					return err
				}
			}
			sb.WriteRune('}')
			return nil
		}
		return ErrUnexpectedJSONTypeOut

	default:
		err = ErrUnexpectedJSONTypeOut
	}
	return
}
