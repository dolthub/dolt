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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/types"
)

var ErrUnexpectedJSONTypeIn = errors.New("unexpected type during JSON marshalling")
var ErrUnexpectedJSONTypeOut = errors.New("unexpected type during JSON unmarshalling")

const (
	JSONNull = "null"
)

// NomsJSON is a type alias for types.JSON. The alias allows MySQL-specific
// logic to be kept separate from the storage-layer code in pkg types.
type NomsJSON types.JSON

var _ sql.JSONValue = NomsJSON{}

// NomsJSONFromJSONValue converts a sql.JSONValue to a NomsJSON value.
func NomsJSONFromJSONValue(ctx context.Context, vrw types.ValueReadWriter, val sql.JSONValue) (NomsJSON, error) {
	if noms, ok := val.(NomsJSON); ok {
		return noms, nil
	}

	sqlDoc, err := val.Unmarshall(sql.NewContext(ctx))
	if err != nil {
		return NomsJSON{}, err
	}

	v, err := marshalJSON(ctx, vrw, sqlDoc.Val)
	if err != nil {
		return NomsJSON{}, err
	}

	doc, err := types.NewJSONDoc(vrw.Format(), vrw, v)
	if err != nil {
		return NomsJSON{}, err
	}

	return NomsJSON(doc), nil
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
	var err error
	vals := make([]types.Value, len(arr))
	for i, elem := range arr {
		vals[i], err = marshalJSON(ctx, vrw, elem)
		if err != nil {
			return nil, err
		}
	}
	return types.NewList(ctx, vrw, vals...)
}

func marshalJSONObject(ctx context.Context, vrw types.ValueReadWriter, obj map[string]interface{}) (types.Value, error) {
	var err error
	i := 0
	vals := make([]types.Value, len(obj)*2)
	for k, v := range obj {
		vals[i] = types.String(k)
		vals[i+1], err = marshalJSON(ctx, vrw, v)
		if err != nil {
			return nil, err
		}
		i += 2
	}
	return types.NewMap(ctx, vrw, vals...)
}

// Unmarshall implements the sql.JSONValue interface.
func (v NomsJSON) Unmarshall(ctx *sql.Context) (doc sql.JSONDocument, err error) {
	nomsVal, err := types.JSON(v).Inner()
	if err != nil {
		return sql.JSONDocument{}, err
	}

	val, err := unmarshalJSON(ctx, nomsVal)
	if err != nil {
		return sql.JSONDocument{}, err
	}

	return sql.JSONDocument{Val: val}, nil
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
	case types.List:
		return unmarshalJSONArray(ctx, val)
	case types.Map:
		return unmarshalJSONObject(ctx, val)
	default:
		return nil, ErrUnexpectedJSONTypeIn
	}
}

func unmarshalJSONArray(ctx context.Context, l types.List) (arr []interface{}, err error) {
	arr = make([]interface{}, l.Len())
	err = l.Iter(ctx, func(v types.Value, index uint64) (stop bool, err error) {
		arr[index], err = unmarshalJSON(ctx, v)
		return
	})
	return
}

func unmarshalJSONObject(ctx context.Context, m types.Map) (obj map[string]interface{}, err error) {
	obj = make(map[string]interface{}, m.Len())
	err = m.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
		ks, ok := key.(types.String)
		if !ok {
			return false, ErrUnexpectedJSONTypeOut
		}

		obj[string(ks)], err = unmarshalJSON(ctx, value)
		return
	})
	return
}

// Compare implements the sql.JSONValue interface.
func (v NomsJSON) Compare(ctx *sql.Context, other sql.JSONValue) (cmp int, err error) {
	noms, ok := other.(NomsJSON)
	if !ok {
		doc, err := v.Unmarshall(ctx)
		if err != nil {
			return 0, err
		}
		return doc.Compare(ctx, other)
	}

	return types.JSON(v).Compare(types.JSON(noms))
}

// ToString implements the sql.JSONValue interface.
func (v NomsJSON) ToString(ctx *sql.Context) (string, error) {
	jd, err := types.JSON(v).Inner()
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

	case types.List:
		sb.WriteRune('[')
		seenOne := false
		err = val.Iter(ctx, func(v types.Value, _ uint64) (stop bool, err error) {
			if seenOne {
				sb.WriteString(", ")
			}
			seenOne = true
			err = marshalToString(ctx, sb, v)
			return
		})
		if err != nil {
			return err
		}
		sb.WriteRune(']')

	case types.Map:
		sb.WriteRune('{')
		seenOne := false
		err = val.Iter(ctx, func(k, v types.Value) (stop bool, err error) {
			if seenOne {
				sb.WriteString(", ")
			}
			seenOne = true

			sb.WriteString(k.HumanReadableString())
			sb.WriteString(": ")

			err = marshalToString(ctx, sb, v)
			return
		})
		if err != nil {
			return err
		}
		sb.WriteRune('}')

	default:
		err = ErrUnexpectedJSONTypeOut
	}
	return
}
