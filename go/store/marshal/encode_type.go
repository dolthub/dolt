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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package marshal implements encoding and decoding of Noms values. The mapping
// between Noms objects and Go values is described  in the documentation for the
// Marshal and Unmarshal functions.
package marshal

import (
	"fmt"
	"reflect"

	"github.com/liquidata-inc/dolt/go/store/types"
)

// MarshalType computes a Noms type from a Go type
//
// The rules for MarshalType is the same as for Marshal, except for omitempty
// which leads to an optional field since it depends on the runtime value and
// can lead to the property not being present.
//
// If a Go struct contains a noms tag with original the field is skipped since
// the Noms type depends on the original Noms value which is not available.
func MarshalType(nbf *types.NomsBinFormat, v interface{}) (nt *types.Type, err error) {
	return MarshalTypeOpt(nbf, v, Opt{})
}

// MarshalTypeOpt is like MarshalType but with additional options.
func MarshalTypeOpt(nbf *types.NomsBinFormat, v interface{}, opt Opt) (*types.Type, error) {
	rv := reflect.ValueOf(v)
	tags := nomsTags{
		set: opt.Set,
	}

	nt, err := encodeType(nbf, rv.Type(), map[string]reflect.Type{}, tags)

	if err != nil {
		return nil, err
	}

	if nt == nil {
		return nil, &UnsupportedTypeError{Type: rv.Type()}
	}

	return nt, err
}

// TypeMarshaler is an interface types can implement to provide their own
// encoding of type.
type TypeMarshaler interface {
	// MarshalNomsType returns the Noms Type encoding of a type, or an error.
	// nil is not a valid return val - if both val and err are nil, MarshalType
	// will panic.
	MarshalNomsType() (t *types.Type, err error)
}

var typeOfTypesType = reflect.TypeOf((*types.Type)(nil))
var typeMarshalerInterface = reflect.TypeOf((*TypeMarshaler)(nil)).Elem()

func encodeType(nbf *types.NomsBinFormat, t reflect.Type, seenStructs map[string]reflect.Type, tags nomsTags) (*types.Type, error) {
	if t.Implements(typeMarshalerInterface) {
		v := reflect.Zero(t)
		typ, err := v.Interface().(TypeMarshaler).MarshalNomsType()
		if err != nil {
			return nil, &marshalNomsError{err}
		}
		if typ == nil {
			return nil, fmt.Errorf("nil result from %s.MarshalNomsType", t)
		}
		return typ, nil
	}

	if t.Implements(marshalerInterface) {
		// There is no way to determine the noms type now. For Marshal it can be
		// different each time MarshalNoms is called and is handled further up the
		// stack.
		err := fmt.Errorf("cannot marshal type which implements %s, perhaps implement %s for %s", marshalerInterface, typeMarshalerInterface, t)
		return nil, &marshalNomsError{err}
	}

	if t.Implements(nomsValueInterface) {
		if t == typeOfTypesType {
			return types.TypeType, nil
		}

		// Use Name because List and Blob are convertible to each other on Go.
		switch t.Name() {
		case "Blob":
			return types.BlobType, nil
		case "Bool":
			return types.BoolType, nil
		case "List":
			return types.MakeListType(types.ValueType)
		case "Map":
			return types.MakeMapType(types.ValueType, types.ValueType)
		case "Float":
			return types.FloaTType, nil
		case "Ref":
			return types.MakeRefType(types.ValueType)
		case "Set":
			return types.MakeSetType(types.ValueType)
		case "String":
			return types.StringType, nil
		case "Value":
			return types.ValueType, nil
		}

		return nil, fmt.Errorf("cannot marshal type %s, it requires type parameters", t)
	}

	switch t.Kind() {
	case reflect.Bool:
		return types.BoolType, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return types.FloaTType, nil
	case reflect.String:
		return types.StringType, nil
	case reflect.Struct:
		return structEncodeType(nbf, t, seenStructs)
	case reflect.Array, reflect.Slice:
		elemType, err := encodeType(nbf, t.Elem(), seenStructs, nomsTags{})

		if err != nil {
			return nil, err
		}

		if elemType == nil {
			break
		}

		if shouldEncodeAsSet(t, tags) {
			return types.MakeSetType(elemType)
		}
		return types.MakeListType(elemType)
	case reflect.Map:
		keyType, err := encodeType(nbf, t.Key(), seenStructs, nomsTags{})

		if err != nil {
			return nil, err
		}

		if keyType == nil {
			break
		}

		if shouldEncodeAsSet(t, tags) {
			return types.MakeSetType(keyType)
		}

		valueType, err := encodeType(nbf, t.Elem(), seenStructs, nomsTags{})

		if err != nil {
			return nil, err
		}

		if valueType != nil {
			return types.MakeMapType(keyType, valueType)
		}
	}

	// This will be reported as an error at a different layer.
	return nil, nil
}

// structEncodeType returns the Noms types.Type if it can be determined from the
// reflect.Type. In some cases we cannot determine the type by only looking at
// the type but we also need to look at the value. In these cases this returns
// nil and we have to wait until we have a value to be able to determine the
// type.
func structEncodeType(nbf *types.NomsBinFormat, t reflect.Type, seenStructs map[string]reflect.Type) (*types.Type, error) {
	name := getStructName(t)
	if name != "" {
		if _, ok := seenStructs[name]; ok {
			return types.MakeCycleType(name), nil
		}
		seenStructs[name] = t
	}

	fields, knownShape, _, err := typeFields(nbf, t, seenStructs, true, false)

	if err != nil {
		return nil, err
	}

	var structType *types.Type
	if knownShape {
		structTypeFields := make([]types.StructField, len(fields))
		for i, fs := range fields {
			structTypeFields[i] = types.StructField{
				Name:     fs.name,
				Type:     fs.nomsType,
				Optional: fs.omitEmpty,
			}
		}
		structType, err = types.MakeStructType(getStructName(t), structTypeFields...)

		if err != nil {
			return nil, err
		}
	}

	return structType, err
}
