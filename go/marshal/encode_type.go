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

	"github.com/attic-labs/noms/go/types"
)

// MarshalType computes a Noms type from a Go type
//
// The rules for MarshalType is the same as for Marshal, except for omitempty
// which leads to an optional field since it depends on the runtime value and
// can lead to the property not being present.
//
// If a Go struct contains a noms tag with original the field is skipped since
// the Noms type depends on the original Noms value which is not available.
func MarshalType(v interface{}) (nt *types.Type, err error) {
	return MarshalTypeOpt(v, Opt{})
}

// MarshalTypeOpt is like MarshalType but with additional options.
func MarshalTypeOpt(v interface{}, opt Opt) (nt *types.Type, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch r := r.(type) {
			case *UnsupportedTypeError, *InvalidTagError:
				err = r.(error)
			case *marshalNomsError:
				err = r.err
			default:
				panic(r)
			}
		}
	}()
	nt = MustMarshalTypeOpt(v, opt)
	return
}

// MustMarshalType computes a Noms type from a Go type or panics if there is an
// error.
func MustMarshalType(v interface{}) (nt *types.Type) {
	return MustMarshalTypeOpt(v, Opt{})
}

// MustMarshalTypeOpt is like MustMarshalType but provides additional options.
func MustMarshalTypeOpt(v interface{}, opt Opt) (nt *types.Type) {
	rv := reflect.ValueOf(v)
	tags := nomsTags{
		set: opt.Set,
	}
	nt = encodeType(rv.Type(), map[string]reflect.Type{}, tags)

	if nt == nil {
		panic(&UnsupportedTypeError{Type: rv.Type()})
	}

	return
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

func encodeType(t reflect.Type, seenStructs map[string]reflect.Type, tags nomsTags) *types.Type {
	if t.Implements(typeMarshalerInterface) {
		v := reflect.Zero(t)
		typ, err := v.Interface().(TypeMarshaler).MarshalNomsType()
		if err != nil {
			panic(&marshalNomsError{err})
		}
		if typ == nil {
			panic(fmt.Errorf("nil result from %s.MarshalNomsType", t))
		}
		return typ
	}

	if t.Implements(marshalerInterface) {
		// There is no way to determine the noms type now. For Marshal it can be
		// different each time MarshalNoms is called and is handled further up the
		// stack.
		err := fmt.Errorf("Cannot marshal type which implements %s, perhaps implement %s for %s", marshalerInterface, typeMarshalerInterface, t)
		panic(&marshalNomsError{err})
	}

	if t.Implements(nomsValueInterface) {
		if t == typeOfTypesType {
			return types.TypeType
		}

		// Use Name because List and Blob are convertible to each other on Go.
		switch t.Name() {
		case "Blob":
			return types.BlobType
		case "Bool":
			return types.BoolType
		case "List":
			return types.MakeListType(types.ValueType)
		case "Map":
			return types.MakeMapType(types.ValueType, types.ValueType)
		case "Number":
			return types.NumberType
		case "Ref":
			return types.MakeRefType(types.ValueType)
		case "Set":
			return types.MakeSetType(types.ValueType)
		case "String":
			return types.StringType
		case "Value":
			return types.ValueType
		}

		err := fmt.Errorf("Cannot marshal type %s, it requires type parameters", t)
		panic(&marshalNomsError{err})
	}

	switch t.Kind() {
	case reflect.Bool:
		return types.BoolType
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return types.NumberType
	case reflect.String:
		return types.StringType
	case reflect.Struct:
		return structEncodeType(t, seenStructs)
	case reflect.Array, reflect.Slice:
		elemType := encodeType(t.Elem(), seenStructs, nomsTags{})
		if elemType == nil {
			break
		}
		if shouldEncodeAsSet(t, tags) {
			return types.MakeSetType(elemType)
		}
		return types.MakeListType(elemType)
	case reflect.Map:
		keyType := encodeType(t.Key(), seenStructs, nomsTags{})
		if keyType == nil {
			break
		}

		if shouldEncodeAsSet(t, tags) {
			return types.MakeSetType(keyType)
		}

		valueType := encodeType(t.Elem(), seenStructs, nomsTags{})
		if valueType != nil {
			return types.MakeMapType(keyType, valueType)
		}
	}

	// This will be reported as an error at a different layer.
	return nil
}

// structEncodeType returns the Noms types.Type if it can be determined from the
// reflect.Type. In some cases we cannot determine the type by only looking at
// the type but we also need to look at the value. In these cases this returns
// nil and we have to wait until we have a value to be able to determine the
// type.
func structEncodeType(t reflect.Type, seenStructs map[string]reflect.Type) *types.Type {
	name := getStructName(t)
	if name != "" {
		if _, ok := seenStructs[name]; ok {
			return types.MakeCycleType(name)
		}
		seenStructs[name] = t
	}

	fields, knownShape, _ := typeFields(t, seenStructs, true, false)

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
		structType = types.MakeStructType(getStructName(t), structTypeFields...)
	}

	return structType
}
