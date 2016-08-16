// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package marshal implements encoding and decoding of Noms values. The mapping between Noms objects and Go values is described  in the documentation for the Marshal and Unmarshal functions.
package marshal

import (
	"reflect"
	"sort"
	"strings"
	"sync"
	"unicode"

	"github.com/attic-labs/noms/go/types"
)

// Marshal converts a Go value to a Noms value.
//
// Marshal traverses the value v recursively. Marshal uses the following type-dependent encodings:
//
// Boolean values are encoded as Noms types.Bool.
//
// Floating point and integer values are encoded as Noms types.Number. At the moment this might lead to some loss in precision because types.Number currently takes a float64.
//
// String values are encoded as Noms types.String.
//
// Struct values are encoded as Noms structs (types.Struct). Each exported Go struct field becomes a member of the Noms struct unless
//   - the field's tag is "-"
// The Noms struct default field name is the Go struct field name where the first character is lower cased,
// but can be specified in the Go struct field's tag value. The "noms" key in
// the Go struct field's tag value is the field name. Examples:
//
//   // Field is ignored.
//   Field int `noms:"-"`
//
//   // Field appears in a Noms struct as field "myName".
//   MyName int
//
//   // Field appears in a Noms struct as key "myName".
//   Field int `noms:"myName"`
//
// Unlike encoding/json Marshal, this does not support "omitempty".
//
// Anonymous struct fields are currently not supported.
//
// Embedded structs are currently not supported (which is the same as anonymous struct fields).
//
// Noms values (values implementing types.Value) are copied over without any change.
//
// Go maps, slices, arrays, pointers, complex, interface (non types.Value), function are not supported. Attempting to encode such a value causes Marshal to return an UnsupportedTypeError.
//
func Marshal(v interface{}) (nomsValue types.Value, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch r.(type) {
			case *UnsupportedTypeError, *InvalidTagError:
				err = r.(error)
				return
			}
			panic(r)
		}
	}()
	rv := reflect.ValueOf(v)
	encoder := typeEncoder(rv.Type())
	nomsValue = encoder(rv)
	return
}

// UnsupportedTypeError is returned by encode when attempting to encode a type that isn't supported.
type UnsupportedTypeError struct {
	Type    reflect.Type
	Message string
}

func (e *UnsupportedTypeError) Error() string {
	msg := e.Message
	if msg == "" {
		msg = "Type is not supported"
	}
	return msg + ", type: " + e.Type.String()
}

// InvalidTagError is returned by encode and decode when the struct field tag is invalid. For example if the field name is not a valid Noms struct field name.
type InvalidTagError struct {
	message string
}

func (e *InvalidTagError) Error() string {
	return e.message
}

var nomsValueInterface = reflect.TypeOf((*types.Value)(nil)).Elem()

type encoderFunc func(v reflect.Value) types.Value

func boolEncoder(v reflect.Value) types.Value {
	return types.Bool(v.Bool())
}

func float64Encoder(v reflect.Value) types.Value {
	return types.Number(v.Float())
}

func intEncoder(v reflect.Value) types.Value {

	return types.Number(float64(v.Int()))
}
func uintEncoder(v reflect.Value) types.Value {
	return types.Number(float64(v.Uint()))
}

func stringEncoder(v reflect.Value) types.Value {
	return types.String(v.String())
}

func nomsValueEncoder(v reflect.Value) types.Value {
	return v.Interface().(types.Value)
}

func typeEncoder(t reflect.Type) encoderFunc {
	switch t.Kind() {
	case reflect.Bool:
		return boolEncoder
	case reflect.Float64, reflect.Float32:
		return float64Encoder
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intEncoder
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return uintEncoder
	case reflect.String:
		return stringEncoder
	case reflect.Struct:
		return structEncoder(t)
	default:
		panic(&UnsupportedTypeError{Type: t})
	}
}

func structEncoder(t reflect.Type) encoderFunc {
	if t.Implements(nomsValueInterface) {
		return nomsValueEncoder
	}

	structEncoderCache.RLock()
	e := structEncoderCache.m[t]
	structEncoderCache.RUnlock()
	if e != nil {
		return e
	}

	fields, structType := typeFields(t)
	if structType != nil {
		e = func(v reflect.Value) types.Value {
			values := make([]types.Value, len(fields))
			for i, f := range fields {
				values[i] = f.encoder(v.Field(f.index))
			}
			return types.NewStructWithType(structType, values)
		}
	} else {
		// Cannot precompute the type since there are noms collections.
		name := t.Name()
		e = func(v reflect.Value) types.Value {
			data := make(types.StructData, len(fields))
			for _, f := range fields {
				data[f.name] = f.encoder(v.Field(f.index))
			}
			return types.NewStruct(name, data)
		}
	}

	structEncoderCache.Lock()
	if structEncoderCache.m == nil {
		structEncoderCache.m = map[reflect.Type]encoderFunc{}
	}
	structEncoderCache.m[t] = e
	structEncoderCache.Unlock()

	return e
}

type field struct {
	name     string
	encoder  encoderFunc
	index    int
	nomsType *types.Type
}

type fieldSlice []field

func (fs fieldSlice) Len() int           { return len(fs) }
func (fs fieldSlice) Swap(i, j int)      { fs[i], fs[j] = fs[j], fs[i] }
func (fs fieldSlice) Less(i, j int) bool { return fs[i].name < fs[j].name }

var structEncoderCache struct {
	sync.RWMutex
	m map[reflect.Type]encoderFunc
}

func getFieldName(fieldName string, f reflect.StructField) string {
	if fieldName == "" {
		fieldName = strings.ToLower(f.Name[:1]) + f.Name[1:]
	}
	if !types.IsValidStructFieldName(fieldName) {
		panic(&InvalidTagError{"Invalid struct field name: " + fieldName})
	}
	return fieldName
}

func validateField(f reflect.StructField, t reflect.Type) {
	if f.Anonymous {
		panic(&UnsupportedTypeError{t, "Embedded structs are not supported"})
	}
	if unicode.IsLower(rune(f.Name[0])) { // we only allow ascii so this is fine
		panic(&UnsupportedTypeError{t, "Non exported fields are not supported"})
	}
}

func typeFields(t reflect.Type) (fields fieldSlice, structType *types.Type) {
	canComputeStructType := true
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		validateField(f, t)
		nt := nomsType(f.Type)
		if nt == nil {
			canComputeStructType = false
		}
		tags := f.Tag.Get("noms")
		if tags == "-" {
			continue
		}

		fields = append(fields, field{
			name:     getFieldName(tags, f),
			encoder:  typeEncoder(f.Type),
			index:    i,
			nomsType: nt,
		})
	}
	sort.Sort(fields)
	if canComputeStructType {
		fieldNames := make([]string, len(fields))
		fieldTypes := make([]*types.Type, len(fields))
		for i, fs := range fields {
			fieldNames[i] = fs.name
			fieldTypes[i] = fs.nomsType
		}
		structType = types.MakeStructType(t.Name(), fieldNames, fieldTypes)
	}
	return
}

func nomsType(t reflect.Type) *types.Type {
	switch t.Kind() {
	case reflect.Bool:
		return types.BoolType
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return types.NumberType
	case reflect.String:
		return types.StringType
	case reflect.Struct:
		return structNomsType(t)
	default:
		// This will be reported as an error at a different layer.
		return nil
	}
}

// structNomsType returns the noms types.Type if it can be determined from the reflect.Type. Note that we can only determine the type for a subset of noms types since the Go type does not fully reflect it. In this cases this returns nil and we have to wait until we have a value to be able to determine the type.
func structNomsType(t reflect.Type) *types.Type {
	if t.Implements(nomsValueInterface) {
		// Use Name because List and Blob are convertible to each other on Go.
		switch t.Name() {
		case "Blob":
			return types.BlobType
		case "Bool":
			return types.BoolType
		case "Number":
			return types.NumberType
		case "String":
			return types.StringType
		}
		// The rest of the noms types need the value to get the exact type.
		return nil
	}

	_, structType := typeFields(t)
	return structType
}
