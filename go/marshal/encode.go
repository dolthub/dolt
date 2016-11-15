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

	"github.com/attic-labs/noms/go/d"
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
// Slices and arrays are encoded as Noms types.List.
//
// Maps are encoded as Noms types.Map
//
// Struct values are encoded as Noms structs (types.Struct). Each exported Go struct field becomes a member of the Noms struct unless
//   - the field's tag is "-"
//   - the field is empty and its tag specifies the "omitempty" option.
//
// The empty values are false, 0, any nil pointer or interface value, and any array, slice, map, or string of length zero.
//
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
//   // Field appears in a Noms struct as key "myName" and the field is
//   //  omitted from the object if its value is empty, as defined above.
//   Field int `noms:"myName,omitempty"
//
//   // Field appears in a Noms struct as key "field" and the field is
//   //  omitted from the object if its value is empty, as defined above.
//   Field int `noms:",omitempty"
//
// The name of the Noms struct is the name of the Go struct where the first character is changed to upper case.
//
// Anonymous struct fields are currently not supported.
//
// Embedded structs are currently not supported (which is the same as anonymous struct fields).
//
// Noms values (values implementing types.Value) are copied over without any change.
//
// When marshalling `interface{}` the dynamic type is used.
//
// Go pointers, complex, function are not supported. Attempting to encode such a value causes Marshal to return an UnsupportedTypeError.
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
	encoder := typeEncoder(rv.Type(), nil)
	nomsValue = encoder(rv)
	return
}

// Marshals a Go value to a Noms value using the same rules as Marshal(). Panics on failure.
func MustMarshal(v interface{}) types.Value {
	r, err := Marshal(v)
	d.Chk.NoError(err)
	return r
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
var emptyInterface = reflect.TypeOf((*interface{})(nil)).Elem()

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

func typeEncoder(t reflect.Type, parentStructTypes []reflect.Type) encoderFunc {
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
		return structEncoder(t, parentStructTypes)
	case reflect.Slice, reflect.Array:
		return listEncoder(t, parentStructTypes)
	case reflect.Map:
		return mapEncoder(t, parentStructTypes)
	case reflect.Interface:
		return func(v reflect.Value) types.Value {
			// Get the dynamic type.
			v2 := reflect.ValueOf(v.Interface())
			return typeEncoder(v2.Type(), parentStructTypes)(v2)
		}
	default:
		panic(&UnsupportedTypeError{Type: t})
	}
}

func structEncoder(t reflect.Type, parentStructTypes []reflect.Type) encoderFunc {
	if t.Implements(nomsValueInterface) {
		return nomsValueEncoder
	}

	e := encoderCache.get(t)
	if e != nil {
		return e
	}

	parentStructTypes = append(parentStructTypes, t)
	fields, structType := typeFields(t, parentStructTypes)
	if structType != nil {
		e = func(v reflect.Value) types.Value {
			values := make([]types.Value, len(fields))
			for i, f := range fields {
				values[i] = f.encoder(v.Field(f.index))
			}
			return types.NewStructWithType(structType, values)
		}
	} else {
		// Cannot precompute the Noms type since there are Noms collections.
		name := strings.Title(t.Name())
		e = func(v reflect.Value) types.Value {
			data := make(types.StructData, len(fields))
			for _, f := range fields {
				fv := v.Field(f.index)
				if !fv.IsValid() || f.omitEmpty && isEmptyValue(fv) {
					continue
				}
				data[f.name] = f.encoder(fv)
			}
			return types.NewStruct(name, data)
		}
	}

	encoderCache.set(t, e)
	return e
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Struct:
		z := reflect.Zero(v.Type())
		return z.Interface() == v.Interface()
	case reflect.Interface:
		return v.IsNil()
	}
	return false
}

type field struct {
	name      string
	encoder   encoderFunc
	index     int
	nomsType  *types.Type
	omitEmpty bool
}

type fieldSlice []field

func (fs fieldSlice) Len() int           { return len(fs) }
func (fs fieldSlice) Swap(i, j int)      { fs[i], fs[j] = fs[j], fs[i] }
func (fs fieldSlice) Less(i, j int) bool { return fs[i].name < fs[j].name }

type encoderCacheT struct {
	sync.RWMutex
	m map[reflect.Type]encoderFunc
}

var encoderCache = &encoderCacheT{}

func (c *encoderCacheT) get(t reflect.Type) encoderFunc {
	c.RLock()
	defer c.RUnlock()
	return c.m[t]
}

func (c *encoderCacheT) set(t reflect.Type, e encoderFunc) {
	c.Lock()
	defer c.Unlock()
	if c.m == nil {
		c.m = map[reflect.Type]encoderFunc{}
	}
	c.m[t] = e
}

func parseTags(tags string, f reflect.StructField) (name string, omitEmpty bool) {
	idx := strings.Index(tags, ",")
	if tags == "" || idx == 0 {
		name = strings.ToLower(f.Name[:1]) + f.Name[1:]
	} else if idx == -1 {
		name = tags
	} else {
		name = tags[:idx]
	}

	if !types.IsValidStructFieldName(name) {
		panic(&InvalidTagError{"Invalid struct field name: " + name})
	}

	if idx != -1 {
		// This is pretty simplistic but it is good enough for now.
		omitEmpty = tags[idx+1:] == "omitempty"
	}

	return
}

func validateField(f reflect.StructField, t reflect.Type) {
	if f.Anonymous {
		panic(&UnsupportedTypeError{t, "Embedded structs are not supported"})
	}
	if unicode.IsLower(rune(f.Name[0])) { // we only allow ascii so this is fine
		panic(&UnsupportedTypeError{t, "Non exported fields are not supported"})
	}
}

func typeFields(t reflect.Type, parentStructTypes []reflect.Type) (fields fieldSlice, structType *types.Type) {
	canComputeStructType := true
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		validateField(f, t)
		nt := nomsType(f.Type, parentStructTypes)
		if nt == nil {
			canComputeStructType = false
		}
		tags := f.Tag.Get("noms")
		if tags == "-" {
			continue
		}

		name, omitEmpty := parseTags(tags, f)
		if omitEmpty {
			canComputeStructType = false
		}
		fields = append(fields, field{
			name:      name,
			encoder:   typeEncoder(f.Type, parentStructTypes),
			index:     i,
			nomsType:  nt,
			omitEmpty: omitEmpty,
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
		structType = types.MakeStructType(strings.Title(t.Name()), fieldNames, fieldTypes)
	}
	return
}

func nomsType(t reflect.Type, parentStructTypes []reflect.Type) *types.Type {
	switch t.Kind() {
	case reflect.Bool:
		return types.BoolType
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return types.NumberType
	case reflect.String:
		return types.StringType
	case reflect.Struct:
		return structNomsType(t, parentStructTypes)
	case reflect.Array, reflect.Slice:
		elemType := nomsType(t.Elem(), parentStructTypes)
		if elemType != nil {
			return types.MakeListType(elemType)
		}
	}
	// This will be reported as an error at a different layer.
	return nil
}

// structNomsType returns the Noms types.Type if it can be determined from the reflect.Type. Note that we can only determine the type for a subset of Noms types since the Go type does not fully reflect it. In this cases this returns nil and we have to wait until we have a value to be able to determine the type.
func structNomsType(t reflect.Type, parentStructTypes []reflect.Type) *types.Type {
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
		// The rest of the Noms types need the value to get the exact type.
		return nil
	}

	for i, pst := range parentStructTypes {
		if pst == t {
			return types.MakeCycleType(uint32(i))
		}
	}

	_, structType := typeFields(t, parentStructTypes)
	return structType
}

func listEncoder(t reflect.Type, parentStructTypes []reflect.Type) encoderFunc {
	e := encoderCache.get(t)
	if e != nil {
		return e
	}

	var elemEncoder encoderFunc
	e = func(v reflect.Value) types.Value {
		values := make([]types.Value, v.Len(), v.Len())
		for i := 0; i < v.Len(); i++ {
			values[i] = elemEncoder(v.Index(i))
		}
		return types.NewList(values...)
	}

	encoderCache.set(t, e)
	elemEncoder = typeEncoder(t.Elem(), parentStructTypes)
	return e
}

func mapEncoder(t reflect.Type, parentStructTypes []reflect.Type) encoderFunc {
	e := encoderCache.get(t)
	if e != nil {
		return e
	}

	var keyEncoder encoderFunc
	var valueEncoder encoderFunc
	e = func(v reflect.Value) types.Value {
		keys := v.MapKeys()
		kvs := make([]types.Value, 2*len(keys))
		for i, k := range keys {
			kvs[2*i] = keyEncoder(k)
			kvs[2*i+1] = valueEncoder(v.MapIndex(k))
		}
		return types.NewMap(kvs...)
	}

	encoderCache.set(t, e)
	keyEncoder = typeEncoder(t.Key(), parentStructTypes)
	valueEncoder = typeEncoder(t.Elem(), parentStructTypes)
	return e
}
