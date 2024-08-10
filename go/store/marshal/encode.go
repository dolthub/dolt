// Copyright 2019 Dolthub, Inc.
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
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package marshal implements encoding and decoding of Noms values. The mapping
// between Noms objects and Go values is described  in the documentation for the
// Marshal and Unmarshal functions.
package marshal

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/dolthub/dolt/go/store/types"
)

// Marshal converts a Go value to a Noms value.
//
// Marshal traverses the value v recursively. Marshal uses the following
// type-dependent encodings:
//
// Boolean values are encoded as Noms types.Bool.
//
// Floating point and integer values are encoded as Noms types.Float. At the
// moment this might lead to some loss in precision because types.Float
// currently takes a float64.
//
// String values are encoded as Noms types.String.
//
// Slices and arrays are encoded as Noms types.List by default. If a
// field is tagged with `noms:"set", it will be encoded as Noms types.Set
// instead.
//
// Maps are encoded as Noms types.Map, or a types.Set if the value type is
// struct{} and the field is tagged with `noms:"set"`.
//
// Struct values are encoded as Noms structs (types.Struct). Each exported Go
// struct field becomes a member of the Noms struct unless
//   - The field's tag is "-"
//   - The field is empty and its tag specifies the "omitempty" option.
//   - The field has the "original" tag, in which case the field is used as an
//     initial value onto which the fields of the Go type are added. When
//     combined with the corresponding support for "original" in Unmarshal(),
//     this allows one to find and modify any values of a known subtype.
//
// Additionally, user-defined types can implement the Marshaler interface to
// provide a custom encoding.
//
// The empty values are false, 0, any nil pointer or interface value, and any
// array, slice, map, or string of length zero.
//
// The Noms struct default field name is the Go struct field name where the
// first character is lower cased, but can be specified in the Go struct field's
// tag value. The "noms" key in the Go struct field's tag value is the field
// name. Examples:
//
//	// Field is ignored.
//	Field int `noms:"-"`
//
//	// Field appears in a Noms struct as field "myName".
//	MyName int
//
//	// Field appears in a Noms struct as key "myName".
//	Field int `noms:"myName"`
//
//	// Field appears in a Noms struct as key "myName" and the field is
//	//  omitted from the object if its value is empty, as defined above.
//	Field int `noms:"myName,omitempty"
//
//	// Field appears in a Noms struct as key "field" and the field is
//	//  omitted from the object if its value is empty, as defined above.
//	Field int `noms:",omitempty"
//
// The name of the Noms struct is the name of the Go struct where the first
// character is changed to upper case. You can also implement the
// StructNameMarshaler interface to get more control over the actual struct
// name.
//
// Anonymous struct fields are usually marshaled as if their inner exported
// fields were fields in the outer struct, subject to the usual Go visibility.
// An anonymous struct field with a name given in its Noms tag is treated as
// having that name, rather than being anonymous.
//
// Noms values (values implementing types.Value) are copied over without any
// change.
//
// When marshalling interface{} the dynamic type is used.
//
// Go pointers, complex, function are not supported. Attempting to encode such a
// value causes Marshal to return an UnsupportedTypeError.
func Marshal(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	return MarshalOpt(ctx, vrw, v, Opt{})
}

// MarshalOpt is like Marshal but provides additional options.
func MarshalOpt(ctx context.Context, vrw types.ValueReadWriter, v interface{}, opt Opt) (nomsValue types.Value, err error) {
	nomsValue, err = marshalOpt(ctx, vrw, v, opt)
	return nomsValue, err
}

// MustMarshalOpt is like MustMarshal, but with additional options.
func marshalOpt(ctx context.Context, vrw types.ValueReadWriter, v interface{}, opt Opt) (types.Value, error) {
	rv := reflect.ValueOf(v)
	nt := nomsTags{
		set: opt.Set,
	}
	encoder, err := typeEncoder(vrw.Format(), rv.Type(), map[string]reflect.Type{}, nt)

	if err != nil {
		return nil, err
	}

	return encoder(ctx, rv, vrw)
}

// Marshaler is an interface types can implement to provide their own encoding.
type Marshaler interface {
	// MarshalNoms returns the Noms Value encoding of a type, or an error.
	// nil is not a valid return val - if both val and err are nil, Marshal will
	// panic.
	MarshalNoms(vrw types.ValueReadWriter) (val types.Value, err error)
}

// StructNameMarshaler is an interface that can be implemented to define the
// name of a Noms struct.
type StructNameMarshaler interface {
	MarshalNomsStructName() string
}

// UnsupportedTypeError is returned by encode when attempting to encode a type
// that isn't supported.
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

// InvalidTagError is returned by encode and decode when the struct field tag is
// invalid. For example if the field name is not a valid Noms struct field name.
type InvalidTagError struct {
	message string
}

func (e *InvalidTagError) Error() string {
	return e.message
}

// marshalNomsError wraps errors from Marshaler.MarshalNoms. These should be
// unwrapped and never leak to the caller of Marshal.
type marshalNomsError struct {
	err error
}

func (e *marshalNomsError) Error() string {
	return e.err.Error()
}

type Opt struct {
	// Marshal []T or map[T]struct{} to Set<T>, or Unmarhsal Set<T> to map[T]struct{}.
	Set bool
}

type nomsTags struct {
	name      string
	omitEmpty bool
	original  bool
	set       bool
	skip      bool
	hasName   bool
}

var nomsValueInterface = reflect.TypeOf((*types.Value)(nil)).Elem()
var emptyInterface = reflect.TypeOf((*interface{})(nil)).Elem()
var marshalerInterface = reflect.TypeOf((*Marshaler)(nil)).Elem()
var structNameMarshalerInterface = reflect.TypeOf((*StructNameMarshaler)(nil)).Elem()

type encoderFunc func(ctx context.Context, v reflect.Value, vrw types.ValueReadWriter) (types.Value, error)

func boolEncoder(ctx context.Context, v reflect.Value, vrw types.ValueReadWriter) (types.Value, error) {
	return types.Bool(v.Bool()), nil
}

func float64Encoder(ctx context.Context, v reflect.Value, vrw types.ValueReadWriter) (types.Value, error) {
	return types.Float(v.Float()), nil
}

func intEncoder(ctx context.Context, v reflect.Value, vrw types.ValueReadWriter) (types.Value, error) {
	// TODO: encoding types.Int as types.Float is lossy, but will require a migration to change
	return types.Float(float64(v.Int())), nil
}

func uintEncoder(ctx context.Context, v reflect.Value, vrw types.ValueReadWriter) (types.Value, error) {
	// TODO: encoding types.Int as types.Uint is lossy, but will require a migration to change
	return types.Float(float64(v.Uint())), nil
}

func stringEncoder(ctx context.Context, v reflect.Value, vrw types.ValueReadWriter) (types.Value, error) {
	return types.String(v.String()), nil
}

func nomsValueEncoder(ctx context.Context, v reflect.Value, vrw types.ValueReadWriter) (types.Value, error) {
	return v.Interface().(types.Value), nil
}

func marshalerEncoder(t reflect.Type) encoderFunc {
	return func(ctx context.Context, v reflect.Value, vrw types.ValueReadWriter) (types.Value, error) {
		val, err := v.Interface().(Marshaler).MarshalNoms(vrw)
		if err != nil {
			return nil, &marshalNomsError{err}
		}
		if val == nil {
			return nil, fmt.Errorf("nil result from %s.MarshalNoms", t.String())
		}
		return val, nil
	}
}

func typeEncoder(nbf *types.NomsBinFormat, t reflect.Type, seenStructs map[string]reflect.Type, tags nomsTags) (encoderFunc, error) {
	if t.Implements(marshalerInterface) {
		return marshalerEncoder(t), nil
	}

	switch t.Kind() {
	case reflect.Bool:
		return boolEncoder, nil
	case reflect.Float64, reflect.Float32:
		return float64Encoder, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intEncoder, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return uintEncoder, nil
	case reflect.String:
		return stringEncoder, nil
	case reflect.Struct:
		return structEncoder(nbf, t, seenStructs)
	case reflect.Slice, reflect.Array:
		if shouldEncodeAsSet(t, tags) {
			return setFromListEncoder(nbf, t, seenStructs)
		}
		return listEncoder(nbf, t, seenStructs)
	case reflect.Map:
		if shouldEncodeAsSet(t, tags) {
			return setEncoder(nbf, t, seenStructs)
		}
		return mapEncoder(nbf, t, seenStructs)
	case reflect.Interface:
		return func(ctx context.Context, v reflect.Value, vrw types.ValueReadWriter) (types.Value, error) {
			// Get the dynamic type.
			v2 := reflect.ValueOf(v.Interface())
			encFunc, err := typeEncoder(nbf, v2.Type(), seenStructs, tags)

			if err != nil {
				return nil, err
			}

			return encFunc(ctx, v2, vrw)
		}, nil
	case reflect.Ptr:
		// Allow implementations of types.Value (like *types.Type)
		if t.Implements(nomsValueInterface) {
			return nomsValueEncoder, nil
		}
		fallthrough
	default:
		return nil, &UnsupportedTypeError{Type: t}
	}
}

func getStructName(t reflect.Type) string {
	if t.Implements(structNameMarshalerInterface) {
		v := reflect.Zero(t)
		return v.Interface().(StructNameMarshaler).MarshalNomsStructName()
	}
	return strings.Title(t.Name())
}

func structEncoder(nbf *types.NomsBinFormat, t reflect.Type, seenStructs map[string]reflect.Type) (encoderFunc, error) {
	if t.Implements(nomsValueInterface) {
		return nomsValueEncoder, nil
	}

	e := encoderCache.get(t)
	if e != nil {
		return e, nil
	}

	structName := getStructName(t)

	seenStructs[t.Name()] = t
	fields, knownShape, originalFieldIndex, err := typeFields(nbf, t, seenStructs, false, false)

	if err != nil {
		return nil, err
	}

	if knownShape {
		fieldNames := make([]string, len(fields))
		for i, f := range fields {
			fieldNames[i] = f.name
		}

		structTemplate := types.MakeStructTemplate(structName, fieldNames)
		e = func(ctx context.Context, v reflect.Value, vrw types.ValueReadWriter) (types.Value, error) {
			values := make(types.ValueSlice, len(fields))
			for i, f := range fields {
				var err error
				values[i], err = f.encoder(ctx, v.FieldByIndex(f.index), vrw)

				if err != nil {
					return nil, err
				}
			}
			return structTemplate.NewStruct(nbf, values)
		}
	} else if originalFieldIndex == nil {
		// Slower path: cannot precompute the Noms type since there are Noms collections,
		// but at least there are a set number of fields.
		e = func(ctx context.Context, v reflect.Value, vrw types.ValueReadWriter) (types.Value, error) {
			data := make(types.StructData, len(fields))
			for _, f := range fields {
				fv := v.FieldByIndex(f.index)
				if !fv.IsValid() || f.omitEmpty && isEmptyValue(fv) {
					continue
				}
				var err error
				data[f.name], err = f.encoder(ctx, fv, vrw)

				if err != nil {
					return nil, err
				}
			}
			return types.NewStruct(nbf, structName, data)
		}
	} else {
		// Slowest path - we are extending some other struct. We need to start with the
		// type of that struct and extend.
		e = func(ctx context.Context, v reflect.Value, vrw types.ValueReadWriter) (types.Value, error) {
			fv := v.FieldByIndex(originalFieldIndex)
			ret := fv.Interface().(types.Struct)
			if ret.IsZeroValue() {
				var err error
				ret, err = types.NewStruct(nbf, structName, nil)
				if err != nil {
					return nil, err
				}
			}
			for _, f := range fields {
				fv := v.FieldByIndex(f.index)
				if !fv.IsValid() || f.omitEmpty && isEmptyValue(fv) {
					continue
				}

				encVal, err := f.encoder(ctx, fv, vrw)
				if err != nil {
					return nil, err
				}

				ret, err = ret.Set(f.name, encVal)
				if err != nil {
					return nil, err
				}
			}
			return ret, nil
		}
	}

	encoderCache.set(t, e)
	return e, nil
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
		return reflect.DeepEqual(z.Interface(), v.Interface())
	case reflect.Interface:
		return v.IsNil()
	}
	return false
}

type field struct {
	name      string
	encoder   encoderFunc
	index     []int
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

// Separate Set encoder cache because the same type with and without the
// `noms:",set"` tag encode differently (Set vs Map).
var setEncoderCache = &encoderCacheT{}

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

func getTags(f reflect.StructField) (tags nomsTags, err error) {
	reflectTags := f.Tag.Get("noms")
	if reflectTags == "-" {
		tags.skip = true
		return
	}

	tagsSlice := strings.Split(reflectTags, ",")

	// The first tag is always the name, or empty to use the field as the name.
	if len(tagsSlice) == 0 || tagsSlice[0] == "" {
		tags.name = strings.ToLower(f.Name[:1]) + f.Name[1:]
	} else {
		tags.name = tagsSlice[0]
		tags.hasName = true
	}

	if !types.IsValidStructFieldName(tags.name) {
		return nomsTags{}, &InvalidTagError{"Invalid struct field name: " + tags.name}
	}

	for i := 1; i < len(tagsSlice); i++ {
		switch tag := tagsSlice[i]; tag {
		case "omitempty":
			tags.omitEmpty = true
		case "original":
			tags.original = true
		case "set":
			tags.set = true
		default:
			return nomsTags{}, &InvalidTagError{"Unrecognized tag: " + tag}
		}
	}
	return
}

func validateField(f reflect.StructField, t reflect.Type) error {
	// PkgPath is the package path that qualifies a lower case (unexported)
	// field name. It is empty for upper case (exported) field names.
	// See https://golang.org/ref/spec#Uniqueness_of_identifiers
	if f.PkgPath != "" && !f.Anonymous { // unexported
		return &UnsupportedTypeError{t, "Non exported fields are not supported"}
	}

	return nil
}

func typeFields(nbf *types.NomsBinFormat, t reflect.Type, seenStructs map[string]reflect.Type, computeType, embedded bool) (fields fieldSlice, knownShape bool, originalFieldIndex []int, err error) {
	knownShape = true
	for i := 0; i < t.NumField(); i++ {
		index := make([]int, 1)
		index[0] = i
		f := t.Field(i)
		tags, err := getTags(f)

		if err != nil {
			return nil, false, nil, err
		}

		if tags.skip {
			continue
		}

		if tags.original {
			originalFieldIndex = f.Index
			continue
		}

		if f.Anonymous && f.PkgPath == "" && !tags.hasName {
			embeddedFields, embeddedKnownShape, embeddedOriginalFieldIndex, err := typeFields(nbf, f.Type, seenStructs, computeType, true)

			if err != nil {
				return nil, false, nil, err
			}

			if embeddedOriginalFieldIndex != nil {
				originalFieldIndex = append(index, embeddedOriginalFieldIndex...)
			}
			knownShape = knownShape && embeddedKnownShape

			for _, ef := range embeddedFields {
				ef.index = append(index, ef.index...)
				fields = append(fields, ef)
			}

			continue
		}

		var nt *types.Type
		err = validateField(f, t)

		if err != nil {
			return nil, false, nil, err
		}

		if computeType {
			var err error
			nt, err = encodeType(nbf, f.Type, seenStructs, tags)

			if err != nil {
				return nil, false, nil, err
			}

			if nt == nil {
				knownShape = false
			}
		}

		if tags.omitEmpty && !computeType {
			knownShape = false
		}

		encFunc, err := typeEncoder(nbf, f.Type, seenStructs, tags)

		if err != nil {
			return nil, false, nil, err
		}

		fields = append(fields, field{
			name:      tags.name,
			encoder:   encFunc,
			index:     index,
			nomsType:  nt,
			omitEmpty: tags.omitEmpty,
		})
	}

	if !embedded {
		sort.Sort(fields)
	}

	return fields, knownShape, originalFieldIndex, err
}

func listEncoder(nbf *types.NomsBinFormat, t reflect.Type, seenStructs map[string]reflect.Type) (encoderFunc, error) {
	e := encoderCache.get(t)
	if e != nil {
		return e, nil
	}

	var elemEncoder encoderFunc
	// lock e until encoder(s) are initialized
	var init sync.RWMutex
	init.Lock()
	defer init.Unlock()
	e = func(ctx context.Context, v reflect.Value, vrw types.ValueReadWriter) (types.Value, error) {
		init.RLock()
		defer init.RUnlock()
		values := make([]types.Value, v.Len())
		for i := 0; i < v.Len(); i++ {
			var err error
			values[i], err = elemEncoder(ctx, v.Index(i), vrw)

			if err != nil {
				return nil, err
			}
		}
		return types.NewList(ctx, vrw, values...)
	}

	encoderCache.set(t, e)
	var err error
	elemEncoder, err = typeEncoder(nbf, t.Elem(), seenStructs, nomsTags{})

	if err != nil {
		return nil, err
	}

	return e, nil
}

// Encode set from array or slice
func setFromListEncoder(nbf *types.NomsBinFormat, t reflect.Type, seenStructs map[string]reflect.Type) (encoderFunc, error) {
	e := setEncoderCache.get(t)
	if e != nil {
		return e, nil
	}

	var elemEncoder encoderFunc
	// lock e until encoder(s) are initialized
	var init sync.RWMutex
	init.Lock()
	defer init.Unlock()
	e = func(ctx context.Context, v reflect.Value, vrw types.ValueReadWriter) (types.Value, error) {
		init.RLock()
		defer init.RUnlock()
		values := make([]types.Value, v.Len())
		for i := 0; i < v.Len(); i++ {
			var err error
			values[i], err = elemEncoder(ctx, v.Index(i), vrw)

			if err != nil {
				return nil, err
			}
		}
		return types.NewSet(ctx, vrw, values...)
	}

	setEncoderCache.set(t, e)

	var err error
	elemEncoder, err = typeEncoder(nbf, t.Elem(), seenStructs, nomsTags{})

	if err != nil {
		return nil, err
	}

	return e, err
}

func setEncoder(nbf *types.NomsBinFormat, t reflect.Type, seenStructs map[string]reflect.Type) (encoderFunc, error) {
	e := setEncoderCache.get(t)
	if e != nil {
		return e, nil
	}

	var encoder encoderFunc
	// lock e until encoder(s) are initialized
	var init sync.RWMutex
	init.Lock()
	defer init.Unlock()
	e = func(ctx context.Context, v reflect.Value, vrw types.ValueReadWriter) (types.Value, error) {
		init.RLock()
		defer init.RUnlock()
		values := make([]types.Value, v.Len())
		for i, k := range v.MapKeys() {
			var err error
			values[i], err = encoder(ctx, k, vrw)

			if err != nil {
				return nil, err
			}
		}
		return types.NewSet(ctx, vrw, values...)
	}

	setEncoderCache.set(t, e)

	var err error
	encoder, err = typeEncoder(nbf, t.Key(), seenStructs, nomsTags{})

	if err != nil {
		return nil, err
	}

	return e, nil
}

func mapEncoder(nbf *types.NomsBinFormat, t reflect.Type, seenStructs map[string]reflect.Type) (encoderFunc, error) {
	e := encoderCache.get(t)
	if e != nil {
		return e, nil
	}

	var keyEncoder encoderFunc
	var valueEncoder encoderFunc
	// lock e until encoder(s) are initialized
	var init sync.RWMutex
	init.Lock()
	defer init.Unlock()
	e = func(ctx context.Context, v reflect.Value, vrw types.ValueReadWriter) (types.Value, error) {
		init.RLock()
		defer init.RUnlock()
		keys := v.MapKeys()
		kvs := make([]types.Value, 2*len(keys))
		for i, k := range keys {
			var err error
			kvs[2*i], err = keyEncoder(ctx, k, vrw)

			if err != nil {
				return nil, err
			}

			kvs[2*i+1], err = valueEncoder(ctx, v.MapIndex(k), vrw)

			if err != nil {
				return nil, err
			}
		}
		return types.NewMap(ctx, vrw, kvs...)
	}

	encoderCache.set(t, e)

	var err error
	keyEncoder, err = typeEncoder(nbf, t.Key(), seenStructs, nomsTags{})

	if err != nil {
		return nil, err
	}

	valueEncoder, err = typeEncoder(nbf, t.Elem(), seenStructs, nomsTags{})

	if err != nil {
		return nil, err
	}

	return e, nil
}

func shouldEncodeAsSet(t reflect.Type, tags nomsTags) bool {
	switch t.Kind() {
	case reflect.Slice, reflect.Array:
		return tags.set
	case reflect.Map:
		// map[T]struct{} `noms:,"set"`
		return tags.set &&
			t.Elem().Kind() == reflect.Struct &&
			t.Elem().NumField() == 0
	default:
		panic(fmt.Errorf("called with unexpectededed kind %v", t.Kind()))
	}
}
