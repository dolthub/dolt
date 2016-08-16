// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package marshal

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/attic-labs/noms/go/types"
)

// Unmarshal converts a Noms value into a Go value. It decodes v and stores the result
// in the value pointed to by out.
//
// Unmarshal uses the inverse of the encodings that Marshal uses with the following additional rules:
//
// To unmarshal a Noms struct into a Go struct, Unmarshal matches incoming object
// fields to the fields used by Marshal (either the struct field name or its tag).
// Unmarshal will only set exported fields of the struct.
//
// If a Noms value is not appropriate for a given target type, or if a Noms number overflows the target type, Unmarshal returns a UnmarshalTypeMismatchError.
//
func Unmarshal(v types.Value, out interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch r.(type) {
			case *UnmarshalTypeMismatchError, *UnsupportedTypeError, *InvalidTagError:
				err = r.(error)
				return
			}
			panic(r)
		}
	}()

	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return &InvalidUnmarshalError{reflect.TypeOf(out)}
	}
	rv = rv.Elem()
	d := typeDecoder(rv.Type())
	d(v, rv)
	return
}

// InvalidUnmarshalError describes an invalid argument passed to Unmarshal. (The argument to Unmarshal must be a non-nil pointer.)
type InvalidUnmarshalError struct {
	Type reflect.Type
}

func (e *InvalidUnmarshalError) Error() string {
	if e.Type == nil {
		return "Cannot unmarshal into Go nil value"
	}

	if e.Type.Kind() != reflect.Ptr {
		return "Cannot unmarshal into Go non pointer of type " + e.Type.String()
	}
	return "Cannot unmarshal into Go nil pointer of type " + e.Type.String()
}

// UnmarshalTypeMismatchError describes a Noms value that was not appropriate for a value of a specific Go type.
type UnmarshalTypeMismatchError struct {
	Value   types.Value
	Type    reflect.Type // type of Go value it could not be assigned to
	details string
}

func (e *UnmarshalTypeMismatchError) Error() string {
	return fmt.Sprintf("Cannot unmarshal %s into Go value of type %s%s", e.Value.Type().Describe(), e.Type.String(), e.details)
}

func overflowError(v types.Number, t reflect.Type) *UnmarshalTypeMismatchError {
	return &UnmarshalTypeMismatchError{v, t, fmt.Sprintf(" (%g does not fit in %s)", v, t)}
}

type decoderFunc func(v types.Value, rv reflect.Value)

func typeDecoder(t reflect.Type) decoderFunc {
	switch t.Kind() {
	case reflect.Bool:
		return boolDecoder
	case reflect.Float32, reflect.Float64:
		return floatDecoder
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intDecoder
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return uintDecoder
	case reflect.String:
		return stringDecoder
	case reflect.Struct, reflect.Interface:
		return structDecoder(t)
	default:
		panic(&UnsupportedTypeError{Type: t})
	}
}
func boolDecoder(v types.Value, rv reflect.Value) {
	if b, ok := v.(types.Bool); ok {
		rv.SetBool(bool(b))
	} else {
		panic(&UnmarshalTypeMismatchError{v, rv.Type(), ""})
	}
}

func stringDecoder(v types.Value, rv reflect.Value) {
	if s, ok := v.(types.String); ok {
		rv.SetString(string(s))
	} else {
		panic(&UnmarshalTypeMismatchError{v, rv.Type(), ""})
	}
}

func floatDecoder(v types.Value, rv reflect.Value) {
	if n, ok := v.(types.Number); ok {
		rv.SetFloat(float64(n))
	} else {
		panic(&UnmarshalTypeMismatchError{v, rv.Type(), ""})
	}
}

func intDecoder(v types.Value, rv reflect.Value) {
	if n, ok := v.(types.Number); ok {
		i := int64(n)
		if rv.OverflowInt(i) {
			panic(overflowError(n, rv.Type()))
		}
		rv.SetInt(i)
	} else {
		panic(&UnmarshalTypeMismatchError{v, rv.Type(), ""})
	}
}

func uintDecoder(v types.Value, rv reflect.Value) {
	if n, ok := v.(types.Number); ok {
		u := uint64(n)
		if rv.OverflowUint(u) {
			panic(overflowError(n, rv.Type()))
		}
		rv.SetUint(u)
	} else {
		panic(&UnmarshalTypeMismatchError{v, rv.Type(), ""})
	}
}

var structDecoderCache struct {
	sync.RWMutex
	m map[reflect.Type]decoderFunc
}

type decField struct {
	name    string
	decoder decoderFunc
	index   int
}

func structDecoder(t reflect.Type) decoderFunc {
	if t.Implements(nomsValueInterface) {
		return nomsValueDecoder
	}

	structDecoderCache.RLock()
	d := structDecoderCache.m[t]
	structDecoderCache.RUnlock()
	if d != nil {
		return d
	}

	name := t.Name()
	fields := make([]decField, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		validateField(f, t)

		tags := f.Tag.Get("noms")
		if tags == "-" {
			continue
		}

		fields = append(fields, decField{
			name:    getFieldName(tags, f),
			decoder: typeDecoder(f.Type),
			index:   i,
		})
	}

	d = func(v types.Value, rv reflect.Value) {
		s := v.(types.Struct)
		// If the name is empty then the Go struct has to be anonymous.
		if s.Type().Desc.(types.StructDesc).Name != name {
			panic(&UnmarshalTypeMismatchError{v, rv.Type(), ", names do not match"})
		}

		for _, f := range fields {
			sf := rv.Field(f.index)
			fv, ok := s.MaybeGet(f.name)
			if !ok {
				panic(&UnmarshalTypeMismatchError{v, rv.Type(), ", missing field \"" + f.name + "\""})
			}
			f.decoder(fv, sf)
		}
	}

	structDecoderCache.Lock()
	if structDecoderCache.m == nil {
		structDecoderCache.m = map[reflect.Type]decoderFunc{}
	}
	structDecoderCache.m[t] = d
	structDecoderCache.Unlock()
	return d
}

func nomsValueDecoder(v types.Value, rv reflect.Value) {
	if !reflect.TypeOf(v).AssignableTo(rv.Type()) {
		panic(&UnmarshalTypeMismatchError{v, rv.Type(), ""})
	}
	rv.Set(reflect.ValueOf(v))
}
