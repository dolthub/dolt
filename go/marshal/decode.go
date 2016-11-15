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
// The name of the Go struct must match (ignoring case) the name of the Noms struct.
//
// To unmarshal a Noms list or set into a slice, Unmarshal resets the slice length to zero and then appends each element to the slice. If the Go slice was nil a new slice is created.
//
// To unmarshal a Noms list or set into a Go array, Unmarshal decodes Noms list elements into corresponding Go array elements.
//
// To unmarshal a Noms map into a Go map, Unmarshal decodes Noms key and values into corresponding Go array elements. If the Go map was nil a new map is created.
//
// When unmarshalling onto `interface{}` the following rules are used:
//  - `types.Bool` -> `bool`
//  - `types.List` -> `[]T`, where `T` is determined recursively using the same rules.
//  - `types.Set` -> same as `types.List`
//  - `types.Map` -> `map[T]V`, where `T` and `V` is determined recursively using the same rules.
//  - `types.Number` -> `float64`
//  - `types.String` -> `string`
//  - `types.Union` -> `interface`
//  - Everything else an error
//
// Unmarshal returns an UnmarshalTypeMismatchError if:
//  - a Noms value is not appropriate for a given target type
//  - a Noms number overflows the target type
//  - a Noms list is decoded into a Go array of a different length
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
	var ts string
	if e.Type == nil {
		ts = "nil"
	} else {
		ts = e.Type.String()
	}
	return fmt.Sprintf("Cannot unmarshal %s into Go value of type %s%s", e.Value.Type().Describe(), ts, e.details)
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
	case reflect.Struct:
		return structDecoder(t)
	case reflect.Interface:
		return interfaceDecoder(t)
	case reflect.Slice:
		return sliceDecoder(t)
	case reflect.Array:
		return arrayDecoder(t)
	case reflect.Map:
		return mapDecoder(t)
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

type decoderCacheT struct {
	sync.RWMutex
	m map[reflect.Type]decoderFunc
}

var decoderCache = &decoderCacheT{}

func (c *decoderCacheT) get(t reflect.Type) decoderFunc {
	c.RLock()
	defer c.RUnlock()
	return c.m[t]
}

func (c *decoderCacheT) set(t reflect.Type, d decoderFunc) {
	c.Lock()
	defer c.Unlock()
	if c.m == nil {
		c.m = map[reflect.Type]decoderFunc{}
	}
	c.m[t] = d
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

	d := decoderCache.get(t)
	if d != nil {
		return d
	}

	fields := make([]decField, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		validateField(f, t)

		tags := f.Tag.Get("noms")
		if tags == "-" {
			continue
		}

		name, _ := parseTags(tags, f)
		fields = append(fields, decField{
			name:    name,
			decoder: typeDecoder(f.Type),
			index:   i,
		})
	}

	d = func(v types.Value, rv reflect.Value) {
		s, ok := v.(types.Struct)
		if !ok {
			panic(&UnmarshalTypeMismatchError{v, rv.Type(), ", expected struct"})
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

	decoderCache.set(t, d)
	return d
}

func nomsValueDecoder(v types.Value, rv reflect.Value) {
	if !reflect.TypeOf(v).AssignableTo(rv.Type()) {
		panic(&UnmarshalTypeMismatchError{v, rv.Type(), ""})
	}
	rv.Set(reflect.ValueOf(v))
}

func iterListOrSlice(v types.Value, t reflect.Type, f func(c types.Value, i uint64)) {
	switch v := v.(type) {
	case types.List:
		v.IterAll(f)
	case types.Set:
		i := uint64(0)
		v.IterAll(func(cv types.Value) {
			f(cv, i)
			i++
		})
	default:
		panic(&UnmarshalTypeMismatchError{v, t, ""})
	}
}

func sliceDecoder(t reflect.Type) decoderFunc {
	d := decoderCache.get(t)
	if d != nil {
		return d
	}

	var decoder decoderFunc

	d = func(v types.Value, rv reflect.Value) {
		var slice reflect.Value
		if rv.IsNil() {
			slice = reflect.MakeSlice(t, 0, int(v.(types.Collection).Len()))
		} else {
			slice = rv.Slice(0, 0)
		}
		iterListOrSlice(v, t, func(v types.Value, _ uint64) {
			elemRv := reflect.New(t.Elem()).Elem()
			decoder(v, elemRv)
			slice = reflect.Append(slice, elemRv)
		})
		rv.Set(slice)
	}

	decoderCache.set(t, d)
	decoder = typeDecoder(t.Elem())
	return d
}

func arrayDecoder(t reflect.Type) decoderFunc {
	d := decoderCache.get(t)
	if d != nil {
		return d
	}

	var decoder decoderFunc

	d = func(v types.Value, rv reflect.Value) {
		size := t.Len()
		list, ok := v.(types.Collection)
		if !ok {
			panic(&UnmarshalTypeMismatchError{v, t, ""})
		}

		l := int(list.Len())
		if l != size {
			panic(&UnmarshalTypeMismatchError{v, t, ", length does not match"})
		}
		iterListOrSlice(list, t, func(v types.Value, i uint64) {
			decoder(v, rv.Index(int(i)))
		})
	}

	decoderCache.set(t, d)
	decoder = typeDecoder(t.Elem())
	return d
}

func mapDecoder(t reflect.Type) decoderFunc {
	d := decoderCache.get(t)
	if d != nil {
		return d
	}

	var keyDecoder decoderFunc
	var valueDecoder decoderFunc

	d = func(v types.Value, rv reflect.Value) {
		m := rv
		if m.IsNil() {
			m = reflect.MakeMap(t)
		}

		nomsMap, ok := v.(types.Map)
		if !ok {
			panic(&UnmarshalTypeMismatchError{v, t, ""})
		}

		nomsMap.IterAll(func(k, v types.Value) {
			keyRv := reflect.New(t.Key()).Elem()
			keyDecoder(k, keyRv)
			valueRv := reflect.New(t.Elem()).Elem()
			valueDecoder(v, valueRv)
			m.SetMapIndex(keyRv, valueRv)
		})
		rv.Set(m)
	}

	decoderCache.set(t, d)
	keyDecoder = typeDecoder(t.Key())
	valueDecoder = typeDecoder(t.Elem())
	return d
}

func interfaceDecoder(t reflect.Type) decoderFunc {
	if t.Implements(nomsValueInterface) {
		return nomsValueDecoder
	}

	if t != emptyInterface {
		panic(&UnsupportedTypeError{Type: t})
	}

	return func(v types.Value, rv reflect.Value) {
		t := getGoTypeForNomsType(v.Type(), rv.Type(), v)
		i := reflect.New(t).Elem()
		typeDecoder(t)(v, i)
		rv.Set(i)
	}
}

func getGoTypeForNomsType(nt *types.Type, rt reflect.Type, v types.Value) reflect.Type {
	switch nt.Kind() {
	case types.BoolKind:
		return reflect.TypeOf(false)
	case types.NumberKind:
		return reflect.TypeOf(float64(0))
	case types.StringKind:
		return reflect.TypeOf("")
	case types.ListKind, types.SetKind:
		et := getGoTypeForNomsType(nt.Desc.(types.CompoundDesc).ElemTypes[0], rt, v)
		return reflect.SliceOf(et)
	case types.MapKind:
		kt := getGoTypeForNomsType(nt.Desc.(types.CompoundDesc).ElemTypes[0], rt, v)
		vt := getGoTypeForNomsType(nt.Desc.(types.CompoundDesc).ElemTypes[1], rt, v)
		return reflect.MapOf(kt, vt)
	case types.UnionKind:
		// Visit union types to raise potential errors
		for _, ut := range nt.Desc.(types.CompoundDesc).ElemTypes {
			getGoTypeForNomsType(ut, rt, v)
		}
		return emptyInterface
	// case types.StructKind:
	// 	reflect.StructOf was not added until Go 1.7
	default:
		panic(&UnmarshalTypeMismatchError{Value: v, Type: rt})
	}
}
