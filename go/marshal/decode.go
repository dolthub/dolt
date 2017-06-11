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

// Unmarshal converts a Noms value into a Go value. It decodes v and stores the
// result in the value pointed to by out.
//
// Unmarshal uses the inverse of the encodings that Marshal uses with the
// following additional rules:
//
// To unmarshal a Noms struct into a Go struct, Unmarshal matches incoming
// object fields to the fields used by Marshal (either the struct field name or
// its tag).  Unmarshal will only set exported fields of the struct.  The name
// of the Go struct must match (ignoring case) the name of the Noms struct. All
// exported fields on the Go struct must be present in the Noms struct, unless
// the field on the Go struct is marked with the "omitempty" tag. Go struct
// fields also support the "original" tag which causes the Go field to receive
// the entire original unmarshaled Noms struct.
//
// To unmarshal a Noms list or set into a slice, Unmarshal resets the slice
// length to zero and then appends each element to the slice. If the Go slice
// was nil a new slice is created when an element is added.
//
// To unmarshal a Noms list into a Go array, Unmarshal decodes Noms list
// elements into corresponding Go array elements.
//
// To unmarshal a Noms map into a Go map, Unmarshal decodes Noms key and values
// into corresponding Go array elements. If the Go map was nil a new map is
// created if any value is set.
//
// To unmarshal a Noms set into a Go map, the field must be tagged with `noms:",set"`,
// and it must have a type of map[<value-type>]struct{}. Unmarshal decodes into
// Go map keys corresponding to the set values and assigns each key a value of struct{}{}.
//
// When unmarshalling onto interface{} the following rules are used:
//  - types.Bool -> bool
//  - types.List -> []T, where T is determined recursively using the same rules.
//  - types.Set -> depends on `noms:",set"` annotation and field type:
//    - without the annotation, same as types.List
//    - with the annotation, same as types.Map for map[T]struct{} fields and same as types.List for slice fields
//  - types.Map -> map[T]V, where T and V is determined recursively using the
//    same rules.
//  - types.Number -> float64
//  - types.String -> string
//  - *types.Type -> *types.Type
//  - types.Union -> interface
//  - Everything else an error
//
// Unmarshal returns an UnmarshalTypeMismatchError if:
//  - a Noms value is not appropriate for a given target type
//  - a Noms number overflows the target type
//  - a Noms list is decoded into a Go array of a different length
func Unmarshal(v types.Value, out interface{}) (err error) {
	return UnmarshalOpt(v, Opt{}, out)
}

// UnmarshalOpt is like Unmarshal but provides additional options.
func UnmarshalOpt(v types.Value, opt Opt, out interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch r := r.(type) {
			case *UnmarshalTypeMismatchError, *UnsupportedTypeError, *InvalidTagError, *InvalidUnmarshalError:
				err = r.(error)
			case *unmarshalNomsError:
				err = r.err
			default:
				panic(r)
			}
		}
	}()

	MustUnmarshalOpt(v, opt, out)
	return
}

// Unmarshals a Noms value into a Go value using the same rules as Unmarshal().
// Panics on failure.
func MustUnmarshal(v types.Value, out interface{}) {
	MustUnmarshalOpt(v, Opt{}, out)
}

// MustUnmarshalOpt is like MustUnmarshal but with additional options.
func MustUnmarshalOpt(v types.Value, opt Opt, out interface{}) {
	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		panic(&InvalidUnmarshalError{reflect.TypeOf(out)})
	}
	rv = rv.Elem()
	nt := nomsTags{
		set: opt.Set,
	}
	d := typeDecoder(rv.Type(), nt)
	d(v, rv)
}

// Unmarshaler is an interface types can implement to provide their own
// decoding.
//
// You probably want to implement this on a pointer to a type, otherwise
// calling UnmarshalNoms will effectively do nothing. For example, to unmarshal
// a MyType you would define:
//
//  func (t *MyType) UnmarshalNoms(v types.Value) error {}
type Unmarshaler interface {
	// UnmarshalNoms decodes v, or returns an error.
	UnmarshalNoms(v types.Value) error
}

var unmarshalerInterface = reflect.TypeOf((*Unmarshaler)(nil)).Elem()

// InvalidUnmarshalError describes an invalid argument passed to Unmarshal. (The
// argument to Unmarshal must be a non-nil pointer.)
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

// UnmarshalTypeMismatchError describes a Noms value that was not appropriate
// for a value of a specific Go type.
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
	return fmt.Sprintf("Cannot unmarshal %s into Go value of type %s%s", types.TypeOf(e.Value).Describe(), ts, e.details)
}

func overflowError(v types.Number, t reflect.Type) *UnmarshalTypeMismatchError {
	return &UnmarshalTypeMismatchError{v, t, fmt.Sprintf(" (%g does not fit in %s)", v, t)}
}

// unmarshalNomsError wraps errors from Marshaler.UnmarshalNoms. These should
// be unwrapped and never leak to the caller of Unmarshal.
type unmarshalNomsError struct {
	err error
}

func (e *unmarshalNomsError) Error() string {
	return e.err.Error()
}

type decoderFunc func(v types.Value, rv reflect.Value)

func typeDecoder(t reflect.Type, tags nomsTags) decoderFunc {
	if reflect.PtrTo(t).Implements(unmarshalerInterface) {
		return marshalerDecoder(t)
	}

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
		if shouldMapDecodeFromSet(t, tags) {
			return mapFromSetDecoder(t)
		}
		return mapDecoder(t, tags)
	case reflect.Ptr:
		// Allow implementations of types.Value (like *types.Type)
		if t.Implements(nomsValueInterface) {
			return nomsValueDecoder
		}
		fallthrough
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

// Separate Set decoder cache because the same type with and without the
// `noms:",set"` tag decode differently (Set vs Map).
var setDecoderCache = &decoderCacheT{}

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
	name      string
	decoder   decoderFunc
	index     []int
	omitEmpty bool
	original  bool
}

func structDecoderFields(t reflect.Type) []decField {
	fields := make([]decField, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		index := make([]int, 1)
		index[0] = i
		f := t.Field(i)
		tags := getTags(f)
		if tags.skip {
			continue
		}

		if f.Anonymous && f.PkgPath == "" && !tags.hasName {
			embeddedFields := structDecoderFields(f.Type)
			for _, ef := range embeddedFields {
				ef.index = append(index, ef.index...)
				fields = append(fields, ef)
			}
			continue
		}

		validateField(f, t)

		fields = append(fields, decField{
			name:      tags.name,
			decoder:   typeDecoder(f.Type, tags),
			index:     index,
			omitEmpty: tags.omitEmpty,
			original:  tags.original,
		})
	}
	return fields
}

func structDecoder(t reflect.Type) decoderFunc {
	if t.Implements(nomsValueInterface) {
		return nomsValueDecoder
	}

	d := decoderCache.get(t)
	if d != nil {
		return d
	}

	fields := structDecoderFields(t)

	d = func(v types.Value, rv reflect.Value) {
		s, ok := v.(types.Struct)
		if !ok {
			panic(&UnmarshalTypeMismatchError{v, rv.Type(), ", expected struct"})
		}

		for _, f := range fields {
			sf := rv.FieldByIndex(f.index)
			if f.original {
				if sf.Type() != reflect.TypeOf(s) {
					panic(&UnmarshalTypeMismatchError{v, rv.Type(), ", field with tag \"original\" must have type Struct"})
				}
				sf.Set(reflect.ValueOf(s))
				continue
			}
			fv, ok := s.MaybeGet(f.name)
			if ok {
				f.decoder(fv, sf)
			} else if !f.omitEmpty {
				panic(&UnmarshalTypeMismatchError{v, rv.Type(), ", missing field \"" + f.name + "\""})
			}
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

func marshalerDecoder(t reflect.Type) decoderFunc {
	return func(v types.Value, rv reflect.Value) {
		ptr := reflect.New(t)
		err := ptr.Interface().(Unmarshaler).UnmarshalNoms(v)
		if err != nil {
			panic(&unmarshalNomsError{err})
		}
		rv.Set(ptr.Elem())
	}
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
	var init sync.RWMutex
	init.Lock()
	defer init.Unlock()
	d = func(v types.Value, rv reflect.Value) {
		var slice reflect.Value
		if rv.IsNil() {
			slice = rv
		} else {
			slice = rv.Slice(0, 0)
		}
		init.RLock()
		defer init.RUnlock()
		iterListOrSlice(v, t, func(v types.Value, _ uint64) {
			elemRv := reflect.New(t.Elem()).Elem()
			decoder(v, elemRv)
			slice = reflect.Append(slice, elemRv)
		})
		rv.Set(slice)
	}

	decoderCache.set(t, d)
	decoder = typeDecoder(t.Elem(), nomsTags{})
	return d
}

func arrayDecoder(t reflect.Type) decoderFunc {
	d := decoderCache.get(t)
	if d != nil {
		return d
	}

	var decoder decoderFunc
	var init sync.RWMutex
	init.Lock()
	defer init.Unlock()
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
		init.RLock()
		defer init.RUnlock()
		iterListOrSlice(list, t, func(v types.Value, i uint64) {
			decoder(v, rv.Index(int(i)))
		})
	}

	decoderCache.set(t, d)
	decoder = typeDecoder(t.Elem(), nomsTags{})
	return d
}

func mapFromSetDecoder(t reflect.Type) decoderFunc {
	d := setDecoderCache.get(t)
	if d != nil {
		return d
	}

	var decoder decoderFunc
	var init sync.RWMutex
	init.Lock()
	defer init.Unlock()
	d = func(v types.Value, rv reflect.Value) {
		m := rv

		nomsSet, ok := v.(types.Set)
		if !ok {
			panic(&UnmarshalTypeMismatchError{v, t, `, field has "set" tag`})
		}

		init.RLock()
		defer init.RUnlock()
		nomsSet.IterAll(func(v types.Value) {
			keyRv := reflect.New(t.Key()).Elem()
			decoder(v, keyRv)
			if m.IsNil() {
				m = reflect.MakeMap(t)
			}
			m.SetMapIndex(keyRv, reflect.New(t.Elem()).Elem())
		})
		rv.Set(m)
	}

	setDecoderCache.set(t, d)
	decoder = typeDecoder(t.Key(), nomsTags{})
	return d
}

func mapDecoder(t reflect.Type, tags nomsTags) decoderFunc {
	d := decoderCache.get(t)
	if d != nil {
		return d
	}

	var keyDecoder decoderFunc
	var valueDecoder decoderFunc
	var init sync.RWMutex
	init.Lock()
	defer init.Unlock()
	d = func(v types.Value, rv reflect.Value) {
		m := rv

		// Special case decoding failure if it looks like the "set" tag is missing,
		// because it's helpful.
		if _, ok := v.(types.Set); ok && !tags.set {
			panic(&UnmarshalTypeMismatchError{v, t, `, field missing "set" tag`})
		}

		nomsMap, ok := v.(types.Map)
		if !ok {
			panic(&UnmarshalTypeMismatchError{v, t, ""})
		}

		init.RLock()
		defer init.RUnlock()
		nomsMap.IterAll(func(k, v types.Value) {
			keyRv := reflect.New(t.Key()).Elem()
			keyDecoder(k, keyRv)
			valueRv := reflect.New(t.Elem()).Elem()
			valueDecoder(v, valueRv)
			if m.IsNil() {
				m = reflect.MakeMap(t)
			}
			m.SetMapIndex(keyRv, valueRv)
		})
		rv.Set(m)
	}

	decoderCache.set(t, d)
	keyDecoder = typeDecoder(t.Key(), nomsTags{})
	valueDecoder = typeDecoder(t.Elem(), nomsTags{})
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
		// TODO: Go directly from value to go type
		t := getGoTypeForNomsType(types.TypeOf(v), rv.Type(), v)
		i := reflect.New(t).Elem()
		typeDecoder(t, nomsTags{})(v, i)
		rv.Set(i)
	}
}

func getGoTypeForNomsType(nt *types.Type, rt reflect.Type, v types.Value) reflect.Type {
	switch nt.TargetKind() {
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

func shouldMapDecodeFromSet(rt reflect.Type, tags nomsTags) bool {
	// map[T]struct{} `noms:,"set"`
	return tags.set &&
		rt.Elem().Kind() == reflect.Struct &&
		rt.Elem().NumField() == 0
}
