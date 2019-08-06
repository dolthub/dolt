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
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package marshal

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/liquidata-inc/dolt/go/store/types"
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
//  - types.Float -> float64
//  - types.String -> string
//  - *types.Type -> *types.Type
//  - types.Union -> interface
//  - Everything else an error
//
// Unmarshal returns an UnmarshalTypeMismatchError if:
//  - a Noms value is not appropriate for a given target type
//  - a Noms number overflows the target type
//  - a Noms list is decoded into a Go array of a different length
func Unmarshal(ctx context.Context, nbf *types.NomsBinFormat, v types.Value, out interface{}) (err error) {
	return UnmarshalOpt(ctx, nbf, v, Opt{}, out)
}

// UnmarshalOpt is like Unmarshal but provides additional options.
func UnmarshalOpt(ctx context.Context, nbf *types.NomsBinFormat, v types.Value, opt Opt, out interface{}) (err error) {
	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return &InvalidUnmarshalError{reflect.TypeOf(out)}
	}
	rv = rv.Elem()
	nt := nomsTags{
		set: opt.Set,
	}
	d, err := typeDecoder(rv.Type(), nt)

	if err != nil {
		return err
	}

	err = d(ctx, nbf, v, rv)

	if err != nil {
		return err
	}

	return err
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
	UnmarshalNoms(ctx context.Context, nbf *types.NomsBinFormat, v types.Value) error
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
	nbf     *types.NomsBinFormat
}

func (e *UnmarshalTypeMismatchError) Error() string {
	var tStr string
	if e.Type != nil {
		tStr = "to: " + e.Type.String()
	}

	valType, err := types.TypeOf(e.Value)

	var valTStr string
	if err == nil {
		valTStr, err = valType.Describe(context.Background())

		if err == nil {
			valTStr = "from: " + valTStr
		}
	}

	return fmt.Sprintf("Cannot unmarshal %s %s details: %s", valTStr, tStr, e.details)
}

func overflowError(nbf *types.NomsBinFormat, v types.Float, t reflect.Type) *UnmarshalTypeMismatchError {
	return &UnmarshalTypeMismatchError{v, t, fmt.Sprintf("(%g does not fit in %s)", v, t), nbf}
}

// unmarshalNomsError wraps errors from Marshaler.UnmarshalNoms. These should
// be unwrapped and never leak to the caller of Unmarshal.
type unmarshalNomsError struct {
	err error
}

func (e *unmarshalNomsError) Error() string {
	return e.err.Error()
}

type decoderFunc func(ctx context.Context, nbf *types.NomsBinFormat, v types.Value, rv reflect.Value) error

func typeDecoder(t reflect.Type, tags nomsTags) (decoderFunc, error) {
	if reflect.PtrTo(t).Implements(unmarshalerInterface) {
		return marshalerDecoder(t), nil
	}

	switch t.Kind() {
	case reflect.Bool:
		return boolDecoder, nil
	case reflect.Float32, reflect.Float64:
		return floatDecoder, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intDecoder, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return uintDecoder, nil
	case reflect.String:
		return stringDecoder, nil
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
			return nomsValueDecoder, nil
		}
		fallthrough
	default:
		return nil, &UnsupportedTypeError{Type: t}
	}
}

func boolDecoder(ctx context.Context, nbf *types.NomsBinFormat, v types.Value, rv reflect.Value) error {
	if b, ok := v.(types.Bool); ok {
		rv.SetBool(bool(b))
	} else {
		return &UnmarshalTypeMismatchError{v, rv.Type(), "", nbf}
	}

	return nil
}

func stringDecoder(ctx context.Context, nbf *types.NomsBinFormat, v types.Value, rv reflect.Value) error {
	if s, ok := v.(types.String); ok {
		rv.SetString(string(s))
	} else {
		return &UnmarshalTypeMismatchError{v, rv.Type(), "", nbf}
	}

	return nil
}

func floatDecoder(ctx context.Context, nbf *types.NomsBinFormat, v types.Value, rv reflect.Value) error {
	if n, ok := v.(types.Float); ok {
		rv.SetFloat(float64(n))
	} else {
		return &UnmarshalTypeMismatchError{v, rv.Type(), "", nbf}
	}

	return nil
}

func intDecoder(ctx context.Context, nbf *types.NomsBinFormat, v types.Value, rv reflect.Value) error {
	if n, ok := v.(types.Float); ok {
		i := int64(n)
		if rv.OverflowInt(i) {
			return overflowError(nbf, n, rv.Type())
		}
		rv.SetInt(i)
	} else {
		return &UnmarshalTypeMismatchError{v, rv.Type(), "", nbf}
	}

	return nil
}

func uintDecoder(ctx context.Context, nbf *types.NomsBinFormat, v types.Value, rv reflect.Value) error {
	if n, ok := v.(types.Float); ok {
		u := uint64(n)
		if rv.OverflowUint(u) {
			return overflowError(nbf, n, rv.Type())
		}
		rv.SetUint(u)
	} else {
		return &UnmarshalTypeMismatchError{v, rv.Type(), "", nbf}
	}

	return nil
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

func structDecoderFields(t reflect.Type) ([]decField, error) {
	fields := make([]decField, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		index := make([]int, 1)
		index[0] = i
		f := t.Field(i)
		tags, err := getTags(f)

		if err != nil {
			return nil, err
		}

		if tags.skip {
			continue
		}

		if f.Anonymous && f.PkgPath == "" && !tags.hasName {
			embeddedFields, err := structDecoderFields(f.Type)

			if err != nil {
				return nil, err
			}

			for _, ef := range embeddedFields {
				ef.index = append(index, ef.index...)
				fields = append(fields, ef)
			}
			continue
		}

		err = validateField(f, t)

		if err != nil {
			return nil, err
		}

		decFunc, err := typeDecoder(f.Type, tags)

		if err != nil {
			return nil, err
		}

		fields = append(fields, decField{
			name:      tags.name,
			decoder:   decFunc,
			index:     index,
			omitEmpty: tags.omitEmpty,
			original:  tags.original,
		})
	}
	return fields, nil
}

func structDecoder(t reflect.Type) (decoderFunc, error) {
	if t.Implements(nomsValueInterface) {
		return nomsValueDecoder, nil
	}

	d := decoderCache.get(t)
	if d != nil {
		return d, nil
	}

	fields, err := structDecoderFields(t)

	if err != nil {
		return nil, err
	}

	d = func(ctx context.Context, nbf *types.NomsBinFormat, v types.Value, rv reflect.Value) error {
		s, ok := v.(types.Struct)
		if !ok {
			return &UnmarshalTypeMismatchError{v, rv.Type(), "expected struct", nbf}
		}

		for _, f := range fields {
			sf := rv.FieldByIndex(f.index)
			if f.original {
				if sf.Type() != reflect.TypeOf(s) {
					return &UnmarshalTypeMismatchError{v, rv.Type(), "field with tag \"original\" must have type Struct", nbf}
				}
				sf.Set(reflect.ValueOf(s))
				continue
			}
			fv, ok, err := s.MaybeGet(f.name)

			if err != nil {
				return err
			}

			if ok {
				err = f.decoder(ctx, nbf, fv, sf)

				if err != nil {
					return err
				}
			} else if !f.omitEmpty {
				return &UnmarshalTypeMismatchError{v, rv.Type(), "missing field \"" + f.name + "\"", nbf}
			}
		}

		return nil
	}

	decoderCache.set(t, d)
	return d, nil
}

func nomsValueDecoder(ctx context.Context, nbf *types.NomsBinFormat, v types.Value, rv reflect.Value) error {
	if !reflect.TypeOf(v).AssignableTo(rv.Type()) {
		return &UnmarshalTypeMismatchError{v, rv.Type(), "", nbf}
	}
	rv.Set(reflect.ValueOf(v))

	return nil
}

func marshalerDecoder(t reflect.Type) decoderFunc {
	return func(ctx context.Context, nbf *types.NomsBinFormat, v types.Value, rv reflect.Value) error {
		ptr := reflect.New(t)
		err := ptr.Interface().(Unmarshaler).UnmarshalNoms(ctx, nbf, v)
		if err != nil {
			return err
		}
		rv.Set(ptr.Elem())
		return nil
	}
}

func iterListOrSlice(ctx context.Context, nbf *types.NomsBinFormat, v types.Value, t reflect.Type, f func(c types.Value, i uint64) error) error {
	switch v := v.(type) {
	case types.List:
		err := v.IterAll(ctx, f)

		if err != nil {
			return err
		}

	case types.Set:
		i := uint64(0)
		err := v.IterAll(ctx, func(cv types.Value) error {
			err := f(cv, i)
			i++

			return err
		})

		if err != nil {
			return err
		}
	default:
		return &UnmarshalTypeMismatchError{v, t, "", nbf}
	}

	return nil
}

func sliceDecoder(t reflect.Type) (decoderFunc, error) {
	d := decoderCache.get(t)
	if d != nil {
		return d, nil
	}

	var decoder decoderFunc
	var init sync.RWMutex
	init.Lock()
	defer init.Unlock()
	d = func(ctx context.Context, nbf *types.NomsBinFormat, v types.Value, rv reflect.Value) error {
		var slice reflect.Value
		if rv.IsNil() {
			slice = rv
		} else {
			slice = rv.Slice(0, 0)
		}
		init.RLock()
		defer init.RUnlock()
		err := iterListOrSlice(ctx, nbf, v, t, func(v types.Value, _ uint64) error {
			elemRv := reflect.New(t.Elem()).Elem()
			err := decoder(ctx, nbf, v, elemRv)

			if err != nil {
				return err
			}

			slice = reflect.Append(slice, elemRv)
			return nil
		})

		if err != nil {
			return err
		}

		rv.Set(slice)

		return nil
	}

	decoderCache.set(t, d)

	var err error
	decoder, err = typeDecoder(t.Elem(), nomsTags{})

	if err != nil {
		return nil, err
	}

	return d, nil
}

func arrayDecoder(t reflect.Type) (decoderFunc, error) {
	d := decoderCache.get(t)
	if d != nil {
		return d, nil
	}

	var decoder decoderFunc
	var init sync.RWMutex
	init.Lock()
	defer init.Unlock()
	d = func(ctx context.Context, nbf *types.NomsBinFormat, v types.Value, rv reflect.Value) error {
		size := t.Len()
		list, ok := v.(types.Collection)
		if !ok {
			return &UnmarshalTypeMismatchError{v, t, "", nbf}
		}

		l := int(list.Len())
		if l != size {
			return &UnmarshalTypeMismatchError{v, t, "length does not match", nbf}
		}
		init.RLock()
		defer init.RUnlock()
		return iterListOrSlice(ctx, nbf, list, t, func(v types.Value, i uint64) error{
			return decoder(ctx, nbf, v, rv.Index(int(i)))
		})
	}

	decoderCache.set(t, d)

	var err error
	decoder, err = typeDecoder(t.Elem(), nomsTags{})

	if err != nil {
		return nil, err
	}

	return d, nil
}

func mapFromSetDecoder(t reflect.Type) (decoderFunc, error) {
	d := setDecoderCache.get(t)
	if d != nil {
		return d, nil
	}

	var decoder decoderFunc
	var init sync.RWMutex
	init.Lock()
	defer init.Unlock()
	d = func(ctx context.Context, nbf *types.NomsBinFormat, v types.Value, rv reflect.Value) error {
		m := rv

		nomsSet, ok := v.(types.Set)
		if !ok {
			return &UnmarshalTypeMismatchError{v, t, `field has "set" tag`, nbf}
		}

		init.RLock()
		defer init.RUnlock()
		err := nomsSet.IterAll(ctx, func(v types.Value) error {
			keyRv := reflect.New(t.Key()).Elem()
			err := decoder(ctx, nbf, v, keyRv)

			if err != nil {
				return err
			}

			if m.IsNil() {
				m = reflect.MakeMap(t)
			}

			m.SetMapIndex(keyRv, reflect.New(t.Elem()).Elem())
			return nil
		})

		if err != nil {
			return err
		}

		rv.Set(m)
		return nil
	}

	setDecoderCache.set(t, d)

	var err error
	decoder, err = typeDecoder(t.Key(), nomsTags{})

	if err != nil {
		return nil, err
	}

	return d, nil
}

func mapDecoder(t reflect.Type, tags nomsTags) (decoderFunc, error) {
	d := decoderCache.get(t)
	if d != nil {
		return d, nil
	}

	var keyDecoder decoderFunc
	var valueDecoder decoderFunc
	var init sync.RWMutex
	init.Lock()
	defer init.Unlock()
	d = func(ctx context.Context, nbf *types.NomsBinFormat, v types.Value, rv reflect.Value) error {
		m := rv

		// Special case decoding failure if it looks like the "set" tag is missing,
		// because it's helpful.
		if _, ok := v.(types.Set); ok && !tags.set {
			return &UnmarshalTypeMismatchError{v, t, `field missing "set" tag`, nbf}
		}

		nomsMap, ok := v.(types.Map)
		if !ok {
			return &UnmarshalTypeMismatchError{v, t, "", nbf}
		}

		init.RLock()
		defer init.RUnlock()
		err := nomsMap.IterAll(ctx, func(k, v types.Value) error {
			keyRv := reflect.New(t.Key()).Elem()
			err := keyDecoder(ctx, nbf, k, keyRv)

			if err != nil {
				return err
			}

			valueRv := reflect.New(t.Elem()).Elem()
			err = valueDecoder(ctx, nbf, v, valueRv)

			if err != nil {
				return err
			}

			if m.IsNil() {
				m = reflect.MakeMap(t)
			}

			m.SetMapIndex(keyRv, valueRv)
			return nil
		})

		if err != nil {
			return err
		}

		rv.Set(m)
		return nil
	}

	decoderCache.set(t, d)

	var err error
	keyDecoder, err = typeDecoder(t.Key(), nomsTags{})

	if err != nil {
		return nil, err
	}

	valueDecoder, err = typeDecoder(t.Elem(), nomsTags{})

	if err != nil {
		return nil, err
	}

	return d, nil
}

func interfaceDecoder(t reflect.Type) (decoderFunc, error) {
	if t.Implements(nomsValueInterface) {
		return nomsValueDecoder, nil
	}

	if t != emptyInterface {
		return nil, &UnsupportedTypeError{Type: t}
	}

	return func(ctx context.Context, nbf *types.NomsBinFormat, v types.Value, rv reflect.Value) error {
		// TODO: Go directly from value to go type
		vt, err := types.TypeOf(v)

		if err != nil {
			return err
		}

		t, err := getGoTypeForNomsType(vt, rv.Type(), v)

		if err != nil {
			return err
		}

		i := reflect.New(t).Elem()
		tdec, err := typeDecoder(t, nomsTags{})

		if err != nil {
			return err
		}

		err = tdec(ctx, nbf, v, i)

		if err != nil {
			return err
		}

		rv.Set(i)
		return nil
	}, nil
}

func getGoTypeForNomsType(nt *types.Type, rt reflect.Type, v types.Value) (reflect.Type, error) {
	switch nt.TargetKind() {
	case types.BoolKind:
		return reflect.TypeOf(false), nil
	case types.FloatKind:
		return reflect.TypeOf(float64(0)), nil
	case types.StringKind:
		return reflect.TypeOf(""), nil
	case types.ListKind, types.SetKind:
		et, err := getGoTypeForNomsType(nt.Desc.(types.CompoundDesc).ElemTypes[0], rt, v)

		if err != nil {
			return nil, err
		}

		return reflect.SliceOf(et), nil
	case types.MapKind:
		kt, err := getGoTypeForNomsType(nt.Desc.(types.CompoundDesc).ElemTypes[0], rt, v)

		if err != nil {
			return nil, err
		}

		vt, err := getGoTypeForNomsType(nt.Desc.(types.CompoundDesc).ElemTypes[1], rt, v)

		if err != nil {
			return nil, err
		}

		return reflect.MapOf(kt, vt), nil
	case types.UnionKind:
		// Visit union types to raise potential errors
		for _, ut := range nt.Desc.(types.CompoundDesc).ElemTypes {
			getGoTypeForNomsType(ut, rt, v)
		}
		return emptyInterface, nil
	// case types.StructKind:
	// 	reflect.StructOf was not added until Go 1.7
	default:
		return nil, &UnmarshalTypeMismatchError{Value: v, Type: rt}
	}
}

func shouldMapDecodeFromSet(rt reflect.Type, tags nomsTags) bool {
	// map[T]struct{} `noms:,"set"`
	return tags.set &&
		rt.Elem().Kind() == reflect.Struct &&
		rt.Elem().NumField() == 0
}
