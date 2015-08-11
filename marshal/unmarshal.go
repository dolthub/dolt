// Modified from golang's encoding/json/decode.go

package marshal

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"reflect"
	"runtime"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// Unmarshal unmarshals nom into v
//
// Unmarshal traverses the value trees of nom and v in tandem,
// unmarshaling the data from nom into v as it goes. New space is
// allocated for reference types encountered in v as needed. The semantics
// are essentially like those provided by encoding/json; Unmarshal will do
// its best to map data from nom onto the value in v, skipping fields in
// nom that don't have an analog in v. The primary difference is that
// there's no analog to the json.Unmarshaler interface.
//
// Like json.Unmarshal, this code also treats overflows and field type
// mismatches as non-fatal; the fields will just be skipped and a the
// first such occurence will be reported in the return value. For example,
// a types.Int32 will be unmarshalled into an int64, because that's safe,
// but a types.Float64 won't be allowed to overflow a float32 in the
// target. Similarly, Unmarshal will skip a piece of data in nom that maps
// to a target of the wrong type in v -- e.g. both nom and v have a field
// named Foo, but it's a types.String in the former and an int in the
// latter.
func Unmarshal(nom types.Value, v interface{}) error {
	u := unmarshalState{}
	return u.unmarshal(nom, v)
}

var (
	refRefType = reflect.TypeOf(ref.Ref{})
)

// An UnmarshalTypeError describes a Noms value that was
// not appropriate for a value of a specific Go type.
type UnmarshalTypeError struct {
	Value string       // type name of Noms value
	Type  reflect.Type // type of Go value it could not be assigned to
}

func (e *UnmarshalTypeError) Error() string {
	return "noms: cannot unmarshal noms " + e.Value + " into Go value of type " + e.Type.String()
}

// An InvalidUnmarshalError describes an invalid argument passed to Unmarshal.
// (The argument to Unmarshal must be a non-nil pointer.)
type InvalidUnmarshalError struct {
	Type reflect.Type
}

func (e *InvalidUnmarshalError) Error() string {
	if e.Type == nil {
		return "noms: Unmarshal(nil)"
	}

	if e.Type.Kind() != reflect.Ptr {
		return "noms: Unmarshal(non-pointer " + e.Type.String() + ")"
	}
	return "noms: Unmarshal(nil " + e.Type.String() + ")"
}

func (u *unmarshalState) unmarshal(nom types.Value, v interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			} else if s, ok := r.(string); ok {
				r = fmt.Errorf(s)
			}
			err = r.(error)
		}
	}()

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return &InvalidUnmarshalError{reflect.TypeOf(v)}
	}

	u.unmarshalValue(nom, rv)
	return u.savedError
}

// unmarshalState represents the state while decoding a Noms value.
type unmarshalState struct {
	savedError error
}

// error aborts the decoding by panicking with err.
func (u *unmarshalState) error(err error) {
	panic(err)
}

// saveError saves the first err it is called with,
// for reporting at the end of the unmarshal.
func (u *unmarshalState) saveError(err error) {
	if u.savedError == nil {
		u.savedError = err
	}
}

// unmarshalValue unpacks an arbitrary types.Value into v.
func (u *unmarshalState) unmarshalValue(nom types.Value, v reflect.Value) {
	if !v.IsValid() {
		return
	}

	switch nom := nom.(type) {
	case types.Blob:
		u.unmarshalBlob(nom, v)
	case types.List:
		u.unmarshalList(nom, v)
	case types.Map:
		u.unmarshalMap(nom, v)
	case primitive:
		u.unmarshalPrimitive(nom, v)
	case types.Ref:
		u.unmarshalRef(nom, v)
	case types.Set:
		u.unmarshalSet(nom, v)
	case types.String:
		u.unmarshalString(nom, v)
	default:
		u.error(&UnmarshalTypeError{reflect.TypeOf(nom).Name(), v.Type()})
	}
}

// indirect walks down v, allocating pointers as needed,
// until it gets to a non-pointer.
func (u *unmarshalState) indirect(v reflect.Value) reflect.Value {
	// If v is a named type and is addressable,
	// start with its address, so that if the type has pointer methods,
	// we find them.
	if v.Kind() != reflect.Ptr && v.Type().Name() != "" && v.CanAddr() {
		v = v.Addr()
	}
	for {
		// Load value from interface, but only if the result will be
		// usefully addressable.
		if v.Kind() == reflect.Interface && !v.IsNil() {
			e := v.Elem()
			if e.Kind() == reflect.Ptr && !e.IsNil() {
				v = e
				continue
			}
		}

		if v.Kind() != reflect.Ptr {
			break
		}

		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	return v
}

func (u *unmarshalState) unmarshalBlob(nom types.Blob, v reflect.Value) {
	origType := v.Type()
	v = u.indirect(v)

	// Decoding into nil interface? Stuff a reader in there.
	if v.Kind() == reflect.Interface && v.NumMethod() == 0 {
		v.Set(reflect.ValueOf(nom.Reader()))
		return
	} else if v.Kind() == reflect.Struct && v.Type() == refRefType {
		v.Set(reflect.ValueOf(nom.Ref()))
		return
	}

	// The reflection stuff all uses int, so I'll need this.
	nomLen := truncateUint64(nom.Len())

	// Check type of target.
	switch v.Kind() {
	default:
		u.saveError(&UnmarshalTypeError{reflect.TypeOf(nom).Name(), origType})
		return
	case reflect.Array:
		break
	case reflect.Slice:
		// If nom is too big, just make v as big as possible.
		if nomLen == 0 || nomLen > v.Cap() {
			v.Set(reflect.MakeSlice(v.Type(), nomLen, nomLen))
		} else {
			v.SetLen(nomLen)
		}
	}

	read, err := io.ReadFull(nom.Reader(), v.Bytes())
	if err != nil {
		u.saveError(err)
	}
	if read < nomLen {
		u.saveError(fmt.Errorf("blob too large"))
	}
	return
}

func (u *unmarshalState) unmarshalList(nom types.List, v reflect.Value) {
	origType := v.Type()
	v = u.indirect(v)

	// Decoding into nil interface?  Switch to non-reflect code.
	if v.Kind() == reflect.Interface && v.NumMethod() == 0 {
		v.Set(reflect.ValueOf(u.listInterface(nom)))
		return
	} else if v.Kind() == reflect.Struct && v.Type() == refRefType {
		v.Set(reflect.ValueOf(nom.Ref()))
		return
	}

	// Check type of target.
	switch v.Kind() {
	default:
		u.saveError(&UnmarshalTypeError{reflect.TypeOf(nom).Name(), origType})
		return
	case reflect.Slice:
		// The reflection stuff all uses int, so if nom is too big, just make v as big as possible.
		nomLen := truncateUint64(nom.Len())
		if nomLen == 0 || nomLen > v.Cap() {
			v.Set(reflect.MakeSlice(v.Type(), nomLen, nomLen))
		} else {
			v.SetLen(nomLen)
		}
	case reflect.Array:
		break
	}

	i := 0
	for ; uint64(i) < nom.Len(); i++ {
		// If v is a fixed array and we've exhausted it, we just skip content from nom.
		if i < v.Len() {
			// Decode into element.
			u.unmarshalValue(nom.Get(uint64(i)), v.Index(i))
		}
	}

	if i < v.Len() {
		if v.Kind() == reflect.Array {
			// Array.  Zero the rest.
			z := reflect.Zero(v.Type().Elem())
			for ; i < v.Len(); i++ {
				v.Index(i).Set(z)
			}
		} else {
			v.SetLen(i)
		}
	}
}

func (u *unmarshalState) unmarshalMap(nom types.Map, v reflect.Value) {
	origType := v.Type()
	v = u.indirect(v)

	// Decoding into nil interface?  Switch to non-reflect code.
	if v.Kind() == reflect.Interface && v.NumMethod() == 0 {
		v.Set(reflect.ValueOf(u.mapInterface(nom)))
		return
	} else if v.Kind() == reflect.Struct && v.Type() == refRefType {
		v.Set(reflect.ValueOf(nom.Ref()))
		return
	}

	switch v.Kind() {
	default:
		u.saveError(&UnmarshalTypeError{reflect.TypeOf(nom).Name(), origType})
		return
	case reflect.Struct:
		u.unmarshalStruct(nom, v)
		return
	case reflect.Map:
		if v.IsNil() {
			v.Set(reflect.MakeMap(v.Type()))
		}
	}

	keyType := v.Type().Key()
	elemType := v.Type().Elem()
	mapKey := reflect.New(keyType).Elem()
	mapElem := reflect.New(elemType).Elem()

	nom.Iter(func(key, value types.Value) (stop bool) {
		mapKey.Set(reflect.Zero(keyType))
		u.unmarshalValue(key, mapKey)

		mapElem.Set(reflect.Zero(elemType))
		u.unmarshalValue(value, mapElem)
		v.SetMapIndex(mapKey, mapElem)
		return
	})
}

// TODO: Should be exported from types package?
type primitive interface {
	Ref() ref.Ref
	ToPrimitive() interface{}
}

func (u *unmarshalState) unmarshalPrimitive(nom primitive, v reflect.Value) {
	origType := v.Type()
	v = u.indirect(v)
	nomValue := reflect.ValueOf(nom.ToPrimitive())

	if v.Kind() == reflect.Interface && v.NumMethod() == 0 {
		// You can set the nil interface to a value of any type.
		v.Set(nomValue)
		return
	} else if v.Kind() == reflect.Struct && v.Type() == refRefType {
		v.Set(reflect.ValueOf(nom.Ref()))
		return
	}

	switch v.Kind() {
	default:
		u.saveError(&UnmarshalTypeError{reflect.TypeOf(nom).Name(), origType})
	case reflect.Bool:
		v.SetBool(nomValue.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n := nomValue.Int()
		if v.OverflowInt(n) {
			u.saveError(&UnmarshalTypeError{fmt.Sprintf("number %d", n), origType})
			break
		}
		v.SetInt(n)
	case reflect.Float32, reflect.Float64:
		n := nomValue.Float()
		if v.OverflowFloat(n) {
			u.saveError(&UnmarshalTypeError{fmt.Sprintf("number %f", n), origType})
			break
		}
		v.SetFloat(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n := nomValue.Uint()
		if v.OverflowUint(n) {
			u.saveError(&UnmarshalTypeError{fmt.Sprintf("number %d", n), origType})
			break
		}
		v.SetUint(n)
	}
}

func (u *unmarshalState) unmarshalRef(nom types.Ref, v reflect.Value) {
	origType := v.Type()
	v = u.indirect(v)

	// Decoding into nil interface? Stuff a string in there.
	if v.Kind() == reflect.Interface && v.NumMethod() == 0 {
		v.Set(reflect.ValueOf(nom.Ref().String()))
		return
	} else if v.Kind() == reflect.Struct && v.Type() == refRefType {
		v.Set(reflect.ValueOf(nom.Ref()))
		return
	}

	// Check type of target.
	switch v.Kind() {
	default:
		u.saveError(&UnmarshalTypeError{reflect.TypeOf(nom).Name(), origType})
		return
	case reflect.String:
		v.SetString(nom.Ref().String())
		return
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			// A byte-slice
			digestLen := len(nom.Ref().Digest())
			v.Set(reflect.MakeSlice(v.Type(), digestLen, digestLen))
			reflect.Copy(v, reflect.ValueOf(nom.Ref().Digest()))
			return
		}
		u.saveError(&UnmarshalTypeError{reflect.TypeOf(nom).Name(), origType})
		return
	}
}

func (u *unmarshalState) unmarshalSet(nom types.Set, v reflect.Value) {
	origType := v.Type()
	v = u.indirect(v)

	// Decoding into nil interface?  Switch to non-reflect code.
	if v.Kind() == reflect.Interface && v.NumMethod() == 0 {
		v.Set(reflect.ValueOf(u.setInterface(nom)))
		return
	} else if v.Kind() == reflect.Struct && v.Type() == refRefType {
		v.Set(reflect.ValueOf(nom.Ref()))
		return
	}

	// Check type of target.
	switch v.Kind() {
	default:
		u.saveError(&UnmarshalTypeError{reflect.TypeOf(nom).Name(), origType})
		return
	case reflect.Map:
		// map must have bool values.
		t := v.Type()
		if t.Elem().Kind() != reflect.Bool {
			u.saveError(&UnmarshalTypeError{reflect.TypeOf(nom).Name(), origType})
			return
		}
		if v.IsNil() {
			v.Set(reflect.MakeMap(t))
		}
		break
	}

	// Iterate through nom, unmarshaling into new elements of the same type as v's keys.
	newElem := reflect.New(v.Type().Key()).Elem() // New returns a pointer, hence the Elem().
	trueValue := reflect.ValueOf(true)
	nom.Iter(func(elem types.Value) (stop bool) {
		u.unmarshalValue(elem, newElem)
		v.SetMapIndex(newElem, trueValue)
		return
	})
	return
}

func (u *unmarshalState) unmarshalString(nom types.String, v reflect.Value) {
	origType := v.Type()
	v = u.indirect(v)
	if v.Kind() == reflect.Interface && v.NumMethod() == 0 {
		// You can set the nil interface to a value of any type.
		v.Set(reflect.ValueOf(nom.String()))
		return
	} else if v.Kind() == reflect.Struct && v.Type() == refRefType {
		v.Set(reflect.ValueOf(nom.Ref()))
		return
	}

	switch v.Kind() {
	default:
		u.saveError(&UnmarshalTypeError{reflect.TypeOf(nom).Name(), origType})
	case reflect.String:
		v.SetString(nom.String())
	}
	return
}

func (u *unmarshalState) unmarshalStruct(nom types.Map, v reflect.Value) {
	v = u.indirect(v)
	d.Chk.Equal(reflect.Struct, v.Kind())

	if v.Type() == refRefType {
		v.Set(reflect.ValueOf(nom.Ref()))
		return
	}

	nom.Iter(func(key, value types.Value) (stop bool) {
		if key, ok := key.(types.String); ok {
			// Look at the fields defined for v and see if any match the key.
			var f *field
			kb := []byte(key.String())
			fields := cachedTypeFields(v.Type())
			for i := range fields {
				ff := &fields[i]
				if bytes.Equal(ff.nameBytes, kb) {
					f = ff
					break
				}
				if f == nil && ff.equalFold(ff.nameBytes, kb) {
					f = ff
				}
			}

			if f != nil {
				// If a field is found, walk down any nested struct definitions and allocate space for any pointers along the way to ensure that the field actually has storage allocated.
				subv := v
				for _, i := range f.index {
					if subv.Kind() == reflect.Ptr {
						if subv.IsNil() {
							subv.Set(reflect.New(subv.Type().Elem()))
						}
						subv = subv.Elem()
					}
					subv = subv.Field(i)
				}
				// subv is left pointing to the field we want to unmarshal into.
				u.unmarshalValue(value, subv)
			}
		}
		return
	})
	return
}

func truncateUint64(u uint64) (out int) {
	// TODO: check at runtime to see if ints are 32 or 64 bits and use the right constant.
	out = math.MaxInt32
	if u < uint64(out) {
		out = int(u)
	}
	return
}

// The xxxInterface routines build up a value to be stored
// in an empty interface.  They are not strictly necessary,
// but they avoid the weight of reflection in this common case.

// valueInterface is like value but returns interface{}
func (u *unmarshalState) valueInterface(nom types.Value) interface{} {
	switch nom := nom.(type) {
	case types.List:
		return u.listInterface(nom)
	case types.Map:
		return u.mapInterface(nom)
	case types.Set:
		return u.setInterface(nom)
	case types.String:
		return nom.String()
	case primitive:
		return nom.ToPrimitive()
	default:
		u.error(fmt.Errorf("Blobs not yet supported"))
		panic("unreachable")
	}
}

// listInterface is like unmarshalList but returns []interface{}.
func (u *unmarshalState) listInterface(nom types.List) (v []interface{}) {
	v = make([]interface{}, 0)
	for i := uint64(0); i < nom.Len(); i++ {
		v = append(v, u.valueInterface(nom.Get(i)))
	}
	return
}

// setInterface is like unmarshalSet but returns map[interface{}]bool.
func (u *unmarshalState) setInterface(nom types.Set) (v map[interface{}]bool) {
	v = make(map[interface{}]bool)
	nom.Iter(func(elem types.Value) (stop bool) {
		v[u.valueInterface(elem)] = true
		return
	})
	return
}

// mapInterface is like unmarshalMap but returns map[string]interface{}.
func (u *unmarshalState) mapInterface(nom types.Map) (m map[interface{}]interface{}) {
	m = make(map[interface{}]interface{})
	nom.Iter(func(key, value types.Value) (stop bool) {
		m[u.valueInterface(key)] = u.valueInterface(value)
		return
	})
	return
}
