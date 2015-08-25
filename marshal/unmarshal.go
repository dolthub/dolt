// Modified from golang's encoding/json/decode.go at 80e6d638bf309181eadcb3fecbe99d2d8518e364.

package marshal

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"reflect"

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
// Any value can be "unmarshaled" into a ref.Ref, though the target will be populated only with the value's ref.
// Primitive values can be unmarshaled into Go primitives.
// Lists can be unmarshaled into slices, with space being allocated dynamically.
// Lists can be unmarshaled into arrays if there is room for all the data.
// Maps can be unmarshaled into Go maps.
// Sets can be unmarshaled into Go maps of the form map[ElementType]bool
// Blobs can be unmarshaled into anything that implements io.Writer by using io.Copy.
//   Note that your Writer will not be cleared or destroyed before data is written to it.
// Blobs can be unmarshaled into slices, with space being allocated dynamically.
// Blobs can be unmarshaled into arrays if there is room for all the data.
//
// Unline json.Unmarshal, this code treats overflows and significant field
// type mismatches as fatal. For example, a types.Int32 will be unmarshalled
// into an int64, because that's safe, but a types.Float64 won't be allowed
// to overflow a float32 in the target. Similarly, Unmarshal will error on a
// piece of data in nom that maps to a target of the wrong type in v -- e.g.
// both nom and v have a field named Foo, but it's a types.String in the
// former and an int in the latter.
func Unmarshal(nom types.Value, v interface{}) {
	rv := reflect.ValueOf(v)
	d.Exp.False(rv.Kind() != reflect.Ptr || rv.IsNil(), invalidUnmarshalMsg(reflect.TypeOf(v)))
	unmarshalValue(nom, rv)
}

var (
	refRefType = reflect.TypeOf(ref.Ref{})
	writerType = reflect.TypeOf((*io.Writer)(nil)).Elem()
)

func invalidTypeMsg(v string, t reflect.Type) string {
	return "noms: cannot unmarshal noms " + v + " into Go value of type " + t.String()
}

// The argument to Unmarshal must be a non-nil pointer
func invalidUnmarshalMsg(t reflect.Type) string {
	if t == nil {
		return "noms: Unmarshal(nil)"
	}

	if t.Kind() != reflect.Ptr {
		return "noms: Unmarshal(non-pointer " + t.String() + ")"
	}
	return "noms: Unmarshal(nil " + t.String() + ")"
}

// unmarshalValue unpacks an arbitrary types.Value into v.
func unmarshalValue(nom types.Value, v reflect.Value) {
	if !v.IsValid() {
		return
	}

	switch nom := nom.(type) {
	case types.Blob:
		unmarshalBlob(nom, v)
	case types.List:
		unmarshalList(nom, v)
	case types.Map:
		unmarshalMap(nom, v)
	case primitive:
		unmarshalPrimitive(nom, v)
	case types.Ref:
		unmarshalRef(nom, v)
	case types.Set:
		unmarshalSet(nom, v)
	case types.String:
		unmarshalString(nom, v)
	default:
		d.Exp.Fail(invalidTypeMsg(reflect.TypeOf(nom).Name(), v.Type()))
	}
}

// indirect walks down v, allocating pointers as needed,
// until it gets to a non-pointer.
func indirect(v reflect.Value) reflect.Value {
	// If v is a named type and is addressable,
	// start with its address, so that if the type has pointer methods,
	// we find them.
	if v.Kind() != reflect.Ptr && v.Type().Name() != "" && v.CanAddr() {
		v = v.Addr()
	}
	for {
		if nv, ok := loadValueFromInterfaceIfAddressable(v); ok {
			v = nv
			continue
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

// indirect walks down v, allocating pointers as needed,
// until it gets to a non-pointer.
func findImplementor(v reflect.Value, i reflect.Type) (reflect.Value, bool) {
	d.Chk.Equal(reflect.Interface, i.Kind())
	// If v is a named type and is addressable,
	// start with its address, so that if the type has pointer methods,
	// we find them.
	if v.Kind() != reflect.Ptr && v.Type().Name() != "" && v.CanAddr() {
		v = v.Addr()
	}
	for {
		if v.Type().Implements(writerType) {
			return v, true
		}
		if nv, ok := loadValueFromInterfaceIfAddressable(v); ok {
			v = nv
			continue
		}

		if v.Kind() != reflect.Ptr {
			break
		}

		d.Chk.False(v.IsNil())
		v = v.Elem()
	}
	return v, false
}

// Load value from interface, but only if the result will be usefully addressable.
func loadValueFromInterfaceIfAddressable(v reflect.Value) (reflect.Value, bool) {
	if v.Kind() == reflect.Interface && !v.IsNil() {
		e := v.Elem()
		if e.Kind() == reflect.Ptr && !e.IsNil() {
			return e, true
		}
	}
	return v, false
}

// TODO: unmarshal into *io.Reader? BUG 160
func unmarshalBlob(nom types.Blob, v reflect.Value) {
	origType := v.Type()  // For error reporting.
	finalV := indirect(v) // To populate any nil pointers.
	if v, ok := findImplementor(v, writerType); ok {
		n, err := io.Copy(v.Interface().(io.Writer), nom.Reader())
		d.Exp.NoError(err)
		d.Exp.EqualValues(nom.Len(), n, "Blob too large")
		return
	}
	v = finalV

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
		d.Exp.Fail(invalidTypeMsg(reflect.TypeOf(nom).Name(), origType))
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
	d.Exp.NoError(err)
	d.Exp.Equal(nomLen, read, "blob too large")
	return
}

func unmarshalList(nom types.List, v reflect.Value) {
	origType := v.Type()
	v = indirect(v)

	// Decoding into nil interface?  Switch to non-reflect code.
	if v.Kind() == reflect.Interface && v.NumMethod() == 0 {
		v.Set(reflect.ValueOf(listInterface(nom)))
		return
	} else if v.Kind() == reflect.Struct && v.Type() == refRefType {
		v.Set(reflect.ValueOf(nom.Ref()))
		return
	}

	// Check type of target.
	switch v.Kind() {
	default:
		d.Exp.Fail(invalidTypeMsg(reflect.TypeOf(nom).Name(), origType))
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
		d.Exp.True(i < v.Len(), "list is too large for target array of size %d", v.Len())
		// Decode into element.
		unmarshalValue(nom.Get(uint64(i)), v.Index(i))
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

func unmarshalMap(nom types.Map, v reflect.Value) {
	origType := v.Type()
	v = indirect(v)

	// Decoding into nil interface?  Switch to non-reflect code.
	if v.Kind() == reflect.Interface && v.NumMethod() == 0 {
		v.Set(reflect.ValueOf(mapInterface(nom)))
		return
	} else if v.Kind() == reflect.Struct && v.Type() == refRefType {
		v.Set(reflect.ValueOf(nom.Ref()))
		return
	}

	switch v.Kind() {
	default:
		d.Exp.Fail(invalidTypeMsg(reflect.TypeOf(nom).Name(), origType))
		return
	case reflect.Struct:
		unmarshalStruct(nom, v)
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
		unmarshalValue(key, mapKey)

		mapElem.Set(reflect.Zero(elemType))
		unmarshalValue(value, mapElem)
		v.SetMapIndex(mapKey, mapElem)
		return
	})
}

// TODO: Should be exported from types package?
type primitive interface {
	Ref() ref.Ref
	ToPrimitive() interface{}
}

func unmarshalPrimitive(nom primitive, v reflect.Value) {
	origType := v.Type()
	v = indirect(v)
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
		d.Exp.Fail(invalidTypeMsg(reflect.TypeOf(nom).Name(), origType))
	case reflect.Bool:
		v.SetBool(nomValue.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n := nomValue.Int()
		d.Exp.False(v.OverflowInt(n), invalidTypeMsg(fmt.Sprintf("number %d", n), origType))
		v.SetInt(n)
	case reflect.Float32, reflect.Float64:
		n := nomValue.Float()
		d.Exp.False(v.OverflowFloat(n), invalidTypeMsg(fmt.Sprintf("number %f", n), origType))
		v.SetFloat(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n := nomValue.Uint()
		d.Exp.False(v.OverflowUint(n), invalidTypeMsg(fmt.Sprintf("number %d", n), origType))
		v.SetUint(n)
	}
}

func unmarshalRef(nom types.Ref, v reflect.Value) {
	origType := v.Type()
	v = indirect(v)

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
		d.Exp.Fail(invalidTypeMsg(reflect.TypeOf(nom).Name(), origType))
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
		d.Exp.Fail(invalidTypeMsg(reflect.TypeOf(nom).Name(), origType))
		return
	}
}

func unmarshalSet(nom types.Set, v reflect.Value) {
	origType := v.Type()
	v = indirect(v)

	// Decoding into nil interface?  Switch to non-reflect code.
	if v.Kind() == reflect.Interface && v.NumMethod() == 0 {
		v.Set(reflect.ValueOf(setInterface(nom)))
		return
	} else if v.Kind() == reflect.Struct && v.Type() == refRefType {
		v.Set(reflect.ValueOf(nom.Ref()))
		return
	}

	// Check type of target.
	switch v.Kind() {
	default:
		d.Exp.Fail(invalidTypeMsg(reflect.TypeOf(nom).Name(), origType))
		return
	case reflect.Map:
		// map must have bool values.
		t := v.Type()
		if t.Elem().Kind() != reflect.Bool {
			d.Exp.Fail(invalidTypeMsg(reflect.TypeOf(nom).Name(), origType))
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
		unmarshalValue(elem, newElem)
		v.SetMapIndex(newElem, trueValue)
		return
	})
	return
}

func unmarshalString(nom types.String, v reflect.Value) {
	origType := v.Type()
	v = indirect(v)
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
		d.Exp.Fail(invalidTypeMsg(reflect.TypeOf(nom).Name(), origType))
	case reflect.String:
		v.SetString(nom.String())
	}
	return
}

func unmarshalStruct(nom types.Map, v reflect.Value) {
	v = indirect(v)
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
				unmarshalValue(value, subv)
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
func valueInterface(nom types.Value) interface{} {
	switch nom := nom.(type) {
	case types.Blob:
		d.Chk.Fail("Blobs should be handled by returing blob.Reader() directly.")
		panic("unreachable")
	case types.List:
		return listInterface(nom)
	case types.Map:
		return mapInterface(nom)
	case types.Set:
		return setInterface(nom)
	case types.String:
		return nom.String()
	case primitive:
		return nom.ToPrimitive()
	default:
		panic("unreachable")
	}
}

// listInterface is like unmarshalList but returns []interface{}.
func listInterface(nom types.List) (v []interface{}) {
	v = make([]interface{}, 0)
	for i := uint64(0); i < nom.Len(); i++ {
		v = append(v, valueInterface(nom.Get(i)))
	}
	return
}

// setInterface is like unmarshalSet but returns map[interface{}]bool.
func setInterface(nom types.Set) (v map[interface{}]bool) {
	v = make(map[interface{}]bool)
	nom.Iter(func(elem types.Value) (stop bool) {
		v[valueInterface(elem)] = true
		return
	})
	return
}

// mapInterface is like unmarshalMap but returns map[string]interface{}.
func mapInterface(nom types.Map) (m map[interface{}]interface{}) {
	m = make(map[interface{}]interface{})
	nom.Iter(func(key, value types.Value) (stop bool) {
		m[valueInterface(key)] = valueInterface(value)
		return
	})
	return
}
