// Modified from golang's encoding/json/encode.go at 80e6d638bf309181eadcb3fecbe99d2d8518e364.

// Package marshal implements encoding and decoding of Noms values into native Go types.
// The mapping between Noms objects and Go values is described
// in the documentation for the Marshal and Unmarshal functions.
package marshal

import (
	"bytes"
	"io"
	"math"
	"reflect"
	"strconv"
	"sync"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// Marshal returns the Noms types.Value of v.
//
// Marshal uses the following type-dependent default encodings:
//
// Boolean values encode as Noms booleans.
//
// Floating point and integer values encode as the equivalent Noms primitive;
// int and uint values with no bit-width are encoded as 32 bit regardless of
// platform defaults, unless they overflow. In that case, they're ignored.
//
// String values encode as Noms strings.
//
// Array and slice values encode as Noms Lists, except that
// []byte encodes as a Noms Blob. A nil slice encodes as an empty List.
//
// Struct values encode as Noms Maps. Each exported struct field
// becomes a member of the object unless
//   - the field's tag is "-", or
//   - the field is empty and its tag specifies the "omitempty" option.
// The empty values are the standard zero values.
// The Map's default keys are the struct field name as a string,
// but can be specified in the struct field's tag value. The "noms" key in
// the struct field's tag value is the key name, followed by an optional comma
// and options. Examples:
//
//   // Field is ignored by this package.
//   Field int `noms:"-"`
//
//   // Field appears in Noms as key "myName".
//   Field int `noms:"myName"`
//
//   // Field appears in Noms as key "myName" and
//   // the field is omitted from the object if its value is empty,
//   // as defined above.
//   Field int `noms:"myName,omitempty"`
//
//   // Field appears in Noms as key "Field" (the default), but
//   // the field is skipped if empty.
//   // Note the leading comma.
//   Field int `noms:",omitempty"`
//
// The key name will be used if it's a non-empty string consisting of
// only Unicode letters, digits, dollar signs, percent signs, hyphens,
// underscores and slashes.
//
// Anonymous struct fields (i.e. embedded structs) are marshaled as if their inner exported fields
// were fields in the outer struct, mostly subject to the usual Go visibility rules.
// These are amended slightly, as follows:
// 1) An anonymous struct field with a name given in its 'noms' tag is treated as
//    having that name, rather than being anonymous.
// 2) An anonymous struct field of interface type is treated the same as having
//    that type as its name, rather than being anonymous.
//
// The Go visibility rules for struct fields are amended for us when
// deciding which field to marshal or unmarshal. If there are
// multiple fields at the same level, and that level is the least
// nested (and would therefore be the nesting level selected by the
// usual Go rules), the following extra rules apply:
//
// 1) Of those fields, if any are Noms-tagged, only tagged fields are considered,
//    even if there are multiple untagged fields that would otherwise conflict.
// 2) If there is exactly one field (tagged or not according to the first rule), that is selected.
// 3) Otherwise there are multiple fields, and all are ignored; no error occurs.
//
// Map values encode as Noms Maps.
//
// Pointer values encode as the value pointed to.
// A nil pointer is an error.
//
// Interface values encode as the value contained in the interface.
// A nil interface is an error.
//
// Channel, complex, and function values cannot be encoded in Noms.
// Attempting to encode such a value causes Marshal to return
// an UnsupportedTypeError.
//
// Marshal does not currently handle cyclic data structures, though Noms could
// handle them.  Passing cyclic structures to Marshal will result in
// an infinite recursion.
//
func Marshal(v interface{}) types.Value {
	return reflectValue(reflect.ValueOf(v))
}

func unsupportedTypeMsg(t reflect.Type) string {
	return "noms: unsupported type: " + t.String()
}

func unsupportedValueMsg(s string) string {
	return "noms: unsupported value: " + s
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

func reflectValue(v reflect.Value) types.Value {
	return valueEncoder(v)(v)
}

type encoderFunc func(v reflect.Value) types.Value

var encoderCache struct {
	sync.RWMutex
	m map[reflect.Type]encoderFunc
}

func valueEncoder(v reflect.Value) encoderFunc {
	if !v.IsValid() {
		return invalidValueEncoder
	}
	return typeEncoder(v.Type())
}

func typeEncoder(t reflect.Type) encoderFunc {
	encoderCache.RLock()
	f := encoderCache.m[t]
	encoderCache.RUnlock()
	if f != nil {
		return f
	}

	// To deal with recursive types, populate the map with an
	// indirect func before we build it. This type waits on the
	// real func (f) to be ready and then calls it.  This indirect
	// func is only used for recursive types.
	encoderCache.Lock()
	if encoderCache.m == nil {
		encoderCache.m = make(map[reflect.Type]encoderFunc)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	encoderCache.m[t] = func(v reflect.Value) types.Value {
		wg.Wait()
		return f(v)
	}
	encoderCache.Unlock()

	// Compute fields without lock.
	// Might duplicate effort but won't hold other computations back.
	f = newTypeEncoder(t)
	wg.Done()
	encoderCache.Lock()
	encoderCache.m[t] = f
	encoderCache.Unlock()
	return f
}

var (
	bytesBufferType = reflect.TypeOf(&bytes.Buffer{})
	readerType      = reflect.TypeOf((*io.Reader)(nil)).Elem()
)

// newTypeEncoder constructs an encoderFunc for a type.
func newTypeEncoder(t reflect.Type) encoderFunc {
	if t.Implements(readerType) {
		return readerEncoder
	}

	switch t.Kind() {
	case reflect.Bool:
		return boolEncoder
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intEncoder
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return uintEncoder
	case reflect.Float32:
		return float32Encoder
	case reflect.Float64:
		return float64Encoder
	case reflect.String:
		return stringEncoder
	case reflect.Interface:
		return interfaceEncoder
	case reflect.Struct:
		return newStructEncoder(t)
	case reflect.Map:
		return newMapEncoder(t)
	case reflect.Slice:
		return newSliceEncoder(t)
	case reflect.Array:
		return newArrayEncoder(t)
	case reflect.Ptr:
		return newPtrEncoder(t)
	default:
		return unsupportedTypeEncoder
	}
}

func invalidValueEncoder(v reflect.Value) types.Value {
	return types.NewRef(ref.Ref{}) // Eh?
}

func boolEncoder(v reflect.Value) types.Value {
	return types.Bool(v.Bool())
}

func intEncoder(v reflect.Value) types.Value {
	switch v.Kind() {
	case reflect.Int8:
		return types.Int8(v.Int())
	case reflect.Int16:
		return types.Int16(v.Int())
	case reflect.Int32:
		return types.Int32(v.Int())
	case reflect.Int:
		n := v.Int()
		d.Exp.False(reflect.ValueOf(types.Int32(0)).OverflowInt(n), " Unsized integers must be 32 bit values; %d is too large.")
		return types.Int32(n)
	case reflect.Int64:
		return types.Int64(v.Int())
	default:
		d.Exp.Fail("Not an integer")
		panic("unreachable")
	}
}

func uintEncoder(v reflect.Value) types.Value {
	n := v.Uint()
	switch v.Kind() {
	case reflect.Uint8:
		return types.UInt8(n)
	case reflect.Uint16:
		return types.UInt16(n)
	case reflect.Uint32:
		return types.UInt32(n)
	case reflect.Uint:
		n := v.Uint()
		d.Exp.False(reflect.ValueOf(types.UInt32(0)).OverflowUint(n), "Unsized integers must be 32 bit values; %d is too large.", n)
		return types.UInt32(n)
	case reflect.Uint64:
		return types.UInt64(n)
	default:
		d.Exp.Fail("Not an unsigned integer", "%d", n)
		panic("unreachable")
	}
}

type floatEncoder int // number of bits

func (bits floatEncoder) encode(v reflect.Value) types.Value {
	f := v.Float()
	d.Exp.False(math.IsInf(f, 0), "Noms can't encode infinity", strconv.FormatFloat(f, 'g', -1, int(bits)))
	d.Exp.False(math.IsNaN(f), "Noms can't encode NaN", strconv.FormatFloat(f, 'g', -1, int(bits)))
	if bits == 64 {
		return types.Float64(f)
	}
	return types.Float32(f)
}

var (
	float32Encoder = (floatEncoder(32)).encode
	float64Encoder = (floatEncoder(64)).encode
)

func stringEncoder(v reflect.Value) types.Value {
	return types.NewString(v.String())
}

func interfaceEncoder(v reflect.Value) types.Value {
	d.Exp.False(v.IsNil(), "Noms can't encode nil interface.")
	return reflectValue(v.Elem())
}

func unsupportedTypeEncoder(v reflect.Value) types.Value {
	d.Exp.Fail(unsupportedTypeMsg(v.Type()))
	panic("unreachable")
}

type structEncoder struct {
	fields    []field
	fieldEncs []encoderFunc
}

func isNilPtrOrNilInterface(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		return v.IsNil()
	default:
		return false
	}
}

func readerEncoder(v reflect.Value) types.Value {
	d.Chk.True(v.Type().Implements(readerType))
	blob, err := types.NewMemoryBlob(v.Interface().(io.Reader))
	d.Exp.NoError(err, "Failed to marshal reader into blob")
	return blob
}

// Noms has no notion of a general-purpose nil value. Thus, if struct encoding encounters a field that holds a nil pointer or interface, it skips it even if that field doesn't have the omitempty option set. Nil maps and slices are encoded as an empty Noms map, set, list or blob as appropriate.
func (se *structEncoder) encode(v reflect.Value) types.Value {
	if v.Type() == refRefType {
		r := ref.Ref{}
		reflect.ValueOf(&r).Elem().Set(v)
		return types.NewRef(r)
	}
	nom := types.NewMap()
	for i, f := range se.fields {
		fv := fieldByIndex(v, f.index)
		if !fv.IsValid() || f.omitEmpty && isEmptyValue(fv) || isNilPtrOrNilInterface(fv) {
			continue
		}
		nom = nom.Set(types.NewString(f.name), se.fieldEncs[i](fv))
	}
	return nom
}

func newStructEncoder(t reflect.Type) encoderFunc {
	fields := cachedTypeFields(t)
	se := &structEncoder{
		fields:    fields,
		fieldEncs: make([]encoderFunc, len(fields)),
	}
	for i, f := range fields {
		se.fieldEncs[i] = typeEncoder(typeByIndex(t, f.index))
	}
	return se.encode
}

type setEncoder struct {
	elemEnc encoderFunc
}

// Noms has no notion of a general-purpose nil value. Thus, if set encoding encounters a value that holds a nil pointer or interface, it skips the value. Nil maps and slices are encoded as an empty Noms map, set, list or blob as appropriate.
func (se *setEncoder) encode(v reflect.Value) types.Value {
	tmp := make([]types.Value, 0, v.Len())
	for _, k := range v.MapKeys() {
		if isNilPtrOrNilInterface(k) || !v.MapIndex(k).Bool() {
			continue
		}
		tmp = append(tmp, se.elemEnc(k))
	}
	return types.NewSet(tmp...)
}

type mapEncoder struct {
	keyEnc  encoderFunc
	elemEnc encoderFunc
}

// Noms has no notion of a general-purpose nil value. Thus, if map encoding encounters a key or value that holds a nil pointer or interface, it skips the whole key/value pair. Nil maps and slices are encoded as an empty Noms map, set, list or blob as appropriate.
func (me *mapEncoder) encode(v reflect.Value) types.Value {
	nom := types.NewMap()
	for _, k := range v.MapKeys() {
		valueAtK := v.MapIndex(k)
		if isNilPtrOrNilInterface(k) || isNilPtrOrNilInterface(valueAtK) {
			continue
		}
		nom = nom.Set(me.keyEnc(k), me.elemEnc(valueAtK))
	}
	return nom
}

func newMapEncoder(t reflect.Type) encoderFunc {
	// Noms sets are unmarshaled to map[interface{}]bool, so we marshal anything that maps to bool as a set.
	if t.Elem().Kind() == reflect.Bool {
		se := &setEncoder{typeEncoder(t.Key())}
		return se.encode
	}
	me := &mapEncoder{typeEncoder(t.Key()), typeEncoder(t.Elem())}
	return me.encode
}

func encodeByteSlice(v reflect.Value) types.Value {
	if v.IsNil() {
		nom, _ := types.NewMemoryBlob(&bytes.Buffer{})
		return nom
	}
	nom, err := types.NewMemoryBlob(bytes.NewReader(v.Bytes()))
	if err != nil {
		panic(err)
	}
	return nom
}

func newSliceEncoder(t reflect.Type) encoderFunc {
	return newArrayEncoder(t)
}

type arrayEncoder struct {
	elemEnc encoderFunc
}

// Noms has no notion of a general-purpose nil value. Thus, if array/slice encoding encounters a value that holds a nil pointer or interface, it skips the value. Nil maps and slices are encoded as an empty Noms map, set, list or blob as appropriate.
func (ae *arrayEncoder) encode(v reflect.Value) types.Value {
	tmp := make([]types.Value, 0, v.Len())
	for i := 0; i < v.Len(); i++ {
		valueAtI := v.Index(i)
		if isNilPtrOrNilInterface(valueAtI) {
			continue
		}
		tmp = append(tmp, ae.elemEnc(valueAtI))
	}
	return types.NewList(tmp...)
}

func newArrayEncoder(t reflect.Type) encoderFunc {
	enc := &arrayEncoder{typeEncoder(t.Elem())}
	return enc.encode
}

type ptrEncoder struct {
	elemEnc encoderFunc
}

func (pe *ptrEncoder) encode(v reflect.Value) types.Value {
	d.Exp.False(v.IsNil(), "Noms can't encode nil ptr.")
	return pe.elemEnc(v.Elem())
}

func newPtrEncoder(t reflect.Type) encoderFunc {
	enc := &ptrEncoder{typeEncoder(t.Elem())}
	return enc.encode
}
