// Modified from golang's encoding/json/decode_test.go at 80e6d638bf309181eadcb3fecbe99d2d8518e364.

package marshal

import (
	"bytes"
	"fmt"
	"image"
	"io"
	"io/ioutil"
	"net"
	"reflect"
	"strings"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type T struct {
	X string
	Y int
	Z int `noms:"-"`
}

type U struct {
	Alphabet string `noms:"alpha"`
}

type tx struct {
	x int
}

// Test data structures for anonymous fields.

type Point struct {
	Z int
}

type Top struct {
	Level0 int
	Embed0
	*Embed0a
	*Embed0b `noms:"e,omitempty"` // treated as named
	Embed0c  `noms:"-"`           // ignored
	Loop
	Embed0p // has Point with X, Y, used
	Embed0q // has Point with Z, used
}

type Embed0 struct {
	Level1a int // overridden by Embed0a's Level1a with noms tag
	Level1b int // used because Embed0a's Level1b is renamed
	Level1c int // used because Embed0a's Level1c is ignored
	Level1d int // annihilated by Embed0a's Level1d
	Level1e int `noms:"x"` // annihilated by Embed0a.Level1f
}

type Embed0a struct {
	Level1a int `noms:"Level1a,omitempty"`
	Level1b int `noms:"LEVEL1B,omitempty"`
	Level1c int `noms:"-"`
	Level1d int // annihilated by Embed0's Level1d
	Level1f int `noms:"x"` // annihilated by Embed0's Level1e
}

type Embed0b Embed0

type Embed0c Embed0

type Embed0p struct {
	image.Point
}

type Embed0q struct {
	Point
}

type Loop struct {
	Loop1 int `noms:",omitempty"`
	Loop2 int `noms:",omitempty"`
	*Loop
}

// From reflect test:
// The X in S6 and S7 annihilate, but they also block the X in S8.S9.
type S5 struct {
	S6
	S7
	S8
}

type S6 struct {
	X int
}

type S7 S6

type S8 struct {
	S9
}

type S9 struct {
	X int
	Y int
}

// From reflect test:
// The X in S11.S6 and S12.S6 annihilate, but they also block the X in S13.S8.S9.
type S10 struct {
	S11
	S12
	S13
}

type S11 struct {
	S6
}

type S12 struct {
	S6
}

type S13 struct {
	S8
}

type unmarshalTest struct {
	in  types.Value
	ptr interface{}
	out interface{}
	err string
}

type Ambig struct {
	// Given "hello", the first match should win.
	First  int `noms:"HELLO"`
	Second int `noms:"Hello"`
}

type XYZ struct {
	X interface{}
	Y interface{}
	Z interface{}
}

var unmarshalTests = []unmarshalTest{
	// basic types
	{in: types.Bool(true), ptr: new(bool), out: true},
	{in: types.Int32(1), ptr: new(int32), out: 1},
	{in: types.Int8(1), ptr: new(int8), out: int8(1)},
	{in: types.Float64(1.2), ptr: new(float64), out: 1.2},
	{in: types.Int16(-5), ptr: new(int16), out: int16(-5)},
	{in: types.Float64(2), ptr: new(interface{}), out: float64(2.0)},
	{in: types.NewString("a\u1234"), ptr: new(string), out: "a\u1234"},
	{in: types.NewString("http://"), ptr: new(string), out: "http://"},
	{in: types.NewMap(
		types.NewString("X"), types.NewList(types.Int16(1)),
		types.NewString("Y"), types.Int32(4)),
		ptr: new(T), out: T{Y: 4}, err: invalidTypeMsg("List", reflect.TypeOf(""))},
	{in: strIntMap(si{"x", 1}), ptr: new(tx), out: tx{}},

	// Z has a "-" tag.
	{in: strIntMap(si{"Y", 1}, si{"Z", 2}), ptr: new(T), out: T{Y: 1}},

	// map tests
	{in: strStrMap("alpha", "abc", "alphabet", "xyz"), ptr: new(U), out: U{Alphabet: "abc"}},
	{in: strStrMap("alpha", "abc"), ptr: new(U), out: U{Alphabet: "abc"}},
	{in: strStrMap("alphabet", "xyz"), ptr: new(U), out: U{}},

	// array tests
	{in: list(1, 2, 3), ptr: new([3]int), out: [3]int{1, 2, 3}},
	{in: list(1, 2, 3), ptr: new([1]int), out: [1]int{1}, err: "list is too large"},
	{in: list(1, 2, 3), ptr: new([5]int), out: [5]int{1, 2, 3, 0, 0}},

	// blob tests
	{in: blob(6, 7, 8), ptr: &[]byte{}, out: []byte{6, 7, 8}},

	// ref tests
	{
		in:  types.Ref{R: ref.Parse("sha1-ffffffffffffffffffffffffffffffffffffffff")},
		ptr: new(string),
		out: "sha1-" + strings.Repeat("f", 40),
	},
	{
		in:  types.Ref{R: ref.Parse("sha1-ffffffffffffffffffffffffffffffffffffffff")},
		ptr: &[]byte{},
		out: byteSlice(0xff, len(ref.Sha1Digest{})),
	},

	// empty array to interface test
	{in: types.NewList(), ptr: new([]interface{}), out: []interface{}{}},
	{in: types.NewMap(types.NewString("T"), types.NewList()), ptr: new(map[string]interface{}), out: map[string]interface{}{"T": []interface{}{}}},

	// composite tests. allNomsValue is in marshal_test.go
	{in: allNomsValue, ptr: new(All), out: allValueUnmarshal},
	{in: allNomsValue, ptr: new(*All), out: &allValueUnmarshal},

	// Overwriting of data.
	// This is what encoding/json does.
	{in: types.NewList(types.Int32(2)), ptr: sliceAddr([]int32{1}), out: []int32{2}},
	{in: strIntMap(si{"key", 2}), ptr: mapAddr(map[string]int32{"old": 0, "key": 1}), out: map[string]int32{"key": 2}},

	// embedded structs
	{
		in:  marshaledEmbedsPlus,
		ptr: new(Top),
		out: Top{
			Level0: 1,
			Embed0: Embed0{
				Level1b: 2,
				Level1c: 3,
			},
			Embed0a: &Embed0a{
				Level1a: 5,
				Level1b: 6,
			},
			Embed0b: &Embed0b{
				Level1a: 8,
				Level1b: 9,
				Level1c: 10,
				Level1d: 11,
				Level1e: 12,
			},
			Loop: Loop{
				Loop1: 13,
				Loop2: 14,
			},
			Embed0p: Embed0p{
				Point: image.Point{X: 15, Y: 16},
			},
			Embed0q: Embed0q{
				Point: Point{Z: 17},
			},
		},
	},
	{
		in:  types.NewMap(types.NewString("hello"), types.Int32(1)),
		ptr: new(Ambig),
		out: Ambig{First: 1},
	},

	{
		in:  types.NewMap(types.NewString("X"), types.Int32(1), types.NewString("Y"), types.Int32(2)),
		ptr: new(S5),
		out: S5{S8: S8{S9: S9{Y: 2}}},
	},
	{
		in:  types.NewMap(types.NewString("X"), types.Int32(1), types.NewString("Y"), types.Int32(2)),
		ptr: new(S10),
		out: S10{S13: S13{S8: S8{S9: S9{Y: 2}}}},
	},
}

// marshaledEmbeds (from marshal_test.go) plus some unexported fields.
var marshaledEmbedsPlus = func() types.Map {
	return marshaledEmbeds.Set(types.NewString("x"), types.Int32(4))
}()

// allValue (from marshal_test.go) with some fields overridden to match how we handle nil in various cases.
var allValueUnmarshal = func() All {
	local := allValue

	local.MapP = map[string]*Small{
		"19": {Tag: "tag19"},
		// Note: We skip nil fields in maps.
	}
	// Note: we marshal nil map values, nil slices and nil slice entries entries as empty slices, maps, etc.
	local.NilMap = map[string]Small{}
	local.SliceP = []*Small{{Tag: "tag22"}, {Tag: "tag23"}}
	local.NilSlice = []Small{}

	// Note: Noms sets are marshaled from/to map[interface{}]bool, and only non-nil keys that map to true are included.
	local.Set = map[Small]bool{Small{Tag: "tag33"}: true}
	local.NilSet = map[Small]bool{}

	return local
}()

func TestUnmarshal(t *testing.T) {
	assert := assert.New(t)
	for i, tt := range unmarshalTests {
		// v = new(right-type)
		v := reflect.New(reflect.TypeOf(tt.ptr).Elem())

		err := d.Try(func() { Unmarshal(tt.in, v.Interface()) })
		if tt.err != "" {
			if assert.NotNil(err) {
				assert.Contains(err.Error(), tt.err, "#%d: %v not in %s", i, err, tt.err)
			}
			continue
		}
		assert.NoError(err, "error in test #%d", i)
		assert.EqualValues(tt.out, v.Elem().Interface())

		// Check round trip.
		nom := Marshal(v.Interface())
		vv := reflect.New(reflect.TypeOf(tt.ptr).Elem())
		Unmarshal(nom, vv.Interface())
		assert.EqualValues(v.Elem().Interface(), vv.Elem().Interface())
	}
}

var unmarshalAsRefTests = []types.Value{
	blob(0, 1, 2),
	list(3, 4, 5),
	types.NewSet(types.Int8(6), types.Int8(7), types.Int8(8)),
	strStrMap("9", "10", "11", "12"),
	types.NewString("13"),
	types.UInt64(14),
}

func TestUnmarshalAsRef(t *testing.T) {
	assert := assert.New(t)

	for _, input := range unmarshalAsRefTests {
		expected := input.Ref()
		target := ref.Ref{}
		Unmarshal(input, &target)

		assert.EqualValues(expected, target)

		// Check round trip.
		nom := Marshal(target)
		newTarget := ref.Ref{}
		Unmarshal(nom, &newTarget)
		assert.EqualValues(target, newTarget)
	}
}

func TestUnmarshalSetP(t *testing.T) {
	assert := assert.New(t)

	expected := map[*Small]bool{
		&Small{Tag: "tag"}: true,
	}
	set := types.NewSet(types.NewMap(types.NewString("Tag"), types.NewString("tag")))
	target := map[*Small]bool{}

	findValueInMapKeys := func(tk *Small, m map[*Small]bool) (found bool) {
		for k := range m {
			found = *k == *tk
		}
		return
	}

	Unmarshal(set, &target)
	if !assert.Len(target, len(expected)) {
		return
	}

	for tk := range target {
		assert.True(findValueInMapKeys(tk, expected))
	}

	// Check round trip.
	nom := Marshal(target)
	newTarget := map[*Small]bool{}
	Unmarshal(nom, &newTarget)
	for ntk := range newTarget {
		assert.True(findValueInMapKeys(ntk, target))
	}
}

func TestUnmarshalBlobIntoWriter(t *testing.T) {
	assert := assert.New(t)

	expected := []byte("abc")
	blob := blob(expected...)
	target := &bytes.Buffer{}

	Unmarshal(blob, &target)
	targetBytes := target.Bytes()
	assert.EqualValues(expected, targetBytes)

	// Check round trip.
	nom := Marshal(target)
	newTarget := &bytes.Buffer{}
	Unmarshal(nom, &newTarget)
	assert.EqualValues(targetBytes, newTarget.Bytes())
}

func TestUnmarshalBlobIntoWriterPtr(t *testing.T) {
	assert := assert.New(t)

	expected := []byte("abc")
	blob := blob(expected...)
	target := &bytes.Buffer{}
	targetP := &target

	Unmarshal(blob, &targetP)
	targetBytes := target.Bytes()
	assert.EqualValues(expected, targetBytes)
}

// Helpers for building up unmarshalTests
func sliceAddr(x []int32) *[]int32                 { return &x }
func mapAddr(x map[string]int32) *map[string]int32 { return &x }

func byteSlice(b byte, times int) (out []byte) {
	out = make([]byte, times)
	for i := range out {
		out[i] = b
	}
	return
}

func blob(b ...byte) types.Blob {
	blob, err := types.NewBlob(bytes.NewReader(b))
	if err != nil {
		panic(err)
	}
	return blob
}

func list(l ...int) types.List {
	out := make([]types.Value, len(l))
	for i, e := range l {
		out[i] = types.Int32(e)
	}
	return types.NewList(out...)
}

func strStrMap(s ...string) types.Map {
	out := make([]types.Value, len(s))
	for i, e := range s {
		out[i] = types.NewString(e)
	}
	return types.NewMap(out...)
}

func strIntMap(pairs ...si) types.Map {
	out := make([]types.Value, 0, 2*len(pairs))
	for _, e := range pairs {
		out = append(out, types.NewString(e.k))
		out = append(out, types.Int32(e.v))
	}
	return types.NewMap(out...)
}

type si struct {
	k string
	v int32
}

/*
TODO(cmasone): Figure out how to generate a random, large noms Value so we can do a version of this that makes sense for us.
func TestUnmarshalMarshal(t *testing.T) {
	initBig()
	var v interface{}
	if err := Unmarshal(jsonBig, &v); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	b:=Marshal(v)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !bytes.Equal(jsonBig, b) {
		t.Errorf("Marshal jsonBig")
		printDiff(t, b, jsonBig)
		return
	}
}
*/

func TestLargeByteSlice(t *testing.T) {
	s0 := make([]byte, 2000)
	for i := range s0 {
		s0[i] = byte(i)
	}
	b := Marshal(s0)

	var s1 []byte
	Unmarshal(b, &s1)
	if !assert.Equal(t, s0, s1, "Marshal large byte slice") {
		printDiff(t, s0, s1)
	}
}

func printDiff(t *testing.T, a, b []byte) {
	for i := 0; ; i++ {
		if i >= len(a) || i >= len(b) || a[i] != b[i] {
			j := i - 10
			if j < 0 {
				j = 0
			}
			t.Errorf("diverge at %d: «%s» vs «%s»", i, trim(a[j:]), trim(b[j:]))
			return
		}
	}
}

func trim(b []byte) []byte {
	if len(b) > 20 {
		return b[0:20]
	}
	return b
}

type Xint struct {
	X int
}

func TestUnmarshalInterface(t *testing.T) {
	var xint Xint
	var i interface{} = &xint
	Unmarshal(strIntMap(si{"X", 1}), &i)
	if xint.X != 1 {
		t.Fatalf("Did not write to xint")
	}
}

func TestUnmarshalPtrPtr(t *testing.T) {
	var xint Xint
	pxint := &xint
	Unmarshal(strIntMap(si{"X", 1}), &pxint)
	if xint.X != 1 {
		t.Fatalf("Did not write to xint")
	}
}

func intp(x int) *int {
	p := new(int)
	*p = x
	return p
}

func intpp(x *int) **int {
	pp := new(*int)
	*pp = x
	return pp
}

var interfaceSetTests = []struct {
	pre  interface{}
	nom  types.Value
	post interface{}
}{
	{"foo", types.NewString("bar"), "bar"},
	{nil, types.NewString("bar"), "bar"},
	{"foo", types.Int32(2), int32(2)},
	{"foo", types.Bool(true), true},
	{"foo", list(1, 2, 3), []interface{}{int32(1), int32(2), int32(3)}},
	{"foo", strStrMap("4", "5"), map[interface{}]interface{}{"4": "5"}},
	{"foo", types.NewSet(types.Int8(6)), map[interface{}]bool{int8(6): true}},

	{intp(1), types.Int64(7), intp(7)},
	{intpp(intp(1)), types.Int64(8), intpp(intp(8))},
}

func TestInterfaceSet(t *testing.T) {
	for _, tt := range interfaceSetTests {
		native := struct{ X interface{} }{tt.pre}
		nom := types.NewMap(types.NewString("X"), tt.nom)
		Unmarshal(nom, &native)
		assert.EqualValues(t, tt.post, native.X, "Unmarshal %v over %#v: X=%#v, want %#v", nom, tt.pre, native.X, tt.post)
	}
}

// TODO: enable blobs to be unmarshaled to *io.Reader, then add test cases here (BUG 160)
var blobInterfaceSetTests = []struct {
	pre  interface{}
	nom  types.Blob
	post []byte
}{
	{"foo", blob(1, 2, 3), []byte{1, 2, 3}},
	{bytes.NewBuffer([]byte{7}), blob(4, 5, 6), []byte{7, 4, 5, 6}},
}

func TestBlobInterfaceSet(t *testing.T) {
	for _, tt := range blobInterfaceSetTests {
		native := struct{ X interface{} }{tt.pre}
		nom := types.NewMap(types.NewString("X"), tt.nom)
		Unmarshal(nom, &native)
		bytes, err := ioutil.ReadAll(native.X.(io.Reader))
		if assert.NoError(t, err) {
			assert.Equal(t, tt.post, bytes)
		}
	}
}

func TestStringKind(t *testing.T) {
	assert := assert.New(t)
	type stringKind string

	var m1, m2 map[stringKind]int
	m1 = map[stringKind]int{
		"foo": 42,
	}

	data := Marshal(m1)
	Unmarshal(data, &m2)

	assert.EqualValues(m1, m2)
}

// Custom types with []byte as underlying type could not be marshalled
// and then unmarshalled.
// Issue 8962.
func TestByteKind(t *testing.T) {
	assert := assert.New(t)
	type byteKind []byte

	a := byteKind("hello")

	data := Marshal(a)

	var b byteKind
	Unmarshal(data, &b)

	assert.EqualValues(a, b)
}

var decodeTypeErrorTests = []struct {
	dest interface{}
	src  types.Value
}{
	{new(string), types.NewMap(types.NewString("guy"), types.NewString("friend"))}, // issue 4628.
	{new(error), types.NewMap()},                                                   // issue 4222
	{new(error), types.NewList()},
	{new(error), types.NewString("")},
	{new(error), types.UInt64(123)},
	{new(error), types.Bool(true)},
}

func TestUnmarshalTypeError(t *testing.T) {
	for _, item := range decodeTypeErrorTests {
		err := d.Try(func() { Unmarshal(item.src, item.dest) })
		assert.IsType(t, d.UsageError{}, err, "expected type error for Unmarshal(%v, type %T): got %T (%v)",
			item.src, item.dest, err, err)
	}
}

type unexportedFields struct {
	Name string
	m    map[string]interface{} `noms:"-"`
	m2   map[string]interface{} `noms:"abcd"`
}

func TestUnmarshalUnexported(t *testing.T) {
	input := types.NewMap(
		types.NewString("Name"), types.NewString("Bob"),
		types.NewString("m"), types.NewMap(types.NewString("x"), types.Int64(123)),
		types.NewString("m2"), types.NewMap(types.NewString("y"), types.Int64(456)),
		types.NewString("abcd"), types.NewMap(types.NewString("z"), types.Int64(789)))
	expected := &unexportedFields{Name: "Bob"}

	out := &unexportedFields{}
	Unmarshal(input, out)
	assert.EqualValues(t, expected, out)
}

// Test semantics of pre-filled struct fields and pre-filled map fields.
// Issue 4900.
func TestPrefilled(t *testing.T) {
	ptrToMap := func(m map[string]interface{}) *map[string]interface{} { return &m }

	// Values here change, cannot reuse table across runs.
	var prefillTests = []struct {
		in  types.Map
		ptr interface{}
		out interface{}
	}{
		{
			in:  strIntMap(si{"X", 1}, si{"Y", 2}),
			ptr: &XYZ{X: float32(3), Y: int16(4), Z: 1.5},
			out: &XYZ{X: int32(1), Y: int32(2), Z: 1.5},
		},
		{
			in:  strIntMap(si{"X", 1}, si{"Y", 2}),
			ptr: ptrToMap(map[string]interface{}{"X": float32(3), "Y": int16(4), "Z": 1.5}),
			out: ptrToMap(map[string]interface{}{"X": int32(1), "Y": int32(2), "Z": 1.5}),
		},
	}

	for _, tt := range prefillTests {
		ptrstr := fmt.Sprintf("%v", tt.ptr)
		Unmarshal(tt.in, tt.ptr) // tt.ptr edited here
		assert.EqualValues(t, tt.ptr, tt.out, "Target should have been overwritten, was originally %s", ptrstr)
	}
}

var invalidUnmarshalTests = []struct {
	v    interface{}
	want string
}{
	{nil, "noms: Unmarshal(nil)"},
	{struct{}{}, "noms: Unmarshal(non-pointer struct {})"},
	{(*int)(nil), "noms: Unmarshal(nil *int)"},
	{new(net.IP), "noms: cannot unmarshal noms String into Go value of type *net.IP"},
}

func TestInvalidUnmarshal(t *testing.T) {
	nom := types.NewString("hello")
	for _, tt := range invalidUnmarshalTests {
		if err := d.Try(func() { Unmarshal(nom, tt.v) }); assert.Error(t, err) {
			assert.Contains(t, err.Error(), tt.want)
		} else {
			assert.Fail(t, "Expecting error!")
		}
	}
}
