// Modified from golang's encoding/json/encode_test.go at 80e6d638bf309181eadcb3fecbe99d2d8518e364.

package marshal

import (
	"bytes"
	"image"
	"io"
	"io/ioutil"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/types"
)

type Optionals struct {
	Sr string `noms:"sr"`
	So string `noms:"so,omitempty"`
	Sw string `noms:"-"`

	Ir int `noms:"omitempty"` // actually named omitempty, not an option
	Io int `noms:"io,omitempty"`

	Slr []string `noms:"slr,random"`
	Slo []string `noms:"slo,omitempty"`

	Mr map[string]interface{} `noms:"mr"`
	Mo map[string]interface{} `noms:",omitempty"`

	Fr float64 `noms:"fr"`
	Fo float64 `noms:"fo,omitempty"`

	Br bool `noms:"br"`
	Bo bool `noms:"bo,omitempty"`

	Ur uint `noms:"ur"`
	Uo uint `noms:"uo,omitempty"`

	Str struct{} `noms:"str"`
	Sto struct{} `noms:"sto,omitempty"`
}

var optionalsExpected = types.NewMap(
	types.NewString("sr"), types.NewString(""),
	types.NewString("omitempty"), types.Int32(0),
	types.NewString("slr"), types.NewList(),
	types.NewString("mr"), types.NewMap(),
	types.NewString("fr"), types.Float64(0),
	types.NewString("br"), types.Bool(false),
	types.NewString("ur"), types.UInt32(0),
	types.NewString("str"), types.NewMap(),
	types.NewString("sto"), types.NewMap(),
)

func TestOmitEmpty(t *testing.T) {
	assert := assert.New(t)
	var o Optionals
	o.Sw = "something"
	o.Mr = map[string]interface{}{}
	o.Mo = map[string]interface{}{}

	nom := Marshal(&o)
	if nom, ok := nom.(types.Map); !ok {
		assert.Fail("%+v should be a Map", nom)
	} else {
		assert.True(optionalsExpected.Equals(nom))
	}
}

type IntType int

type MyStruct struct {
	IntType
}

func TestAnonymousNonstruct(t *testing.T) {
	var i IntType = 11
	a := MyStruct{i}

	nom := Marshal(a)
	if nom, ok := nom.(types.Map); !ok {
		assert.Fail(t, "nom should be a Map, not %T", nom)
	} else {
		assert.EqualValues(t, i, nom.Get(types.NewString("IntType")))
	}
}

var marshaledEmbeds = types.NewMap(
	types.NewString("Level0"), types.Int32(1),
	types.NewString("Level1b"), types.Int32(2),
	types.NewString("Level1c"), types.Int32(3),
	types.NewString("Level1a"), types.Int32(5),
	types.NewString("LEVEL1B"), types.Int32(6),
	types.NewString("e"), types.NewMap(
		types.NewString("Level1a"), types.Int32(8),
		types.NewString("Level1b"), types.Int32(9),
		types.NewString("Level1c"), types.Int32(10),
		types.NewString("Level1d"), types.Int32(11),
		types.NewString("x"), types.Int32(12)),
	types.NewString("Loop1"), types.Int32(13),
	types.NewString("Loop2"), types.Int32(14),
	types.NewString("X"), types.Int32(15),
	types.NewString("Y"), types.Int32(16),
	types.NewString("Z"), types.Int32(17))

func TestMarshalEmbeds(t *testing.T) {
	top := &Top{
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
	}
	b := Marshal(top)
	assert.EqualValues(t, marshaledEmbeds, b)
}

type BugA struct {
	S string
}

type BugB struct {
	BugA
	S string
}

type BugC struct {
	S string
}

// Legal Go: We never use the repeated embedded field (S).
type BugX struct {
	A int
	BugA
	BugB
}

// Issue 5245.
func TestEmbeddedBug(t *testing.T) {
	assert := assert.New(t)
	v := BugB{
		BugA{"A"},
		"B",
	}
	nom := Marshal(v)
	nom = nom.(types.Map)

	expected := types.NewMap(types.NewString("S"), types.NewString("B"))
	assert.EqualValues(expected, nom)

	// Now check that the duplicate field, S, does not appear.
	x := BugX{
		A: 23,
	}
	nom = Marshal(x)
	nom = nom.(types.Map)
	expected = types.NewMap(types.NewString("A"), types.Int32(23))
	assert.EqualValues(expected, nom)
}

type BugD struct { // Same as BugA after tagging.
	XXX string `noms:"S"`
}

// BugD's tagged S field should dominate BugA's.
type BugY struct {
	BugA
	BugD
}

// Test that a field with a tag dominates untagged fields.
func TestTaggedFieldDominates(t *testing.T) {
	assert := assert.New(t)
	v := BugY{
		BugA{"BugA"},
		BugD{"BugD"},
	}
	nom := Marshal(v)
	nom = nom.(types.Map)

	expected := types.NewMap(types.NewString("S"), types.NewString("BugD"))
	assert.EqualValues(expected, nom)
}

// There are no tags here, so S should not appear.
type BugZ struct {
	BugA
	BugC
	BugY // Contains a tagged S field through BugD; should not dominate.
}

func TestDuplicatedFieldDisappears(t *testing.T) {
	assert := assert.New(t)
	v := BugZ{
		BugA{"BugA"},
		BugC{"BugC"},
		BugY{
			BugA{"nested BugA"},
			BugD{"nested BugD"},
		},
	}
	nom := Marshal(v)
	nom = nom.(types.Map)

	expected := types.NewMap()
	assert.EqualValues(expected, nom)
}

func TestMarshalSetP(t *testing.T) {
	assert := assert.New(t)

	setP := map[*Small]bool{
		&Small{Tag: "tag"}: true,
		nil:                true,
	}
	expected := types.NewSet(types.NewMap(types.NewString("Tag"), types.NewString("tag")))
	nom := Marshal(setP)

	// Check against canned marshalled representation.
	if nom, ok := nom.(types.Set); !ok {
		assert.Fail("Marshal should return set.", "nom is %v", nom)
		return
	} else if assert.NotNil(nom) {
		assert.True(expected.Equals(nom), "%v != %v", expected, nom)
	}
}

func TestMarshalBuffer(t *testing.T) {
	assert := assert.New(t)

	expected := []byte("abc")
	nom := Marshal(bytes.NewBufferString("abc"))
	validateBlob(assert, expected, nom)
}

func TestMarshalReader(t *testing.T) {
	assert := assert.New(t)

	expected := []byte("abc")
	var in io.Reader = bytes.NewBuffer(expected)
	nom := Marshal(in)
	validateBlob(assert, expected, nom)

	in = bytes.NewBuffer(expected)
	nom = Marshal(&in)
	validateBlob(assert, expected, nom)
}

func validateBlob(assert *assert.Assertions, expected []byte, nom types.Value) {
	if nom, ok := nom.(types.Blob); !ok || nom == nil {
		assert.Fail("Marshal should return blob.", "nom is %v", nom)
	} else {
		nomBytes, err := ioutil.ReadAll(nom.Reader())
		assert.NoError(err)
		assert.EqualValues(expected, nomBytes)
	}
}

func TestMarshal(t *testing.T) {
	assert := assert.New(t)
	nom := Marshal(allValue)

	// Check against canned marshalled representation.
	if nom, ok := nom.(types.Map); !ok {
		assert.Fail("Marshal should return map.", "nom is %v", nom)
		return
	}
	nomMap := nom.(types.Map)
	assert.NotNil(nomMap)
	assert.Equal(allNomsValue.Len(), nomMap.Len(), "%d != %d", allNomsValue.Len(), nomMap.Len())
	nomMap.Iter(func(k, v types.Value) (stop bool) {
		expected := allNomsValue.Get(k)
		assert.True(expected.Equals(v), "%s: %v != %v", k.(types.String).String(), expected, v)
		return
	})

	nom = Marshal(pallValue)
	if nom, ok := nom.(types.Map); !ok {
		assert.Fail("Marshal should return map.", "nom is %v", nom)
		return
	}
	nomMap = nom.(types.Map)
	assert.NotNil(nomMap)
	assert.Equal(pallNomsValue.Len(), nomMap.Len(), "%d != %d", pallNomsValue.Len(), nomMap.Len())
	nomMap.Iter(func(k, v types.Value) (stop bool) {
		expected := pallNomsValue.Get(k)
		assert.True(expected.Equals(v), "%s: %v != %v", k.(types.String).String(), expected, v)
		return
	})
}

// A struct with fields for all the things we can marshal and unmarshal relatively symmetrically.
type All struct {
	Bool    bool
	Int     int
	Int8    int8
	Int16   int16
	Int32   int32
	Int64   int64
	Uint    uint
	Uint8   uint8
	Uint16  uint16
	Uint32  uint32
	Uint64  uint64
	Float32 float32
	Float64 float64

	Foo  string `noms:"bar"`
	Foo2 string `noms:"bar2,dummyopt"`

	PBool    *bool
	PInt     *int
	PInt8    *int8
	PInt16   *int16
	PInt32   *int32
	PInt64   *int64
	PUint    *uint
	PUint8   *uint8
	PUint16  *uint16
	PUint32  *uint32
	PUint64  *uint64
	PFloat32 *float32
	PFloat64 *float64

	String  string
	PString *string

	Map   map[string]Small
	MapP  map[string]*Small
	PMap  *map[string]Small
	PMapP *map[string]*Small

	EmptyMap map[string]Small
	NilMap   map[string]Small

	Slice   []Small
	SliceP  []*Small
	PSlice  *[]Small
	PSliceP *[]*Small

	EmptySlice []Small
	NilSlice   []Small

	StringSlice []string
	ByteSlice   []byte

	Small   Small
	PSmall  *Small
	PPSmall **Small

	Interface  interface{}
	PInterface *interface{}

	Set map[Small]bool
	// SetP  map[*Small]bool must be tested separately. Two maps that use pointers for keys will never compare equal unless literally the same pointers are used as keys in each. So, even if the maps had as keys pointers to structs that were equal, the maps would not be equal. This breaks the test harness.
	PSet  *map[Small]bool
	PSetP *map[*Small]bool

	EmptySet map[Small]bool
	NilSet   map[Small]bool

	unexported int
}

type Small struct {
	Tag string
}

// Sets values for everything except the fields that are pointers.
var allValue = All{
	Bool:    true,
	Int:     2,
	Int8:    3,
	Int16:   4,
	Int32:   5,
	Int64:   6,
	Uint:    7,
	Uint8:   8,
	Uint16:  9,
	Uint32:  10,
	Uint64:  11,
	Float32: 14.1,
	Float64: 15.1,
	Foo:     "foo",
	Foo2:    "foo2",
	String:  "16",
	Map: map[string]Small{
		"17": {Tag: "tag17"},
		"18": {Tag: "tag18"},
	},
	MapP: map[string]*Small{
		"19": {Tag: "tag19"},
		"20": nil,
	},
	EmptyMap:    map[string]Small{},
	NilMap:      nil,
	Slice:       []Small{{Tag: "tag20"}, {Tag: "tag21"}},
	SliceP:      []*Small{{Tag: "tag22"}, nil, {Tag: "tag23"}},
	EmptySlice:  []Small{},
	NilSlice:    nil,
	StringSlice: []string{"str24", "str25", "str26"},

	ByteSlice: []byte{27, 28, 29},
	Small:     Small{Tag: "tag30"},
	PSmall:    &Small{Tag: "tag31"},
	Interface: 5.2,

	Set: map[Small]bool{
		Small{Tag: "tag32"}: false,
		Small{Tag: "tag33"}: true,
	},

	EmptySet: map[Small]bool{},
	NilSet:   nil,
}

// Sets values for ONLY the fields that are pointers.
var pallValue = All{
	PBool:      &allValue.Bool,
	PInt:       &allValue.Int,
	PInt8:      &allValue.Int8,
	PInt16:     &allValue.Int16,
	PInt32:     &allValue.Int32,
	PInt64:     &allValue.Int64,
	PUint:      &allValue.Uint,
	PUint8:     &allValue.Uint8,
	PUint16:    &allValue.Uint16,
	PUint32:    &allValue.Uint32,
	PUint64:    &allValue.Uint64,
	PFloat32:   &allValue.Float32,
	PFloat64:   &allValue.Float64,
	PString:    &allValue.String,
	PMap:       &allValue.Map,
	PMapP:      &allValue.MapP,
	PSlice:     &allValue.Slice,
	PSliceP:    &allValue.SliceP,
	PPSmall:    &allValue.PSmall,
	PInterface: &allValue.Interface,
	PSet:       &allValue.Set,
}

// Used in creating canned marshaled values below.
func makeNewBlob(b []byte) types.Blob {
	blob, err := types.NewMemoryBlob(bytes.NewBuffer(b))
	if err != nil {
		panic(err) // Sigh
	}
	return blob
}

// Canned marshaled version of allValue
var allNomsValue = types.NewMap(
	types.NewString("Bool"), types.Bool(true),
	types.NewString("Int"), types.Int32(2),
	types.NewString("Int8"), types.Int8(3),
	types.NewString("Int16"), types.Int16(4),
	types.NewString("Int32"), types.Int32(5),
	types.NewString("Int64"), types.Int64(6),
	types.NewString("Uint"), types.UInt32(7),
	types.NewString("Uint8"), types.UInt8(8),
	types.NewString("Uint16"), types.UInt16(9),
	types.NewString("Uint32"), types.UInt32(10),
	types.NewString("Uint64"), types.UInt64(11),
	types.NewString("Float32"), types.Float32(14.1),
	types.NewString("Float64"), types.Float64(15.1),
	types.NewString("bar"), types.NewString("foo"),
	types.NewString("bar2"), types.NewString("foo2"),
	types.NewString("String"), types.NewString("16"),

	types.NewString("Map"), types.NewMap(
		types.NewString("17"), types.NewMap(
			types.NewString("Tag"), types.NewString("tag17")),
		types.NewString("18"), types.NewMap(
			types.NewString("Tag"), types.NewString("tag18"))),

	types.NewString("MapP"), types.NewMap(
		types.NewString("19"), types.NewMap(
			types.NewString("Tag"), types.NewString("tag19"))),

	types.NewString("EmptyMap"), types.NewMap(),
	types.NewString("NilMap"), types.NewMap(),

	types.NewString("Slice"), types.NewList(
		types.NewMap(types.NewString("Tag"), types.NewString("tag20")),
		types.NewMap(types.NewString("Tag"), types.NewString("tag21"))),
	types.NewString("SliceP"), types.NewList(
		types.NewMap(types.NewString("Tag"), types.NewString("tag22")),
		types.NewMap(types.NewString("Tag"), types.NewString("tag23"))),

	types.NewString("EmptySlice"), types.NewList(),
	types.NewString("NilSlice"), types.NewList(),
	types.NewString("StringSlice"), types.NewList(
		types.NewString("str24"), types.NewString("str25"), types.NewString("str26")),
	types.NewString("ByteSlice"), types.NewList(
		types.UInt8(27), types.UInt8(28), types.UInt8(29)),
	types.NewString("Small"), types.NewMap(types.NewString("Tag"), types.NewString("tag30")),
	types.NewString("PSmall"), types.NewMap(types.NewString("Tag"), types.NewString("tag31")),

	types.NewString("Interface"), types.Float64(5.2),

	types.NewString("Set"), types.NewSet(types.NewMap(types.NewString("Tag"), types.NewString("tag33"))),

	types.NewString("EmptySet"), types.NewSet(),
	types.NewString("NilSet"), types.NewSet())

// Canned marshaled version of pallValue.
var pallNomsValue = types.NewMap(
	types.NewString("Bool"), types.Bool(false),
	types.NewString("Int"), types.Int32(0),
	types.NewString("Int8"), types.Int8(0),
	types.NewString("Int16"), types.Int16(0),
	types.NewString("Int32"), types.Int32(0),
	types.NewString("Int64"), types.Int64(0),
	types.NewString("Uint"), types.UInt32(0),
	types.NewString("Uint8"), types.UInt8(0),
	types.NewString("Uint16"), types.UInt16(0),
	types.NewString("Uint32"), types.UInt32(0),
	types.NewString("Uint64"), types.UInt64(0),
	types.NewString("Float32"), types.Float32(0),
	types.NewString("Float64"), types.Float64(0),
	types.NewString("bar"), types.NewString(""),
	types.NewString("bar2"), types.NewString(""),

	types.NewString("PBool"), types.Bool(true),
	types.NewString("PInt"), types.Int32(2),
	types.NewString("PInt8"), types.Int8(3),
	types.NewString("PInt16"), types.Int16(4),
	types.NewString("PInt32"), types.Int32(5),
	types.NewString("PInt64"), types.Int64(6),
	types.NewString("PUint"), types.UInt32(7),
	types.NewString("PUint8"), types.UInt8(8),
	types.NewString("PUint16"), types.UInt16(9),
	types.NewString("PUint32"), types.UInt32(10),
	types.NewString("PUint64"), types.UInt64(11),
	types.NewString("PFloat32"), types.Float32(14.1),
	types.NewString("PFloat64"), types.Float64(15.1),

	types.NewString("String"), types.NewString(""),
	types.NewString("PString"), types.NewString("16"),

	types.NewString("Map"), types.NewMap(),
	types.NewString("MapP"), types.NewMap(),

	types.NewString("PMap"), types.NewMap(
		types.NewString("17"), types.NewMap(
			types.NewString("Tag"), types.NewString("tag17")),
		types.NewString("18"), types.NewMap(
			types.NewString("Tag"), types.NewString("tag18"))),

	types.NewString("PMapP"), types.NewMap(
		types.NewString("19"), types.NewMap(
			types.NewString("Tag"), types.NewString("tag19"))),

	types.NewString("EmptyMap"), types.NewMap(),
	types.NewString("NilMap"), types.NewMap(),

	types.NewString("Slice"), types.NewList(),
	types.NewString("SliceP"), types.NewList(),

	types.NewString("PSlice"), types.NewList(
		types.NewMap(types.NewString("Tag"), types.NewString("tag20")),
		types.NewMap(types.NewString("Tag"), types.NewString("tag21"))),
	types.NewString("PSliceP"), types.NewList(
		types.NewMap(types.NewString("Tag"), types.NewString("tag22")),
		types.NewMap(types.NewString("Tag"), types.NewString("tag23"))),

	types.NewString("EmptySlice"), types.NewList(),
	types.NewString("NilSlice"), types.NewList(),
	types.NewString("StringSlice"), types.NewList(),
	types.NewString("ByteSlice"), types.NewList(),

	types.NewString("Small"), types.NewMap(types.NewString("Tag"), types.NewString("")),
	// PSmall and Interface are not marhsaled, as they're a nil ptr and nil interface, respectively.
	types.NewString("PPSmall"), types.NewMap(types.NewString("Tag"), types.NewString("tag31")),

	types.NewString("PInterface"), types.Float64(5.2),

	types.NewString("Set"), types.NewSet(),

	types.NewString("PSet"), types.NewSet(types.NewMap(types.NewString("Tag"), types.NewString("tag33"))),

	types.NewString("EmptySet"), types.NewSet(),
	types.NewString("NilSet"), types.NewSet())
