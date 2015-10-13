package types

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/ref"
)

func TestWritePrimitives(t *testing.T) {
	assert := assert.New(t)

	f := func(k NomsKind, v Value, ex interface{}) {
		w := newJsonArrayWriter()
		tref := MakePrimitiveTypeRef(k)
		w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
		assert.EqualValues([]interface{}{k, ex}, *w)
	}

	f(BoolKind, Bool(true), true)
	f(BoolKind, Bool(false), false)

	f(UInt8Kind, UInt8(0), uint8(0))
	f(UInt16Kind, UInt16(0), uint16(0))
	f(UInt32Kind, UInt32(0), uint32(0))
	f(UInt64Kind, UInt64(0), uint64(0))
	f(Int8Kind, Int8(0), int8(0))
	f(Int16Kind, Int16(0), int16(0))
	f(Int32Kind, Int32(0), int32(0))
	f(Int64Kind, Int64(0), int64(0))
	f(Float32Kind, Float32(0), float32(0))
	f(Float64Kind, Float64(0), float64(0))

	f(StringKind, NewString("hi"), "hi")

	blob, err := NewBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	assert.NoError(err)
	f(BlobKind, blob, "AAE=")
}

func TestWriteList(t *testing.T) {
	assert := assert.New(t)

	tref := MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(Int32Kind))
	v := NewList(Int32(0), Int32(1), Int32(2), Int32(3))

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{ListKind, Int32Kind, []interface{}{int32(0), int32(1), int32(2), int32(3)}}, *w)
}

func TestWriteListOfList(t *testing.T) {
	assert := assert.New(t)

	it := MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(Int16Kind))
	tref := MakeCompoundTypeRef("", ListKind, it)
	v := NewList(NewList(Int16(0)), NewList(Int16(1), Int16(2), Int16(3)))

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{ListKind, ListKind, Int16Kind,
		[]interface{}{[]interface{}{int16(0)}, []interface{}{int16(1), int16(2), int16(3)}}}, *w)
}

func TestWriteSet(t *testing.T) {
	assert := assert.New(t)

	tref := MakeCompoundTypeRef("", SetKind, MakePrimitiveTypeRef(UInt32Kind))
	v := NewSet(UInt32(0), UInt32(1), UInt32(2), UInt32(3))

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{SetKind, UInt32Kind, []interface{}{uint32(3), uint32(1), uint32(0), uint32(2)}}, *w)
}

func TestWriteSetOfSet(t *testing.T) {
	assert := assert.New(t)

	st := MakeCompoundTypeRef("", SetKind, MakePrimitiveTypeRef(Int32Kind))
	tref := MakeCompoundTypeRef("", SetKind, st)
	v := NewSet(NewSet(Int32(0)), NewSet(Int32(1), Int32(2), Int32(3)))

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{SetKind, SetKind, Int32Kind, []interface{}{[]interface{}{int32(0)}, []interface{}{int32(1), int32(3), int32(2)}}}, *w)
}

func TestWriteMap(t *testing.T) {
	assert := assert.New(t)

	tref := MakeCompoundTypeRef("", MapKind, MakePrimitiveTypeRef(StringKind), MakePrimitiveTypeRef(BoolKind))
	v := NewMap(NewString("a"), Bool(false), NewString("b"), Bool(true))

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{MapKind, StringKind, BoolKind, []interface{}{"a", false, "b", true}}, *w)
}

func TestWriteMapOfMap(t *testing.T) {
	assert := assert.New(t)

	kt := MakeCompoundTypeRef("", MapKind, MakePrimitiveTypeRef(StringKind), MakePrimitiveTypeRef(Int64Kind))
	vt := MakeCompoundTypeRef("", SetKind, MakePrimitiveTypeRef(BoolKind))
	tref := MakeCompoundTypeRef("", MapKind, kt, vt)
	v := NewMap(NewMap(NewString("a"), Int64(0)), NewSet(Bool(true)))

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{MapKind, MapKind, StringKind, Int64Kind, SetKind, BoolKind, []interface{}{[]interface{}{"a", int64(0)}, []interface{}{true}}}, *w)
}

func TestWriteEmptyStruct(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetOrderedTypes(NewListOfTypeRef().Append(
		MakeStructTypeRef("S", []Field{}, Choices{})))
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef("S", pkgRef)
	v := NewMap()

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S"}, *w)
}

func TestWriteStruct(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetOrderedTypes(NewListOfTypeRef().Append(
		MakeStructTypeRef("S", []Field{
			Field{"x", MakePrimitiveTypeRef(Int8Kind), false},
			Field{"b", MakePrimitiveTypeRef(BoolKind), false},
		}, Choices{})))
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef("S", pkgRef)
	v := NewMap(NewString("x"), Int8(42), NewString("b"), Bool(true))

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S", int8(42), true}, *w)
}

func TestWriteStructOptionalField(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetOrderedTypes(NewListOfTypeRef().Append(
		MakeStructTypeRef("S", []Field{
			Field{"x", MakePrimitiveTypeRef(Int8Kind), true},
			Field{"b", MakePrimitiveTypeRef(BoolKind), false},
		}, Choices{})))
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef("S", pkgRef)
	v := NewMap(NewString("x"), Int8(42), NewString("b"), Bool(true))

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S", true, int8(42), true}, *w)

	v = NewMap(NewString("b"), Bool(true))

	w = newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S", false, true}, *w)
}

func TestWriteStructWithUnion(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetOrderedTypes(NewListOfTypeRef().Append(
		MakeStructTypeRef("S", []Field{
			Field{"x", MakePrimitiveTypeRef(Int8Kind), false},
		}, Choices{
			Field{"b", MakePrimitiveTypeRef(BoolKind), false},
			Field{"s", MakePrimitiveTypeRef(StringKind), false},
		})))
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef("S", pkgRef)
	v := NewMap(NewString("x"), Int8(42), NewString("$unionIndex"), UInt32(1), NewString("$unionValue"), NewString("hi"))

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S", int8(42), uint32(1), "hi"}, *w)

	v = NewMap(NewString("x"), Int8(42), NewString("$unionIndex"), UInt32(0), NewString("$unionValue"), Bool(true))

	w = newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S", int8(42), uint32(0), true}, *w)
}

func TestWriteStructWithList(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetOrderedTypes(NewListOfTypeRef().Append(
		MakeStructTypeRef("S", []Field{
			Field{"l", MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(StringKind)), false},
		}, Choices{})))
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef("S", pkgRef)
	v := NewMap(NewString("l"), NewList(NewString("a"), NewString("b")))

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S", []interface{}{"a", "b"}}, *w)

	v = NewMap(NewString("l"), NewList())
	w = newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S", []interface{}{}}, *w)
}

func TestWriteStructWithStruct(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetOrderedTypes(NewListOfTypeRef().Append(
		MakeStructTypeRef("S2", []Field{
			Field{"x", MakePrimitiveTypeRef(Int32Kind), false},
		}, Choices{}),
		MakeStructTypeRef("S", []Field{
			Field{"s", MakeTypeRef("S2", ref.Ref{}), false},
		}, Choices{})))
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef("S", pkgRef)
	v := NewMap(NewString("s"), NewMap(NewString("x"), Int32(42)))

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S", int32(42)}, *w)
}

func TestWriteStructWithBlob(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetOrderedTypes(NewListOfTypeRef().Append(
		MakeStructTypeRef("S", []Field{
			Field{"b", MakePrimitiveTypeRef(BlobKind), false},
		}, Choices{})))
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef("S", pkgRef)
	b, _ := NewBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	v := NewMap(NewString("b"), b)

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S", "AAE="}, *w)
}

func TestWriteEnum(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetOrderedTypes(NewListOfTypeRef().Append(
		MakeEnumTypeRef("E", "a", "b", "c")))
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef("E", pkgRef)
	v := UInt32(1)

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "E", uint32(1)}, *w)
}

func TestWriteListOfEnum(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetOrderedTypes(NewListOfTypeRef().Append(
		MakeEnumTypeRef("E", "a", "b", "c")))
	pkgRef := RegisterPackage(&pkg)
	et := MakeTypeRef("E", pkgRef)
	tref := MakeCompoundTypeRef("", ListKind, et)
	v := NewList(UInt32(0), UInt32(1), UInt32(2))

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{ListKind, TypeRefKind, pkgRef.String(), "E", []interface{}{uint32(0), uint32(1), uint32(2)}}, *w)
}

func TestWriteListOfValue(t *testing.T) {
	assert := assert.New(t)

	tref := MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(ValueKind))
	blob, _ := NewBlob(bytes.NewBuffer([]byte{0x01}))
	v := NewList(
		Bool(true),
		UInt8(1),
		UInt16(1),
		UInt32(1),
		UInt64(1),
		Int8(1),
		Int16(1),
		Int32(1),
		Int64(1),
		Float32(1),
		Float64(1),
		NewString("hi"),
		blob,
	)

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})

	assert.EqualValues([]interface{}{ListKind, ValueKind, []interface{}{
		BoolKind, true,
		UInt8Kind, uint8(1),
		UInt16Kind, uint16(1),
		UInt32Kind, uint32(1),
		UInt64Kind, uint64(1),
		Int8Kind, int8(1),
		Int16Kind, int16(1),
		Int32Kind, int32(1),
		Int64Kind, int64(1),
		Float32Kind, float32(1),
		Float64Kind, float64(1),
		StringKind, "hi",
		BlobKind, "AQ==",
	}}, *w)
}

func TestWriteListOfValueWithStruct(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetOrderedTypes(NewListOfTypeRef().Append(
		MakeStructTypeRef("S", []Field{
			Field{"x", MakePrimitiveTypeRef(Int32Kind), false},
		}, Choices{})))
	pkgRef := RegisterPackage(&pkg)

	tref := MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(ValueKind))
	st := MakeTypeRef("S", pkgRef)
	v := NewList(NewMap(NewString("$type"), st, NewString("x"), Int32(42)))

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{ListKind, ValueKind, []interface{}{TypeRefKind, pkgRef.String(), "S", int32(42)}}, *w)
}

func TestWriteListOfValueWithTypeRefs(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetOrderedTypes(NewListOfTypeRef().Append(
		MakeStructTypeRef("S", []Field{
			Field{"x", MakePrimitiveTypeRef(Int32Kind), false},
		}, Choices{})))
	pkgRef := RegisterPackage(&pkg)

	tref := MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(ValueKind))
	v := NewList(
		Bool(true),
		MakePrimitiveTypeRef(Int32Kind),
		MakePrimitiveTypeRef(TypeRefKind),
		MakeTypeRef("S", pkgRef),
	)

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{ListKind, ValueKind, []interface{}{
		BoolKind, true,
		TypeRefKind, Int32Kind,
		TypeRefKind, TypeRefKind,
		TypeRefKind, TypeRefKind, pkgRef.String(), "S",
	}}, *w)
}

func TestWriteRef(t *testing.T) {
	assert := assert.New(t)

	tref := MakeCompoundTypeRef("", RefKind, MakePrimitiveTypeRef(UInt32Kind))
	r := ref.Parse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")
	v := Ref{R: r}

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{RefKind, UInt32Kind, r.String()}, *w)
}

func TestWriteTypeRefValue(t *testing.T) {
	assert := assert.New(t)

	test := func(expected []interface{}, v TypeRef) {
		w := newJsonArrayWriter()
		nv := valueAsNomsValue{Value: v, t: MakePrimitiveTypeRef(TypeRefKind)}
		w.writeTopLevelValue(nv)
		assert.EqualValues(expected, *w)
	}

	test([]interface{}{TypeRefKind, Int32Kind}, MakePrimitiveTypeRef(Int32Kind))
	test([]interface{}{TypeRefKind, ListKind, []interface{}{BoolKind}},
		MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(BoolKind)))
	test([]interface{}{TypeRefKind, MapKind, []interface{}{BoolKind, StringKind}},
		MakeCompoundTypeRef("", MapKind, MakePrimitiveTypeRef(BoolKind), MakePrimitiveTypeRef(StringKind)))
	test([]interface{}{TypeRefKind, EnumKind, "E", []interface{}{"a", "b", "c"}},
		MakeEnumTypeRef("E", "a", "b", "c"))

	test([]interface{}{TypeRefKind, StructKind, "S", []interface{}{"x", Int16Kind, false, "v", ValueKind, true}, []interface{}{}},
		MakeStructTypeRef("S", []Field{
			Field{"x", MakePrimitiveTypeRef(Int16Kind), false},
			Field{"v", MakePrimitiveTypeRef(ValueKind), true},
		}, Choices{}))

	test([]interface{}{TypeRefKind, StructKind, "S", []interface{}{}, []interface{}{"x", Int16Kind, false, "v", ValueKind, false}},
		MakeStructTypeRef("S", []Field{}, Choices{
			Field{"x", MakePrimitiveTypeRef(Int16Kind), false},
			Field{"v", MakePrimitiveTypeRef(ValueKind), false},
		}))

	pkgRef := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	test([]interface{}{TypeRefKind, TypeRefKind, pkgRef.String(), "E"},
		MakeTypeRef("E", pkgRef))

	test([]interface{}{TypeRefKind, StructKind, "S", []interface{}{"e", TypeRefKind, pkgRef.String(), "E", false, "x", Int64Kind, false}, []interface{}{}},
		MakeStructTypeRef("S", []Field{
			Field{"e", MakeTypeRef("E", pkgRef), false},
			Field{"x", MakePrimitiveTypeRef(Int64Kind), false},
		}, Choices{}))
}

func TestWriteListOfTypeRefs(t *testing.T) {
	assert := assert.New(t)

	tref := MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(TypeRefKind))
	v := NewList(MakePrimitiveTypeRef(BoolKind), MakeEnumTypeRef("E", "a", "b", "c"), MakePrimitiveTypeRef(StringKind))

	w := newJsonArrayWriter()
	w.writeTopLevelValue(valueAsNomsValue{Value: v, t: tref})
	assert.EqualValues([]interface{}{ListKind, TypeRefKind, []interface{}{BoolKind, EnumKind, "E", []interface{}{"a", "b", "c"}, StringKind}}, *w)
}

func TestWritePackage(t *testing.T) {
	pkg := PackageDef{
		Types: ListOfTypeRefDef{
			MakeStructTypeRef("EnumStruct",
				[]Field{
					Field{"hand", MakeTypeRef("Handedness", ref.Ref{}), false},
				},
				Choices{},
			),
			MakeEnumTypeRef("Handedness", "right", "left", "switch"),
		},
	}.New()

	w := newJsonArrayWriter()
	w.writeTopLevelValue(pkg)

	// struct Package {
	// 	Dependencies: Set(Ref(Package))
	// 	Types: List(TypeRef)
	// }

	exp := []interface{}{
		TypeRefKind, __typesPackageInFile_package_CachedRef.String(), "Package",
		[]interface{}{}, // Dependencies
		[]interface{}{
			StructKind, "EnumStruct", []interface{}{
				"hand", TypeRefKind, "sha1-0000000000000000000000000000000000000000", "Handedness", false,
			}, []interface{}{},
			EnumKind, "Handedness", []interface{}{"right", "left", "switch"},
		},
	}

	assert.EqualValues(t, exp, w.toArray())
}
