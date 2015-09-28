package types

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func TestRead(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := []interface{}{float64(1), "hi", true}
	r := newJsonArrayReader(a, cs)

	assert.Equal(float64(1), r.read().(float64))
	assert.False(r.atEnd())

	assert.Equal("hi", r.readString())
	assert.False(r.atEnd())

	assert.Equal(true, r.readBool())
	assert.True(r.atEnd())
}

func parseJson(s string) (v []interface{}) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.Decode(&v)
	return
}

func TestReadTypeRef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := parseJson(`[0, true]`)
	r := newJsonArrayReader(a, cs)
	k := r.readKind()
	assert.Equal(BoolKind, k)

	r = newJsonArrayReader(a, cs)
	tr := r.readTypeRef()
	assert.Equal(BoolKind, tr.Kind())
	b := r.readTopLevelValue(tr, nil).NomsValue()
	assert.EqualValues(Bool(true), b)
}

func TestReadListOfInt32(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := parseJson(fmt.Sprintf("[%d, %d, 0, 1, 2, 3]", ListKind, Int32Kind))
	r := newJsonArrayReader(a, cs)
	tr := r.readTypeRef()

	RegisterFromValFunction(tr, func(v Value) NomsValue {
		return valueAsNomsValue{v}
	})

	assert.Equal(ListKind, tr.Kind())
	assert.Equal(Int32Kind, tr.Desc.(CompoundDesc).ElemTypes[0].Kind())
	l := r.readList(tr, nil).NomsValue()
	assert.EqualValues(NewList(Int32(0), Int32(1), Int32(2), Int32(3)), l)
}

func TestReadListOfValue(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := parseJson(fmt.Sprintf(`[%d, %d, %d, 1, %d, "hi", %d, true]`, ListKind, ValueKind, Int32Kind, StringKind, BoolKind))
	r := newJsonArrayReader(a, cs)
	tr := r.readTypeRef()

	listTr := MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(ValueKind))

	RegisterFromValFunction(listTr, func(v Value) NomsValue {
		return valueAsNomsValue{v}
	})

	assert.Equal(ListKind, tr.Kind())
	assert.Equal(ValueKind, tr.Desc.(CompoundDesc).ElemTypes[0].Kind())
	l := r.readList(tr, nil).NomsValue()
	assert.EqualValues(NewList(Int32(1), NewString("hi"), Bool(true)), l)
}

func TestReadValueListOfInt8(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := parseJson(fmt.Sprintf(`[%d, %d, %d, [0, 1, 2]]`, ValueKind, ListKind, Int8Kind))
	r := newJsonArrayReader(a, cs)
	tr := r.readTypeRef()
	listTr := MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(Int8Kind))

	RegisterFromValFunction(listTr, func(v Value) NomsValue {
		return valueAsNomsValue{v}
	})

	assert.Equal(ValueKind, tr.Kind())
	l := r.readTopLevelValue(tr, nil).NomsValue()
	assert.EqualValues(NewList(Int8(0), Int8(1), Int8(2)), l)
}

func TestReadMapOfInt64ToFloat64(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := parseJson(fmt.Sprintf("[%d, %d, %d, 0, 1, 2, 3]", MapKind, Int64Kind, Float64Kind))
	r := newJsonArrayReader(a, cs)
	tr := r.readTypeRef()

	RegisterFromValFunction(tr, func(v Value) NomsValue {
		return valueAsNomsValue{v}
	})

	assert.Equal(MapKind, tr.Kind())
	assert.Equal(Int64Kind, tr.Desc.(CompoundDesc).ElemTypes[0].Kind())
	assert.Equal(Float64Kind, tr.Desc.(CompoundDesc).ElemTypes[1].Kind())
	m := r.readMap(tr, nil).NomsValue()
	assert.EqualValues(NewMap(Int64(0), Float64(1), Int64(2), Float64(3)), m)
}

func TestReadValueMapOfUInt64ToUInt32(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := parseJson(fmt.Sprintf("[%d, %d, %d, %d, [0, 1, 2, 3]]", ValueKind, MapKind, UInt64Kind, UInt32Kind))
	r := newJsonArrayReader(a, cs)
	tr := r.readTypeRef()
	mapTr := MakeCompoundTypeRef("", MapKind, MakePrimitiveTypeRef(UInt64Kind), MakePrimitiveTypeRef(UInt32Kind))

	RegisterFromValFunction(mapTr, func(v Value) NomsValue {
		return valueAsNomsValue{v}
	})

	assert.Equal(ValueKind, tr.Kind())
	m := r.readTopLevelValue(tr, nil).NomsValue()
	assert.True(NewMap(UInt64(0), UInt32(1), UInt64(2), UInt32(3)).Equals(m))
}

func TestReadSetOfUInt8(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := parseJson(fmt.Sprintf("[%d, %d, 0, 1, 2, 3]", SetKind, UInt8Kind))
	r := newJsonArrayReader(a, cs)
	tr := r.readTypeRef()

	RegisterFromValFunction(tr, func(v Value) NomsValue {
		return valueAsNomsValue{v}
	})

	assert.Equal(SetKind, tr.Kind())
	assert.Equal(UInt8Kind, tr.Desc.(CompoundDesc).ElemTypes[0].Kind())
	s := r.readSet(tr, nil).NomsValue()
	assert.EqualValues(NewSet(UInt8(0), UInt8(1), UInt8(2), UInt8(3)), s)
}

func TestReadValueSetOfUInt16(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := parseJson(fmt.Sprintf("[%d, %d, %d, [0, 1, 2, 3]]", ValueKind, SetKind, UInt16Kind))
	r := newJsonArrayReader(a, cs)
	tr := r.readTypeRef()

	setTr := MakeCompoundTypeRef("", SetKind, MakePrimitiveTypeRef(UInt16Kind))

	RegisterFromValFunction(setTr, func(v Value) NomsValue {
		return valueAsNomsValue{v}
	})

	assert.Equal(ValueKind, tr.Kind())
	m := r.readTopLevelValue(tr, nil).NomsValue()
	assert.True(NewSet(UInt16(0), UInt16(1), UInt16(2), UInt16(3)).Equals(m))
}

func TestReadStruct(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	// Cannot use parse since it is in a different package that depends on types!
	// struct A1 {
	//   x: Float32
	//   b: Bool
	//   s: String
	// }

	tref := MakeStructTypeRef("A1", []Field{
		Field{"x", MakePrimitiveTypeRef(Int16Kind), false},
		Field{"s", MakePrimitiveTypeRef(StringKind), false},
		Field{"b", MakePrimitiveTypeRef(BoolKind), false},
	}, Choices{})
	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("A1", tref))
	ref := RegisterPackage(&pkg)

	// TODO: Should use ordinal of type and not name
	a := parseJson(fmt.Sprintf(`[%d, "%s", "A1", 42, "hi", true]`, TypeRefKind, ref.String()))
	r := newJsonArrayReader(a, cs)
	tr := r.readTypeRef()

	RegisterFromValFunction(tr, func(v Value) NomsValue {
		return valueAsNomsValue{v}
	})

	assert.Equal(TypeRefKind, tr.Kind())
	v := r.readExternal(tr).NomsValue().(Map)

	assert.True(v.Get(NewString("$name")).Equals(NewString("A1")))
	assert.True(v.Get(NewString("$type")).Equals(tr))
	assert.True(v.Get(NewString("x")).Equals(Int16(42)))
	assert.True(v.Get(NewString("s")).Equals(NewString("hi")))
	assert.True(v.Get(NewString("b")).Equals(Bool(true)))
}

func TestReadStructUnion(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	// Cannot use parse since it is in a different package that depends on types!
	// struct A2 {
	//   x: Float32
	//   union {
	//     b: Bool
	//     s: String
	//   }
	// }

	tref := MakeStructTypeRef("A2", []Field{
		Field{"x", MakePrimitiveTypeRef(Float32Kind), false},
	}, Choices{
		Field{"b", MakePrimitiveTypeRef(BoolKind), false},
		Field{"s", MakePrimitiveTypeRef(StringKind), false},
	})
	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("A2", tref))
	ref := RegisterPackage(&pkg)

	a := parseJson(fmt.Sprintf(`[%d, "%s", "A2", 42, 1, "hi"]`, TypeRefKind, ref.String()))
	r := newJsonArrayReader(a, cs)
	tr := r.readTypeRef()

	RegisterFromValFunction(tr, func(v Value) NomsValue {
		return valueAsNomsValue{v}
	})

	assert.Equal(TypeRefKind, tr.Kind())
	v := r.readExternal(tr).NomsValue().(Map)

	assert.True(v.Get(NewString("$name")).Equals(NewString("A2")))
	assert.True(v.Get(NewString("$type")).Equals(tr))
	assert.True(v.Get(NewString("x")).Equals(Float32(42)))
	assert.False(v.Has(NewString("b")))
	assert.False(v.Has(NewString("s")))
	assert.True(v.Get(NewString("$unionIndex")).Equals(UInt32(1)))
	assert.True(v.Get(NewString("$unionValue")).Equals(NewString("hi")))
}

func TestReadStructOptional(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	// Cannot use parse since it is in a different package that depends on types!
	// struct A3 {
	//   x: Float32
	//   s: optional String
	//   b: optional Bool
	// }

	tref := MakeStructTypeRef("A3", []Field{
		Field{"x", MakePrimitiveTypeRef(Float32Kind), false},
		Field{"s", MakePrimitiveTypeRef(StringKind), true},
		Field{"b", MakePrimitiveTypeRef(BoolKind), true},
	}, Choices{})
	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("A3", tref))
	ref := RegisterPackage(&pkg)

	// TODO: Should use ordinal of type and not name
	a := parseJson(fmt.Sprintf(`[%d, "%s", "A3", 42, false, true, false]`, TypeRefKind, ref.String()))
	r := newJsonArrayReader(a, cs)
	tr := r.readTypeRef()

	RegisterFromValFunction(tr, func(v Value) NomsValue {
		return valueAsNomsValue{v}
	})

	assert.Equal(TypeRefKind, tr.Kind())
	v := r.readExternal(tr).NomsValue().(Map)

	assert.True(v.Get(NewString("$name")).Equals(NewString("A3")))
	assert.True(v.Get(NewString("$type")).Equals(tr))
	assert.True(v.Get(NewString("x")).Equals(Float32(42)))
	assert.False(v.Has(NewString("s")))
	assert.True(v.Get(NewString("b")).Equals(Bool(false)))
}

func TestReadStructWithList(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	// Cannot use parse since it is in a different package that depends on types!
	// struct A4 {
	//   b: Bool
	//   l: List(Int32)
	//   s: String
	// }

	tref := MakeStructTypeRef("A4", []Field{
		Field{"b", MakePrimitiveTypeRef(BoolKind), false},
		Field{"l", MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(Int32Kind)), false},
		Field{"s", MakePrimitiveTypeRef(StringKind), false},
	}, Choices{})
	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("A4", tref))
	ref := RegisterPackage(&pkg)

	// TODO: Should use ordinal of type and not name
	a := parseJson(fmt.Sprintf(`[%d, "%s", "A4", true, [0, 1, 2], "hi"]`, TypeRefKind, ref.String()))
	r := newJsonArrayReader(a, cs)
	tr := r.readTypeRef()
	l32Tr := MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(Int32Kind))

	RegisterFromValFunction(tr, func(v Value) NomsValue {
		return valueAsNomsValue{v}
	})
	RegisterFromValFunction(l32Tr, func(v Value) NomsValue {
		return valueAsNomsValue{v}
	})

	assert.Equal(TypeRefKind, tr.Kind())
	v := r.readExternal(tr).NomsValue().(Map)

	assert.True(v.Get(NewString("$name")).Equals(NewString("A4")))
	assert.True(v.Get(NewString("$type")).Equals(tr))
	assert.True(v.Get(NewString("b")).Equals(Bool(true)))
	assert.True(v.Get(NewString("l")).Equals(NewList(Int32(0), Int32(1), Int32(2))))
	assert.True(v.Get(NewString("s")).Equals(NewString("hi")))
}

func TestReadStructWithValue(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	// Cannot use parse since it is in a different package that depends on types!
	// struct A5 {
	//   b: Bool
	//   v: Value
	//   s: String
	// }

	tref := MakeStructTypeRef("A5", []Field{
		Field{"b", MakePrimitiveTypeRef(BoolKind), false},
		Field{"v", MakePrimitiveTypeRef(ValueKind), false},
		Field{"s", MakePrimitiveTypeRef(StringKind), false},
	}, Choices{})
	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("A5", tref))
	ref := RegisterPackage(&pkg)

	// TODO: Should use ordinal of type and not name
	a := parseJson(fmt.Sprintf(`[%d, "%s", "A5", true, %d, 42, "hi"]`, TypeRefKind, ref.String(), UInt8Kind))
	r := newJsonArrayReader(a, cs)
	tr := r.readTypeRef()

	RegisterFromValFunction(tr, func(v Value) NomsValue {
		return valueAsNomsValue{v}
	})

	assert.Equal(TypeRefKind, tr.Kind())
	v := r.readExternal(tr).NomsValue().(Map)

	assert.True(v.Get(NewString("$name")).Equals(NewString("A5")))
	assert.True(v.Get(NewString("$type")).Equals(tr))
	assert.True(v.Get(NewString("b")).Equals(Bool(true)))
	assert.True(v.Get(NewString("v")).Equals(UInt8(42)))
	assert.True(v.Get(NewString("s")).Equals(NewString("hi")))
}

func TestReadValueStruct(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	// Cannot use parse since it is in a different package that depends on types!
	// struct A1 {
	//   x: Float32
	//   b: Bool
	//   s: String
	// }

	tref := MakeStructTypeRef("A1", []Field{
		Field{"x", MakePrimitiveTypeRef(Int16Kind), false},
		Field{"s", MakePrimitiveTypeRef(StringKind), false},
		Field{"b", MakePrimitiveTypeRef(BoolKind), false},
	}, Choices{})
	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("A1", tref))
	pkgRef := RegisterPackage(&pkg)

	// TODO: Should use ordinal of type and not name
	a := parseJson(fmt.Sprintf(`[%d, %d, "%s", "A1", 42, "hi", true]`, ValueKind, TypeRefKind, pkgRef.String()))
	r := newJsonArrayReader(a, cs)
	tr := r.readTypeRef()

	structTr := MakeTypeRef("A1", pkgRef)
	RegisterFromValFunction(structTr, func(v Value) NomsValue {
		return valueAsNomsValue{v}
	})

	assert.Equal(ValueKind, tr.Kind())
	v := r.readTopLevelValue(tr, &pkg).NomsValue().(Map)

	assert.True(v.Get(NewString("$name")).Equals(NewString("A1")))
	assert.True(v.Get(NewString("$type")).Equals(structTr))
	assert.True(v.Get(NewString("x")).Equals(Int16(42)))
	assert.True(v.Get(NewString("s")).Equals(NewString("hi")))
	assert.True(v.Get(NewString("b")).Equals(Bool(true)))
}

func TestReadEnum(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	tref := MakeEnumTypeRef("E", "a", "b", "c")
	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("E", tref))
	ref := RegisterPackage(&pkg)

	// TODO: Should use ordinal of type and not name
	a := parseJson(fmt.Sprintf(`[%d, "%s", "E", 1]`, TypeRefKind, ref.String()))
	r := newJsonArrayReader(a, cs)
	tr := r.readTypeRef()
	assert.Equal(TypeRefKind, tr.Kind())
	v := r.readExternal(tr).NomsValue()
	assert.Equal(uint32(1), uint32(v.(UInt32)))
}

func TestReadValueEnum(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	tref := MakeEnumTypeRef("E", "a", "b", "c")
	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("E", tref))
	ref := RegisterPackage(&pkg)

	// TODO: Should use ordinal of type and not name
	a := parseJson(fmt.Sprintf(`[%d, %d, "%s", "E", 1]`, ValueKind, TypeRefKind, ref.String()))
	r := newJsonArrayReader(a, cs)
	tr := r.readTypeRef()
	assert.Equal(ValueKind, tr.Kind())
	v := r.readTopLevelValue(tr, &pkg).NomsValue()
	assert.Equal(uint32(1), uint32(v.(UInt32)))
}

func TestReadRef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	r := ref.Parse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")

	a := parseJson(fmt.Sprintf(`[%d, %d, "%s"]`, RefKind, UInt32Kind, r.String()))
	reader := newJsonArrayReader(a, cs)
	tr := reader.readTypeRef()
	RegisterFromValFunction(tr, func(v Value) NomsValue {
		return valueAsNomsValue{v}
	})
	assert.Equal(RefKind, tr.Kind())
	assert.Equal(UInt32Kind, tr.Desc.(CompoundDesc).ElemTypes[0].Kind())
	rOut := reader.readRefValue(tr).NomsValue()
	assert.True(Ref{r}.Equals(rOut))
}

func TestReadValueRef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	r := ref.Parse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")

	a := parseJson(fmt.Sprintf(`[%d, %d, %d, "%s"]`, ValueKind, RefKind, UInt32Kind, r.String()))
	reader := newJsonArrayReader(a, cs)
	tr := reader.readTypeRef()

	refTypeRef := MakeCompoundTypeRef("", RefKind, MakePrimitiveTypeRef(UInt32Kind))
	RegisterFromValFunction(refTypeRef, func(v Value) NomsValue {
		return valueAsNomsValue{v}
	})
	assert.Equal(ValueKind, tr.Kind())
	rOut := reader.readTopLevelValue(tr, nil).NomsValue()
	assert.True(Ref{r}.Equals(rOut))
}

func TestReadStructWithEnum(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	// Cannot use parse since it is in a different package that depends on types!
	// enum E {
	//   a
	//   b
	// }
	// struct A1 {
	//   x: Float32
	//   e: E
	//   s: String
	// }

	structTref := MakeStructTypeRef("A1", []Field{
		Field{"x", MakePrimitiveTypeRef(Int16Kind), false},
		Field{"e", MakeTypeRef("E", ref.Ref{}), false},
		Field{"b", MakePrimitiveTypeRef(BoolKind), false},
	}, Choices{})
	enumTref := MakeEnumTypeRef("E", "a", "b", "c")
	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("A1", structTref).Set("E", enumTref))
	pkgRef := RegisterPackage(&pkg)

	// TODO: Should use ordinal of type and not name
	a := parseJson(fmt.Sprintf(`[%d, "%s", "A1", 42, 1, true]`, TypeRefKind, pkgRef.String()))
	r := newJsonArrayReader(a, cs)
	tr := r.readTypeRef()

	// structTr := MakeTypeRef("A1", ref)
	RegisterFromValFunction(tr, func(v Value) NomsValue {
		return valueAsNomsValue{v}
	})

	assert.Equal(TypeRefKind, tr.Kind())
	v := r.readTopLevelValue(tr, &pkg).NomsValue().(Map)

	assert.True(v.Get(NewString("$name")).Equals(NewString("A1")))
	assert.True(v.Get(NewString("$type")).Equals(tr))
	assert.True(v.Get(NewString("x")).Equals(Int16(42)))
	assert.True(v.Get(NewString("e")).Equals(UInt32(1)))
	assert.True(v.Get(NewString("b")).Equals(Bool(true)))
}
