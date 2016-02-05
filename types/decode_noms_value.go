package types

import (
	"encoding/base64"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

func fromTypedEncodeable(i []interface{}, cs chunks.ChunkSource) Value {
	r := newJsonArrayReader(i, cs)
	return r.readTopLevelValue()
}

type jsonArrayReader struct {
	a  []interface{}
	i  int
	cs chunks.ChunkSource
}

func newJsonArrayReader(a []interface{}, cs chunks.ChunkSource) *jsonArrayReader {
	return &jsonArrayReader{a: a, i: 0, cs: cs}
}

func (r *jsonArrayReader) read() interface{} {
	v := r.a[r.i]
	r.i++
	return v
}

func (r *jsonArrayReader) atEnd() bool {
	return r.i >= len(r.a)
}

func (r *jsonArrayReader) readString() string {
	return r.read().(string)
}

func (r *jsonArrayReader) readBool() bool {
	return r.read().(bool)
}

func (r *jsonArrayReader) readFloat() float64 {
	v, err := strconv.ParseFloat(r.readString(), 64)
	d.Chk.Nil(err)
	return v
}

func (r *jsonArrayReader) readInt() int64 {
	v, err := strconv.ParseInt(r.readString(), 10, 64)
	d.Chk.Nil(err)
	return v
}

func (r *jsonArrayReader) readUint() uint64 {
	v, err := strconv.ParseUint(r.readString(), 10, 64)
	d.Chk.Nil(err)
	return v
}

func (r *jsonArrayReader) readArray() []interface{} {
	return r.read().([]interface{})
}

func (r *jsonArrayReader) readKind() NomsKind {
	return NomsKind(r.read().(float64))
}

func (r *jsonArrayReader) readRef() ref.Ref {
	s := r.readString()
	return ref.Parse(s)
}

func (r *jsonArrayReader) readTypeAsTag() Type {
	kind := r.readKind()
	switch kind {
	case ListKind, SetKind, RefKind:
		elemType := r.readTypeAsTag()
		return MakeCompoundType(kind, elemType)
	case MapKind:
		keyType := r.readTypeAsTag()
		valueType := r.readTypeAsTag()
		return MakeCompoundType(kind, keyType, valueType)
	case TypeKind:
		return MakePrimitiveType(TypeKind)
	case UnresolvedKind:
		pkgRef := r.readRef()
		ordinal := int16(r.readInt())
		d.Chk.NotEqual(int16(-1), ordinal)
		return MakeType(pkgRef, ordinal)
	}

	if IsPrimitiveKind(kind) {
		return MakePrimitiveType(kind)
	}

	panic("unreachable")
}

func (r *jsonArrayReader) readBlob(t Type) Value {
	s := r.readString()
	decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(s))
	b, err := ioutil.ReadAll(decoder)
	d.Exp.NoError(err)
	return newBlobLeaf(b)
}

func (r *jsonArrayReader) readList(t Type, pkg *Package) Value {
	desc := t.Desc.(CompoundDesc)
	data := []Value{}
	elemType := desc.ElemTypes[0]
	for !r.atEnd() {
		v := r.readValueWithoutTag(elemType, pkg)
		data = append(data, v)
	}

	t = fixupType(t, pkg)
	return valueFromType(newListLeaf(r.cs, t, data...), t)
}

func (r *jsonArrayReader) readSet(t Type, pkg *Package) Value {
	desc := t.Desc.(CompoundDesc)
	data := setData{}
	elemType := desc.ElemTypes[0]
	for !r.atEnd() {
		v := r.readValueWithoutTag(elemType, pkg)
		data = append(data, v)
	}

	t = fixupType(t, pkg)
	return valueFromType(newSetLeaf(r.cs, t, data...), t)
}

func (r *jsonArrayReader) readMap(t Type, pkg *Package) Value {
	desc := t.Desc.(CompoundDesc)
	data := mapData{}
	keyType := desc.ElemTypes[0]
	valueType := desc.ElemTypes[1]

	for !r.atEnd() {
		k := r.readValueWithoutTag(keyType, pkg)
		v := r.readValueWithoutTag(valueType, pkg)
		data = append(data, mapEntry{k, v})
	}

	t = fixupType(t, pkg)
	return valueFromType(newMapLeaf(r.cs, t, data...), t)
}

func indexTypeForMetaSequence(t Type) Type {
	switch t.Kind() {
	case MapKind, SetKind:
		elemType := t.Desc.(CompoundDesc).ElemTypes[0]
		if elemType.IsOrdered() {
			return elemType
		} else {
			return MakeCompoundType(RefKind, MakePrimitiveType(ValueKind))
		}
	case BlobKind, ListKind:
		return MakePrimitiveType(Uint64Kind)
	}

	panic("unreached")
}

func (r *jsonArrayReader) maybeReadMetaSequence(t Type, pkg *Package) (Value, bool) {
	if !r.read().(bool) {
		return nil, false
	}

	r2 := newJsonArrayReader(r.readArray(), r.cs)
	data := metaSequenceData{}
	indexType := indexTypeForMetaSequence(t)
	for !r2.atEnd() {
		ref := r2.readRef()
		v := r2.readValueWithoutTag(indexType, pkg)
		data = append(data, metaTuple{nil, ref, v})
	}

	t = fixupType(t, pkg)
	return newMetaSequenceFromData(data, t, r.cs), true
}

func (r *jsonArrayReader) readEnum(t Type, pkg *Package) Value {
	t = fixupType(t, pkg)
	return enumFromType(uint32(r.readUint()), t)
}

func (r *jsonArrayReader) readPackage(t Type, pkg *Package) Value {
	r2 := newJsonArrayReader(r.readArray(), r.cs)
	types := []Type{}
	for !r2.atEnd() {
		types = append(types, r2.readTypeAsValue(pkg))
	}

	r3 := newJsonArrayReader(r.readArray(), r.cs)
	deps := []ref.Ref{}
	for !r3.atEnd() {
		deps = append(deps, r3.readRef())
	}

	return NewPackage(types, deps)
}

func (r *jsonArrayReader) readRefValue(t Type, pkg *Package) Value {
	ref := r.readRef()
	t = fixupType(t, pkg)
	return refFromType(ref, t)
}

func (r *jsonArrayReader) readTopLevelValue() Value {
	t := r.readTypeAsTag()
	return r.readValueWithoutTag(t, nil)
}

func (r *jsonArrayReader) readValueWithoutTag(t Type, pkg *Package) Value {
	switch t.Kind() {
	case BlobKind:
		if ms, ok := r.maybeReadMetaSequence(t, pkg); ok {
			return ms
		}

		return r.readBlob(t)
	case BoolKind:
		return Bool(r.read().(bool))
	case Uint8Kind:
		return Uint8(r.readUint())
	case Uint16Kind:
		return Uint16(r.readUint())
	case Uint32Kind:
		return Uint32(r.readUint())
	case Uint64Kind:
		return Uint64(r.readUint())
	case Int8Kind:
		return Int8(r.readInt())
	case Int16Kind:
		return Int16(r.readInt())
	case Int32Kind:
		return Int32(r.readInt())
	case Int64Kind:
		return Int64(r.readInt())
	case Float32Kind:
		return Float32(r.readFloat())
	case Float64Kind:
		return Float64(r.readFloat())
	case StringKind:
		return NewString(r.readString())
	case ValueKind:
		// The value is always tagged
		t := r.readTypeAsTag()
		return r.readValueWithoutTag(t, pkg)
	case ListKind:
		if ms, ok := r.maybeReadMetaSequence(t, pkg); ok {
			return ms
		}

		r2 := newJsonArrayReader(r.readArray(), r.cs)
		return r2.readList(t, pkg)
	case MapKind:
		if ms, ok := r.maybeReadMetaSequence(t, pkg); ok {
			return ms
		}

		r2 := newJsonArrayReader(r.readArray(), r.cs)
		return r2.readMap(t, pkg)
	case PackageKind:
		return r.readPackage(t, pkg)
	case RefKind:
		return r.readRefValue(t, pkg)
	case SetKind:
		if ms, ok := r.maybeReadMetaSequence(t, pkg); ok {
			return ms
		}

		r2 := newJsonArrayReader(r.readArray(), r.cs)
		return r2.readSet(t, pkg)
	case EnumKind, StructKind:
		panic("not allowed")
	case TypeKind:
		return r.readTypeKindToValue(t, pkg)
	case UnresolvedKind:
		return r.readUnresolvedKindToValue(t, pkg)
	}
	panic("not reachable")
}

func (r *jsonArrayReader) readTypeKindToValue(t Type, pkg *Package) Value {
	d.Chk.IsType(PrimitiveDesc(0), t.Desc)
	return r.readTypeAsValue(pkg)
}

func (r *jsonArrayReader) readUnresolvedKindToValue(t Type, pkg *Package) Value {
	// When we have a struct referencing another struct/enum in the same package the package ref is empty. In that case we use the package that is passed into this function.
	d.Chk.True(t.IsUnresolved())
	pkgRef := t.PackageRef()
	ordinal := t.Ordinal()
	if !pkgRef.IsEmpty() {
		pkg2 := LookupPackage(pkgRef)
		if pkg2 != nil {
			pkg = pkg2
		} else {
			pkg = ReadPackage(pkgRef, r.cs)
		}
	}

	d.Chk.NotNil(pkg, "Woah, got a nil pkg. pkgRef: %s, ordinal: %d\n", pkgRef, ordinal)

	typeDef := pkg.types[ordinal]

	if typeDef.Kind() == EnumKind {
		return r.readEnum(t, pkg)
	}

	d.Chk.Equal(StructKind, typeDef.Kind())
	return r.readStruct(typeDef, t, pkg)
}

func (r *jsonArrayReader) readTypeAsValue(pkg *Package) Type {
	k := r.readKind()
	switch k {
	case EnumKind:
		name := r.readString()
		r2 := newJsonArrayReader(r.readArray(), r.cs)
		ids := []string{}
		for !r2.atEnd() {
			ids = append(ids, r2.readString())
		}
		return MakeEnumType(name, ids...)
	case ListKind, MapKind, RefKind, SetKind:
		r2 := newJsonArrayReader(r.readArray(), r.cs)
		elemTypes := []Type{}
		for !r2.atEnd() {
			t := r2.readTypeAsValue(pkg)
			elemTypes = append(elemTypes, t)
		}
		return MakeCompoundType(k, elemTypes...)
	case StructKind:
		name := r.readString()

		fields := []Field{}
		choices := Choices{}

		fieldReader := newJsonArrayReader(r.readArray(), r.cs)
		for !fieldReader.atEnd() {
			fieldName := fieldReader.readString()
			fieldType := fieldReader.readTypeAsValue(pkg)
			optional := fieldReader.readBool()
			fields = append(fields, Field{Name: fieldName, T: fieldType, Optional: optional})
		}
		choiceReader := newJsonArrayReader(r.readArray(), r.cs)
		for !choiceReader.atEnd() {
			fieldName := choiceReader.readString()
			fieldType := choiceReader.readTypeAsValue(pkg)
			optional := choiceReader.readBool()
			choices = append(choices, Field{Name: fieldName, T: fieldType, Optional: optional})
		}
		return MakeStructType(name, fields, choices)
	case UnresolvedKind:
		pkgRef := r.readRef()
		ordinal := int16(r.readInt())
		if ordinal == -1 {
			namespace := r.readString()
			name := r.readString()
			d.Chk.True(pkgRef.IsEmpty(), "Unresolved Type may not have a package ref")
			return MakeUnresolvedType(namespace, name)
		}
		return MakeType(pkgRef, ordinal)
	}

	d.Chk.True(IsPrimitiveKind(k))
	return MakePrimitiveType(k)
}

// fixupType goes trough the object graph of tr and updates the PackageRef to pkg if the the old PackageRef was an empty ref.
func fixupType(tr Type, pkg *Package) Type {
	t, _ := fixupTypeInternal(tr, pkg)
	return t
}

func fixupTypeInternal(tr Type, pkg *Package) (Type, bool) {
	switch desc := tr.Desc.(type) {
	case EnumDesc, StructDesc:
		panic("unreachable")
	case PrimitiveDesc:
		return tr, false
	case CompoundDesc:
		elemTypes := make([]Type, len(desc.ElemTypes))
		changed := false
		for i, elemType := range desc.ElemTypes {
			if t, c := fixupTypeInternal(elemType, pkg); c {
				changed = true
				elemTypes[i] = t
			} else {
				elemTypes[i] = elemType
			}
		}

		if !changed {
			return tr, false
		}

		return MakeCompoundType(tr.Kind(), elemTypes...), true
	case UnresolvedDesc:
		if tr.HasPackageRef() {
			return tr, false
		}
		return MakeType(pkg.Ref(), tr.Ordinal()), true
	}
	panic("unreachable")
}

func (r *jsonArrayReader) readStruct(typeDef, typ Type, pkg *Package) Value {
	// We've read `[StructKind, sha1, name` at this point
	values := []Value{}
	desc := typeDef.Desc.(StructDesc)
	for _, f := range desc.Fields {
		if f.Optional {
			b := r.read().(bool)
			values = append(values, Bool(b))
			if b {
				values = append(values, r.readValueWithoutTag(f.T, pkg))
			}
		} else {
			values = append(values, r.readValueWithoutTag(f.T, pkg))
		}
	}
	if len(desc.Union) > 0 {
		unionIndex := uint32(r.readUint())
		values = append(values, Uint32(unionIndex), r.readValueWithoutTag(desc.Union[unionIndex].T, pkg))
	}

	typ = fixupType(typ, pkg)
	return structBuilderForType(values, typ, typeDef)
}
