package types

import (
	"encoding/base64"
	"io/ioutil"
	"strings"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

func fromTypedEncodeable(i []interface{}, cs chunks.ChunkStore) Value {
	r := newJsonArrayReader(i, cs)
	return r.readTopLevelValue()
}

type jsonArrayReader struct {
	a  []interface{}
	i  int
	cs chunks.ChunkStore
}

func newJsonArrayReader(a []interface{}, cs chunks.ChunkStore) *jsonArrayReader {
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
	case ListKind, SetKind, RefKind, MetaSequenceKind:
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
		ordinal := int16(r.read().(float64))
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
	// TODO: Skip the List wrapper.
	return valueFromType(r.cs, newListNoCopy(r.cs, data, t), t)
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
	// TODO: Skip the Set wrapper.
	return valueFromType(r.cs, newSetFromData(r.cs, data, t), t)
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
	// TODO: Skip the Map wrapper.
	return valueFromType(r.cs, newMapFromData(r.cs, data, t), t)
}

func indexTypeForMetaSequence(t Type) Type {
	desc := t.Desc.(CompoundDesc)
	concreteType := desc.ElemTypes[0]

	switch concreteType.Kind() {
	case MapKind, SetKind:
		return concreteType.Desc.(CompoundDesc).ElemTypes[0]
	case BlobKind, ListKind:
		return MakePrimitiveType(Uint64Kind)
	}

	panic("unreached")
}

func (r *jsonArrayReader) readMetaSequence(t Type, pkg *Package) Value {
	data := metaSequenceData{}
	indexType := indexTypeForMetaSequence(t)
	for !r.atEnd() {
		ref := r.readRef()
		v := r.readValueWithoutTag(indexType, pkg)
		data = append(data, metaTuple{ref, v})
	}

	t = fixupType(t, pkg)
	// Denormalize the type. Compound objects must return the same Type() as their leaf counterparts.
	concreteType := t.Desc.(CompoundDesc).ElemTypes[0]
	return newMetaSequenceFromData(data, concreteType, r.cs)
}

func (r *jsonArrayReader) readEnum(t Type, pkg *Package) Value {
	t = fixupType(t, pkg)
	return enumFromType(uint32(r.read().(float64)), t)
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
		return r.readBlob(t)
	case BoolKind:
		return Bool(r.read().(bool))
	case Uint8Kind:
		return Uint8(r.read().(float64))
	case Uint16Kind:
		return Uint16(r.read().(float64))
	case Uint32Kind:
		return Uint32(r.read().(float64))
	case Uint64Kind:
		return Uint64(r.read().(float64))
	case Int8Kind:
		return Int8(r.read().(float64))
	case Int16Kind:
		return Int16(r.read().(float64))
	case Int32Kind:
		return Int32(r.read().(float64))
	case Int64Kind:
		return Int64(r.read().(float64))
	case Float32Kind:
		return Float32(r.read().(float64))
	case Float64Kind:
		return Float64(r.read().(float64))
	case StringKind:
		return NewString(r.readString())
	case ValueKind:
		// The value is always tagged
		t := r.readTypeAsTag()
		return r.readValueWithoutTag(t, pkg)
	case ListKind:
		r2 := newJsonArrayReader(r.readArray(), r.cs)
		return r2.readList(t, pkg)
	case MapKind:
		r2 := newJsonArrayReader(r.readArray(), r.cs)
		return r2.readMap(t, pkg)
	case PackageKind:
		return r.readPackage(t, pkg)
	case RefKind:
		return r.readRefValue(t, pkg)
	case SetKind:
		r2 := newJsonArrayReader(r.readArray(), r.cs)
		return r2.readSet(t, pkg)
	case EnumKind, StructKind:
		panic("not allowed")
	case TypeKind:
		return r.readTypeKindToValue(t, pkg)
	case UnresolvedKind:
		return r.readUnresolvedKindToValue(t, pkg)
	case MetaSequenceKind:
		r2 := newJsonArrayReader(r.readArray(), r.cs)
		return r2.readMetaSequence(t, pkg)
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
			pkg = readPackage(pkgRef, r.cs)
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
	case ListKind, MapKind, RefKind, SetKind, MetaSequenceKind:
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
		ordinal := int16(r.read().(float64))
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
	switch desc := tr.Desc.(type) {
	case EnumDesc, StructDesc:
		panic("unreachable")
	case PrimitiveDesc:
		return tr
	case CompoundDesc:
		elemTypes := make([]Type, len(desc.ElemTypes))
		for i, elemType := range desc.ElemTypes {
			elemTypes[i] = fixupType(elemType, pkg)
		}
		return MakeCompoundType(tr.Kind(), elemTypes...)
	case UnresolvedDesc:
		if tr.HasPackageRef() {
			return tr
		}
		return MakeType(pkg.Ref(), tr.Ordinal())
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
		unionIndex := uint32(r.read().(float64))
		values = append(values, Uint32(unionIndex), r.readValueWithoutTag(desc.Union[unionIndex].T, pkg))
	}

	typ = fixupType(typ, pkg)
	return structBuilderForType(r.cs, values, typ, typeDef)
}
