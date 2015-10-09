package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type valueAsNomsValue struct {
	Value
	t TypeRef
}

func (v valueAsNomsValue) NomsValue() Value {
	return v.Value
}

func (v valueAsNomsValue) TypeRef() TypeRef {
	return v.t
}

func fromTypedEncodeable(w typedValueWrapper, cs chunks.ChunkSource) NomsValue {
	i := w.TypedValue()
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

func (r *jsonArrayReader) peek() interface{} {
	return r.a[r.i]
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

func (r *jsonArrayReader) readTypeRefAsTag() TypeRef {
	kind := r.readKind()
	switch kind {
	case ListKind, SetKind, RefKind:
		elemType := r.readTypeRefAsTag()
		return MakeCompoundTypeRef("", kind, elemType)
	case MapKind:
		keyType := r.readTypeRefAsTag()
		valueType := r.readTypeRefAsTag()
		return MakeCompoundTypeRef("", kind, keyType, valueType)
	case TypeRefKind:
		// TypeRefKind can mean that we have a TypeRef primitive or a TypeRef pointing to a TypeRef in a Package. Do a lookahead.
		i := r.peek()
		if _, ok := i.(float64); ok {
			return MakePrimitiveTypeRef(TypeRefKind)
		}

		pkgRef := r.readRef()
		// TODO: Should be the ordinal
		name := r.readString()
		return MakeTypeRef(name, pkgRef)
	}

	if IsPrimitiveKind(kind) {
		return MakePrimitiveTypeRef(kind)
	}

	panic("unreachable")
}

func (r *jsonArrayReader) readList(t TypeRef, pkg *Package) NomsValue {
	desc := t.Desc.(CompoundDesc)
	ll := []Value{}
	elemType := desc.ElemTypes[0]
	for !r.atEnd() {
		v := r.readValueWithoutTag(elemType, pkg)
		ll = append(ll, v.NomsValue())
	}

	return ToNomsValueFromTypeRef(t, NewList(ll...))
}

func (r *jsonArrayReader) readSet(t TypeRef, pkg *Package) NomsValue {
	desc := t.Desc.(CompoundDesc)
	ll := []Value{}
	elemType := desc.ElemTypes[0]
	for !r.atEnd() {
		v := r.readValueWithoutTag(elemType, pkg)
		ll = append(ll, v.NomsValue())
	}
	return ToNomsValueFromTypeRef(t, NewSet(ll...))
}

func (r *jsonArrayReader) readMap(t TypeRef, pkg *Package) NomsValue {
	desc := t.Desc.(CompoundDesc)
	ll := []Value{}
	keyType := desc.ElemTypes[0]
	valueType := desc.ElemTypes[1]
	for !r.atEnd() {
		k := r.readValueWithoutTag(keyType, pkg)
		v := r.readValueWithoutTag(valueType, pkg)
		ll = append(ll, k.NomsValue(), v.NomsValue())
	}
	return ToNomsValueFromTypeRef(t, NewMap(ll...))
}

func (r *jsonArrayReader) readEnum(t TypeRef) NomsValue {
	return valueAsNomsValue{UInt32(r.read().(float64)), t}
}

func (r *jsonArrayReader) readRefValue(t TypeRef) NomsValue {
	ref := r.readRef()
	v := Ref{R: ref}
	return ToNomsValueFromTypeRef(t, v)
}

func (r *jsonArrayReader) readTopLevelValue() NomsValue {
	t := r.readTypeRefAsTag()
	return r.readValueWithoutTag(t, nil)
}

func (r *jsonArrayReader) readValueWithoutTag(t TypeRef, pkg *Package) NomsValue {
	switch t.Kind() {
	case BoolKind:
		return valueAsNomsValue{Bool(r.read().(bool)), t}
	case UInt8Kind:
		return valueAsNomsValue{UInt8(r.read().(float64)), t}
	case UInt16Kind:
		return valueAsNomsValue{UInt16(r.read().(float64)), t}
	case UInt32Kind:
		return valueAsNomsValue{UInt32(r.read().(float64)), t}
	case UInt64Kind:
		return valueAsNomsValue{UInt64(r.read().(float64)), t}
	case Int8Kind:
		return valueAsNomsValue{Int8(r.read().(float64)), t}
	case Int16Kind:
		return valueAsNomsValue{Int16(r.read().(float64)), t}
	case Int32Kind:
		return valueAsNomsValue{Int32(r.read().(float64)), t}
	case Int64Kind:
		return valueAsNomsValue{Int64(r.read().(float64)), t}
	case Float32Kind:
		return valueAsNomsValue{Float32(r.read().(float64)), t}
	case Float64Kind:
		return valueAsNomsValue{Float64(r.read().(float64)), t}
	case StringKind:
		return valueAsNomsValue{NewString(r.readString()), t}
	case BlobKind:
		panic("not implemented")
	case ValueKind:
		// The value is always tagged
		t := r.readTypeRefAsTag()
		return r.readValueWithoutTag(t, pkg)
	case ListKind:
		r2 := newJsonArrayReader(r.readArray(), r.cs)
		return r2.readList(t, pkg)
	case MapKind:
		r2 := newJsonArrayReader(r.readArray(), r.cs)
		return r2.readMap(t, pkg)
	case RefKind:
		return r.readRefValue(t)
	case SetKind:
		r2 := newJsonArrayReader(r.readArray(), r.cs)
		return r2.readSet(t, pkg)
	case EnumKind, StructKind:
		panic("not allowed")
	case TypeRefKind:
		return r.readTypeRefKindToValue(t, pkg)
	}
	panic("not reachable")
}

func (r *jsonArrayReader) readTypeRefKindToValue(t TypeRef, pkg *Package) NomsValue {
	if _, ok := t.Desc.(PrimitiveDesc); ok {
		return valueAsNomsValue{r.readTypeRefAsValue(pkg), t}
	}

	pkgRef := t.PackageRef()
	name := t.Name()
	pkg2 := LookupPackage(pkgRef)
	if pkg2 != nil {
		pkg = pkg2
	}

	typeDef := pkg.NamedTypes().Get(name)
	if typeDef.Kind() == EnumKind {
		return r.readEnum(t)
	}

	d.Chk.Equal(StructKind, typeDef.Kind())
	return r.readStruct(typeDef, t, pkg)
}

func (r *jsonArrayReader) readTypeRefAsValue(pkg *Package) TypeRef {
	k := r.readKind()
	switch k {
	case EnumKind:
		name := r.readString()
		r2 := newJsonArrayReader(r.readArray(), r.cs)
		ids := []string{}
		for !r2.atEnd() {
			ids = append(ids, r2.readString())
		}
		return MakeEnumTypeRef(name, ids...)
	case ListKind, MapKind, RefKind, SetKind:
		r2 := newJsonArrayReader(r.readArray(), r.cs)
		elemTypes := []TypeRef{}
		for !r2.atEnd() {
			t := r2.readTypeRefAsValue(pkg)
			elemTypes = append(elemTypes, t)
		}
		return MakeCompoundTypeRef("", k, elemTypes...)
	case StructKind:
		name := r.readString()

		fields := []Field{}
		choices := Choices{}

		fieldReader := newJsonArrayReader(r.readArray(), r.cs)
		for !fieldReader.atEnd() {
			fieldName := fieldReader.readString()
			fieldType := fieldReader.readTypeRefAsValue(pkg)
			optional := fieldReader.readBool()
			fields = append(fields, Field{Name: fieldName, T: fieldType, Optional: optional})
		}
		choiceReader := newJsonArrayReader(r.readArray(), r.cs)
		for !choiceReader.atEnd() {
			fieldName := choiceReader.readString()
			fieldType := choiceReader.readTypeRefAsValue(pkg)
			optional := choiceReader.readBool()
			choices = append(choices, Field{Name: fieldName, T: fieldType, Optional: optional})
		}
		return MakeStructTypeRef(name, fields, choices)

	case TypeRefKind:
		pkgRef := r.readRef()
		name := r.readString()
		return MakeTypeRef(name, pkgRef)
	}

	d.Chk.True(IsPrimitiveKind(k))
	return MakePrimitiveTypeRef(k)
}

// fixupTypeRef goes trough the object graph of tr and updates the PackageRef to pkg if the the old PackageRef was an empty ref.
func fixupTypeRef(tr TypeRef, pkg *Package) TypeRef {
	switch desc := tr.Desc.(type) {
	case EnumDesc, PrimitiveDesc:
		return tr
	case CompoundDesc:
		elemTypes := make([]TypeRef, len(desc.ElemTypes))
		for i, elemType := range desc.ElemTypes {
			elemTypes[i] = fixupTypeRef(elemType, pkg)
		}
		return MakeCompoundTypeRef(tr.Name(), tr.Kind(), elemTypes...)
	case StructDesc:
		fields := make([]Field, len(desc.Fields))
		for i, f := range desc.Fields {
			fields[i] = Field{f.Name, fixupTypeRef(f.T, pkg), f.Optional}
		}
		choices := make(Choices, len(desc.Union))
		for i, c := range desc.Union {
			choices[i] = Field{c.Name, fixupTypeRef(c.T, pkg), c.Optional}
		}
		return MakeStructTypeRef(tr.Name(), fields, choices)
	case UnresolvedDesc:
		if tr.PackageRef() != (ref.Ref{}) {
			return tr
		}
		return MakeTypeRef(tr.Name(), pkg.Ref())
	}
	if tr.Kind() == TypeRefKind {
	}
	panic("unreachable")
}

func (r *jsonArrayReader) readStruct(typeDef, typeRef TypeRef, pkg *Package) NomsValue {
	typeRef = fixupTypeRef(typeRef, pkg)
	typeDef = fixupTypeRef(typeDef, pkg)

	// We've read `[StructKind, sha1, name` at this point
	desc := typeDef.Desc.(StructDesc)
	m := NewMap(
		NewString("$name"), NewString(typeRef.Name()),
		NewString("$type"), typeRef)

	for _, f := range desc.Fields {
		if f.Optional {
			b := r.read().(bool)
			if b {
				v := r.readValueWithoutTag(f.T, pkg)
				m = m.Set(NewString(f.Name), v.NomsValue())
			}
		} else {
			v := r.readValueWithoutTag(f.T, pkg)
			m = m.Set(NewString(f.Name), v.NomsValue())
		}
	}
	if len(desc.Union) > 0 {
		i := uint32(r.read().(float64))
		m = m.Set(NewString("$unionIndex"), UInt32(i))
		v := r.readValueWithoutTag(desc.Union[i].T, pkg)
		m = m.Set(NewString("$unionValue"), v.NomsValue())
	}

	return ToNomsValueFromTypeRef(typeRef, m)
}
