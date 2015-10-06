package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type valueAsNomsValue struct {
	v Value
}

func (v valueAsNomsValue) NomsValue() Value {
	return v.v
}

func (v valueAsNomsValue) TypeRef() TypeRef {
	return v.TypeRef()
}

func (v valueAsNomsValue) Ref() ref.Ref {
	return v.Ref()
}

func (v valueAsNomsValue) Chunks() []Future {
	return v.Chunks()
}

func (v valueAsNomsValue) Equals(other Value) bool {
	return v.Equals(other)
}

func fromTypedEncodeable(w typedValueWrapper, cs chunks.ChunkSource) NomsValue {
	i := w.TypedValue()
	r := newJsonArrayReader(i, cs)
	t := r.readTypeRef()
	return r.readTopLevelValue(t, nil)
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

func (r *jsonArrayReader) readPackage() *Package {
	ref := r.readRef()
	// TODO: Should load the package if not registered?
	return LookupPackage(ref)
}

func (r *jsonArrayReader) readTypeRef() TypeRef {
	kind := r.readKind()

	switch kind {
	case ListKind, SetKind, RefKind:
		elemType := r.readTypeRef()
		return MakeCompoundTypeRef("", kind, elemType)
	case MapKind:
		keyType := r.readTypeRef()
		valueType := r.readTypeRef()
		return MakeCompoundTypeRef("", kind, keyType, valueType)
	case TypeRefKind:
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
		v := r.readValue(elemType, pkg)
		ll = append(ll, v.NomsValue())
	}

	return ToNomsValueFromTypeRef(t, NewList(ll...))
}

func (r *jsonArrayReader) readSet(t TypeRef, pkg *Package) NomsValue {
	desc := t.Desc.(CompoundDesc)
	ll := []Value{}
	elemType := desc.ElemTypes[0]
	for !r.atEnd() {
		v := r.readValue(elemType, pkg)
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
		k := r.readValue(keyType, pkg)
		v := r.readValue(valueType, pkg)
		ll = append(ll, k.NomsValue(), v.NomsValue())
	}
	return ToNomsValueFromTypeRef(t, NewMap(ll...))
}

func (r *jsonArrayReader) readStruct(external, t TypeRef, pkg *Package) NomsValue {
	desc := t.Desc.(StructDesc)
	m := NewMap(
		NewString("$name"), NewString(t.Name()),
		NewString("$type"), external)

	for _, f := range desc.Fields {
		if f.Optional {
			b := r.read().(bool)
			if b {
				v := r.readValue(f.T, pkg)
				m = m.Set(NewString(f.Name), v.NomsValue())
			}
		} else {
			v := r.readValue(f.T, pkg)
			m = m.Set(NewString(f.Name), v.NomsValue())
		}
	}
	if len(desc.Union) > 0 {
		i := uint32(r.read().(float64))
		m = m.Set(NewString("$unionIndex"), UInt32(i))
		v := r.readValue(desc.Union[i].T, pkg)
		m = m.Set(NewString("$unionValue"), v.NomsValue())
	}

	return ToNomsValueFromTypeRef(external, m)
}

func (r *jsonArrayReader) readEnum(TypeRef) NomsValue {
	return valueAsNomsValue{UInt32(r.read().(float64))}
}

func (r *jsonArrayReader) readExternal(external TypeRef) NomsValue {
	pkg := LookupPackage(external.PackageRef())
	name := external.Name()
	typeRef := pkg.NamedTypes().Get(name)
	d.Chk.True(typeRef.Kind() == EnumKind || typeRef.Kind() == StructKind)
	return r.readTypeRefValue(typeRef, external, pkg)
}

func (r *jsonArrayReader) readTypeRefValue(typeRef, external TypeRef, pkg *Package) NomsValue {
	switch typeRef.Kind() {
	case StructKind:
		return r.readStruct(external, typeRef, pkg)
	case EnumKind:
		return r.readEnum(typeRef)
	default:
		panic("unreachable")
	}

	return nil
}

func (r *jsonArrayReader) readRefValue(t TypeRef) NomsValue {
	ref := r.readRef()
	v := Ref{R: ref}
	return ToNomsValueFromTypeRef(t, v)
}

func (r *jsonArrayReader) readValue(t TypeRef, pkg *Package) NomsValue {
	switch t.Kind() {
	case ListKind, MapKind, SetKind:
		a := r.readArray()
		r2 := newJsonArrayReader(a, r.cs)
		return r2.readTopLevelValue(t, pkg)
	case TypeRefKind:
		// The inner value is not tagged
		d.Chk.NotNil(t.PackageRef())
		pkgFromTypeRef := LookupPackage(t.PackageRef())

		if t.PackageRef() != (ref.Ref{}) {
			pkg = pkgFromTypeRef

		}
		d.Chk.NotNil(pkg)
		external := t
		typeRef := pkg.NamedTypes().Get(t.Name())
		return r.readTypeRefValue(typeRef, external, pkg)
	default:
		return r.readTopLevelValue(t, pkg)
	}
}

func (r *jsonArrayReader) readTopLevelValue(t TypeRef, pkg *Package) NomsValue {
	switch t.Kind() {
	case BoolKind:
		return valueAsNomsValue{Bool(r.read().(bool))}
	case UInt8Kind:
		return valueAsNomsValue{UInt8(r.read().(float64))}
	case UInt16Kind:
		return valueAsNomsValue{UInt16(r.read().(float64))}
	case UInt32Kind:
		return valueAsNomsValue{UInt32(r.read().(float64))}
	case UInt64Kind:
		return valueAsNomsValue{UInt64(r.read().(float64))}
	case Int8Kind:
		return valueAsNomsValue{Int8(r.read().(float64))}
	case Int16Kind:
		return valueAsNomsValue{Int16(r.read().(float64))}
	case Int32Kind:
		return valueAsNomsValue{Int32(r.read().(float64))}
	case Int64Kind:
		return valueAsNomsValue{Int64(r.read().(float64))}
	case Float32Kind:
		return valueAsNomsValue{Float32(r.read().(float64))}
	case Float64Kind:
		return valueAsNomsValue{Float64(r.read().(float64))}
	case StringKind:
		return valueAsNomsValue{NewString(r.readString())}
	case BlobKind:
		panic("not implemented")
	case ValueKind:
		// The value is always tagged
		t := r.readTypeRef()
		return r.readValue(t, pkg)
	case ListKind:
		return r.readList(t, pkg)
	case MapKind:
		return r.readMap(t, pkg)
	case RefKind:
		return r.readRefValue(t)
	case SetKind:
		return r.readSet(t, pkg)
	case EnumKind, StructKind:
		panic("not allowed")
	case TypeRefKind:
		return r.readExternal(t)
	}
	panic("not reachable")
}
