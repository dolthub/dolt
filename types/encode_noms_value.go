package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type typedValueWrapper interface {
	TypedValue() []interface{}
}

// typedValue implements enc.typedValue which is used to tag the value for now so that we can trigger a different encoding strategy.
type typedValue struct {
	v []interface{}
}

func (tv typedValue) TypedValue() []interface{} {
	return tv.v
}

func encNomsValue(v NomsValue, cs chunks.ChunkSink) typedValue {
	w := newJsonArrayWriter()
	t := v.TypeRef()
	w.writeTypeRef(t)
	w.writeTopLevelValue(t, v.NomsValue(), nil)
	return typedValue{w.toArray()}
}

type jsonArrayWriter []interface{}

func newJsonArrayWriter() *jsonArrayWriter {
	return &jsonArrayWriter{}
}

func (w *jsonArrayWriter) write(v interface{}) {
	*w = append(*w, v)
}

func (w *jsonArrayWriter) toArray() []interface{} {
	return *w
}

func (w *jsonArrayWriter) writeRef(r ref.Ref) {
	w.write(r.String())
}

func (w *jsonArrayWriter) writeTypeRef(t TypeRef) {
	k := t.Kind()
	w.write(k)
	switch k {
	case EnumKind, StructKind, TypeRefKind:
		r := t.PackageRef()
		d.Chk.NotEqual(ref.Ref{}, r)
		w.writeRef(r)
		// TODO: Should be ordinal instead of name.
		w.write(t.Name())
	case ListKind, MapKind, RefKind, SetKind:
		for _, elemType := range t.Desc.(CompoundDesc).ElemTypes {
			w.writeTypeRef(elemType)
		}
	}

}

func (w *jsonArrayWriter) writeValue(t TypeRef, v Value, pkg *Package) {
	switch t.Kind() {
	case ListKind, MapKind, SetKind:
		w2 := newJsonArrayWriter()
		w2.writeTopLevelValue(t, v, pkg)
		w.write(w2.toArray())
	default:
		w.writeTopLevelValue(t, v, pkg)
	}
}

func (w *jsonArrayWriter) writeTopLevelValue(t TypeRef, v Value, pkg *Package) {
	switch t.Kind() {
	case BoolKind, Float32Kind, Float64Kind, Int16Kind, Int32Kind, Int64Kind, Int8Kind, UInt16Kind, UInt32Kind, UInt64Kind, UInt8Kind:
		w.write(v.(primitive).ToPrimitive())
	case StringKind:
		w.write(v.(String).String())
	case BlobKind:
		panic("not yet implemented")
	case ValueKind:
		// The value is always tagged
		runtimeType := v.TypeRef()
		w.writeTypeRef(runtimeType)
		w.writeValue(runtimeType, v, pkg)
	case ListKind:
		w.writeList(t, v.(List), pkg)
	case MapKind:
		w.writeMap(t, v.(Map), pkg)
	case RefKind:
		w.writeRef(v.Ref())
	case SetKind:
		w.writeSet(t, v.(Set), pkg)
	case EnumKind, StructKind:
		panic("Enums and Structs use TypeRefKind at top level")
	case TypeRefKind:
		w.writeExternal(t, v, pkg)
	}
}

func (w *jsonArrayWriter) writeList(t TypeRef, l List, pkg *Package) {
	desc := t.Desc.(CompoundDesc)
	elemType := desc.ElemTypes[0]
	l.IterAll(func(v Value, i uint64) {
		w.writeValue(elemType, v, pkg)
	})
}

func (w *jsonArrayWriter) writeSet(t TypeRef, s Set, pkg *Package) {
	desc := t.Desc.(CompoundDesc)
	elemType := desc.ElemTypes[0]
	s.IterAll(func(v Value) {
		w.writeValue(elemType, v, pkg)
	})
}

func (w *jsonArrayWriter) writeMap(t TypeRef, m Map, pkg *Package) {
	desc := t.Desc.(CompoundDesc)
	keyType := desc.ElemTypes[0]
	valueType := desc.ElemTypes[1]
	m.IterAll(func(k, v Value) {
		w.writeValue(keyType, k, pkg)
		w.writeValue(valueType, v, pkg)
	})
}

func (w *jsonArrayWriter) writeExternal(t TypeRef, v Value, pkg *Package) {
	d.Chk.Equal(t.Kind(), TypeRefKind)
	d.Chk.True(t.IsUnresolved())

	if t.PackageRef() != (ref.Ref{}) {
		pkg = LookupPackage(t.PackageRef())
	}
	t = pkg.NamedTypes().Get(t.Name())

	switch t.Kind() {
	case StructKind:
		w.writeStruct(t, v.(Map), pkg)
	case EnumKind:
		w.writeEnum(t, v.(UInt32))
	default:
		panic("unreachable")
	}
}

func (w *jsonArrayWriter) writeStruct(t TypeRef, m Map, pkg *Package) {
	d.Chk.False(t.IsUnresolved())
	desc := t.Desc.(StructDesc)
	for _, f := range desc.Fields {
		v, ok := m.MaybeGet(NewString(f.Name))
		if f.Optional {
			if ok {
				w.write(true)
				w.writeValue(f.T, v, pkg)
			} else {
				w.write(false)
			}
		} else {
			d.Chk.True(ok)
			w.writeValue(f.T, v, pkg)
		}
	}
	if len(desc.Union) > 0 {
		i := uint32(m.Get(NewString("$unionIndex")).(UInt32))
		v := m.Get(NewString("$unionValue"))
		w.write(i)
		w.writeValue(desc.Union[i].T, v, pkg)
	}
}

func (w *jsonArrayWriter) writeEnum(t TypeRef, v UInt32) {
	w.write(uint32(v))
}
