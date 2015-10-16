package types

import (
	"bytes"
	"encoding/base64"
	"io"

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
	w.writeTopLevelValue(v)
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

func (w *jsonArrayWriter) writeTypeRefAsTag(t TypeRef) {
	k := t.Kind()
	w.write(k)
	switch k {
	case EnumKind, StructKind:
		panic("unreachable")
	case ListKind, MapKind, RefKind, SetKind:
		for _, elemType := range t.Desc.(CompoundDesc).ElemTypes {
			w.writeTypeRefAsTag(elemType)
		}
	case TypeRefKind:
		if t.IsUnresolved() {
			pkgRef := t.PackageRef()
			d.Chk.NotEqual(ref.Ref{}, pkgRef)
			w.writeRef(pkgRef)
			w.write(t.Ordinal())
		}
	}
}

func (w *jsonArrayWriter) writeTopLevelValue(v NomsValue) {
	tr := v.TypeRef()
	w.writeTypeRefAsTag(tr)
	w.writeValue(v.NomsValue(), tr, nil)
}

func (w *jsonArrayWriter) writeValue(v Value, tr TypeRef, pkg *Package) {
	switch tr.Kind() {
	case BlobKind:
		w.writeBlob(v.(Blob))
	case BoolKind, Float32Kind, Float64Kind, Int16Kind, Int32Kind, Int64Kind, Int8Kind, UInt16Kind, UInt32Kind, UInt64Kind, UInt8Kind:
		w.write(v.(primitive).ToPrimitive())
	case ListKind:
		w2 := newJsonArrayWriter()
		elemType := tr.Desc.(CompoundDesc).ElemTypes[0]
		v.(List).IterAll(func(v Value, i uint64) {
			w2.writeValue(v, elemType, pkg)
		})
		w.write(w2.toArray())
	case MapKind:
		w2 := newJsonArrayWriter()
		elemTypes := tr.Desc.(CompoundDesc).ElemTypes
		v.(Map).IterAll(func(k, v Value) {
			w2.writeValue(k, elemTypes[0], pkg)
			w2.writeValue(v, elemTypes[1], pkg)
		})
		w.write(w2.toArray())
	case RefKind:
		w.writeRef(v.Ref())
	case SetKind:
		w2 := newJsonArrayWriter()
		elemType := tr.Desc.(CompoundDesc).ElemTypes[0]
		v.(Set).IterAll(func(v Value) {
			w2.writeValue(v, elemType, pkg)
		})
		w.write(w2.toArray())
	case StringKind:
		w.write(v.(String).String())
	case TypeRefKind:
		if tr.HasPackageRef() {
			pkg = LookupPackage(tr.PackageRef())
		}
		w.writeTypeRefKindValue(v, tr, pkg)
	case ValueKind:
		w.writeTypeRefAsTag(v.TypeRef())
		w.writeValue(v, v.TypeRef(), pkg)
	default:
		d.Chk.Fail("Unknown NomsKind")
	}
}

func (w *jsonArrayWriter) writeTypeRefAsValue(v TypeRef) {
	k := v.Kind()
	w.write(k)
	switch k {
	case EnumKind:
		w.write(v.Name())
		w2 := newJsonArrayWriter()
		for _, id := range v.Desc.(EnumDesc).IDs {
			w2.write(id)
		}
		w.write(w2.toArray())
	case ListKind, MapKind, RefKind, SetKind:
		w2 := newJsonArrayWriter()
		for _, elemType := range v.Desc.(CompoundDesc).ElemTypes {
			w2.writeTypeRefAsValue(elemType)
		}
		w.write(w2.toArray())
	case StructKind:
		w.write(v.Name())
		fieldWriter := newJsonArrayWriter()
		for _, field := range v.Desc.(StructDesc).Fields {
			fieldWriter.write(field.Name)
			fieldWriter.writeTypeRefAsValue(field.T)
			fieldWriter.write(field.Optional)
		}
		w.write(fieldWriter.toArray())
		choiceWriter := newJsonArrayWriter()
		for _, choice := range v.Desc.(StructDesc).Union {
			choiceWriter.write(choice.Name)
			choiceWriter.writeTypeRefAsValue(choice.T)
			choiceWriter.write(choice.Optional)
		}
		w.write(choiceWriter.toArray())
	case TypeRefKind:
		if v.IsUnresolved() {
			w.writeRef(v.PackageRef())
			w.write(v.Ordinal())
		}
	default:
		d.Chk.True(IsPrimitiveKind(k), v.Describe())

	}
}

// writeTypeRefKindValue writes either a struct, enum or a TypeRef value
func (w *jsonArrayWriter) writeTypeRefKindValue(v Value, tr TypeRef, pkg *Package) {
	if t, ok := v.(TypeRef); ok {
		w.writeTypeRefAsValue(t)
	} else { // Enum or Struct
		pkgRef := tr.PackageRef()
		if pkgRef != (ref.Ref{}) {
			pkg = LookupPackage(pkgRef)
		}

		typeDef := pkg.Types().Get(uint64(tr.Ordinal()))

		k := typeDef.Kind()
		if k == EnumKind {
			w.write(uint32(v.(UInt32)))
		} else {
			d.Chk.Equal(StructKind, k)
			w.writeStruct(v.(Map), typeDef, pkg)
		}
	}
}

func (w *jsonArrayWriter) writeBlob(b Blob) {
	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	n, err := io.Copy(encoder, b.Reader())
	encoder.Close()
	d.Exp.Equal(uint64(n), b.Len())
	d.Exp.NoError(err)
	w.write(buf.String())
}

func (w *jsonArrayWriter) writeStruct(m Map, t TypeRef, pkg *Package) {
	desc := t.Desc.(StructDesc)
	for _, f := range desc.Fields {
		v, ok := m.MaybeGet(NewString(f.Name))
		if f.Optional {
			if ok {
				w.write(true)
				w.writeValue(v, f.T, pkg)
			} else {
				w.write(false)
			}
		} else {
			d.Chk.True(ok)
			w.writeValue(v, f.T, pkg)
		}
	}
	if len(desc.Union) > 0 {
		i := uint32(m.Get(NewString("$unionIndex")).(UInt32))
		v := m.Get(NewString("$unionValue"))
		w.write(i)
		w.writeValue(v, desc.Union[i].T, pkg)
	}
}
