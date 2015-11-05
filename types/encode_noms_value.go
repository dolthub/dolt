package types

import (
	"bytes"
	"encoding/base64"
	"io"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

func encNomsValue(v Value, cs chunks.ChunkSink) []interface{} {
	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	return w.toArray()
}

type jsonArrayWriter struct {
	a  []interface{}
	cs chunks.ChunkSink
}

func newJsonArrayWriter(cs chunks.ChunkSink) *jsonArrayWriter {
	return &jsonArrayWriter{cs: cs, a: []interface{}{}}
}

func (w *jsonArrayWriter) write(v interface{}) {
	w.a = append(w.a, v)
}

func (w *jsonArrayWriter) toArray() []interface{} {
	return w.a
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
	case UnresolvedKind:
		pkgRef := t.PackageRef()
		d.Chk.NotEqual(ref.Ref{}, pkgRef)
		w.writeRef(pkgRef)
		w.write(t.Ordinal())

		pkg := LookupPackage(pkgRef)
		if pkg != nil {
			writeChildValueInternal(*pkg, w.cs)
		}
	}
}

func (w *jsonArrayWriter) writeTopLevelValue(v Value) {
	tr := v.TypeRef()
	w.writeTypeRefAsTag(tr)
	w.writeValue(v, tr, nil)
}

func (w *jsonArrayWriter) writeValue(v Value, tr TypeRef, pkg *Package) {
	switch tr.Kind() {
	case BlobKind:
		w.writeBlob(v.(Blob))
	case BoolKind, Float32Kind, Float64Kind, Int16Kind, Int32Kind, Int64Kind, Int8Kind, UInt16Kind, UInt32Kind, UInt64Kind, UInt8Kind:
		w.write(v.(primitive).ToPrimitive())
	case ListKind:
		w2 := newJsonArrayWriter(w.cs)
		elemType := tr.Desc.(CompoundDesc).ElemTypes[0]
		tr = fixupTypeRef(tr, pkg)
		l := internalValueFromTypeRef(v, tr)
		l.(List).IterAll(func(v Value, i uint64) {
			w2.writeValue(v, elemType, pkg)
		})
		w.write(w2.toArray())
	case MapKind:
		w2 := newJsonArrayWriter(w.cs)
		elemTypes := tr.Desc.(CompoundDesc).ElemTypes
		tr = fixupTypeRef(tr, pkg)
		m := internalValueFromTypeRef(v, tr)
		m.(Map).IterAll(func(k, v Value) {
			w2.writeValue(k, elemTypes[0], pkg)
			w2.writeValue(v, elemTypes[1], pkg)
		})
		w.write(w2.toArray())
	case PackageKind:
		ptr := MakePrimitiveTypeRef(TypeRefKind)
		w2 := newJsonArrayWriter(w.cs)
		for _, v := range v.(Package).types {
			w2.writeValue(v, ptr, pkg)
		}
		w.write(w2.toArray())
		w3 := newJsonArrayWriter(w.cs)
		for _, r := range v.(Package).dependencies {
			w3.writeRef(r)
		}
		w.write(w3.toArray())
	case RefKind:
		w.writeRef(v.(RefBase).TargetRef())
	case SetKind:
		w2 := newJsonArrayWriter(w.cs)
		elemType := tr.Desc.(CompoundDesc).ElemTypes[0]
		tr = fixupTypeRef(tr, pkg)
		s := internalValueFromTypeRef(v, tr)
		s.(Set).IterAll(func(v Value) {
			w2.writeValue(v, elemType, pkg)
		})
		w.write(w2.toArray())
	case StringKind:
		w.write(v.(String).String())
	case TypeRefKind:
		w.writeTypeRefKindValue(v, tr, pkg)
	case UnresolvedKind:
		if tr.HasPackageRef() {
			pkg = LookupPackage(tr.PackageRef())
		}
		w.writeUnresolvedKindValue(v, tr, pkg)
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
		w2 := newJsonArrayWriter(w.cs)
		for _, id := range v.Desc.(EnumDesc).IDs {
			w2.write(id)
		}
		w.write(w2.toArray())
	case ListKind, MapKind, RefKind, SetKind:
		w2 := newJsonArrayWriter(w.cs)
		for _, elemType := range v.Desc.(CompoundDesc).ElemTypes {
			w2.writeTypeRefAsValue(elemType)
		}
		w.write(w2.toArray())
	case StructKind:
		w.write(v.Name())
		fieldWriter := newJsonArrayWriter(w.cs)
		for _, field := range v.Desc.(StructDesc).Fields {
			fieldWriter.write(field.Name)
			fieldWriter.writeTypeRefAsValue(field.T)
			fieldWriter.write(field.Optional)
		}
		w.write(fieldWriter.toArray())
		choiceWriter := newJsonArrayWriter(w.cs)
		for _, choice := range v.Desc.(StructDesc).Union {
			choiceWriter.write(choice.Name)
			choiceWriter.writeTypeRefAsValue(choice.T)
			choiceWriter.write(choice.Optional)
		}
		w.write(choiceWriter.toArray())
	case UnresolvedKind:
		pkgRef := v.PackageRef()
		w.writeRef(pkgRef)
		// Don't use Ordinal() here since we might need to serialize a TypeRef that hasn't gotten a valid ordinal yet.
		ordinal := v.Desc.(UnresolvedDesc).ordinal
		w.write(ordinal)
		if ordinal == -1 {
			w.write(v.Namespace())
			w.write(v.Name())
		}

		pkg := LookupPackage(pkgRef)
		if pkg != nil {
			writeChildValueInternal(*pkg, w.cs)
		}

	default:
		d.Chk.True(IsPrimitiveKind(k), v.Describe())
	}
}

// writeTypeRefKindValue writes either a struct, enum or a TypeRef value
func (w *jsonArrayWriter) writeTypeRefKindValue(v Value, tr TypeRef, pkg *Package) {
	d.Chk.IsType(TypeRef{}, v)
	w.writeTypeRefAsValue(v.(TypeRef))
}

// writeUnresolvedKindValue writes either a struct or an enum
func (w *jsonArrayWriter) writeUnresolvedKindValue(v Value, tr TypeRef, pkg *Package) {
	d.Chk.NotNil(pkg)
	typeDef := pkg.types[tr.Ordinal()]
	switch typeDef.Kind() {
	default:
		d.Chk.Fail("An Unresolved TypeRef can only reference a StructKind or Enum Kind.", "Actually referenced: %+v", typeDef)
	case EnumKind:
		w.writeEnum(v, tr, pkg)
	case StructKind:
		w.writeStruct(v, tr, typeDef, pkg)
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

func (w *jsonArrayWriter) writeStruct(v Value, typeRef, typeDef TypeRef, pkg *Package) {
	typeRef = fixupTypeRef(typeRef, pkg)

	c := structReaderForTypeRef(v, typeRef, typeDef)
	desc := typeDef.Desc.(StructDesc)

	for _, f := range desc.Fields {
		if f.Optional {
			ok := bool((<-c).(Bool))
			w.write(ok)
			if ok {
				w.writeValue(<-c, f.T, pkg)
			}
		} else {
			w.writeValue(<-c, f.T, pkg)
		}
	}
	if len(desc.Union) > 0 {
		unionIndex := uint32((<-c).(UInt32))
		w.write(unionIndex)
		w.writeValue(<-c, desc.Union[unionIndex].T, pkg)
	}
}

func (w *jsonArrayWriter) writeEnum(v Value, t TypeRef, pkg *Package) {
	t = fixupTypeRef(t, pkg)
	i := enumPrimitiveValueFromTypeRef(v, t)
	w.write(i)
}
