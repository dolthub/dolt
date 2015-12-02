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

func (w *jsonArrayWriter) writeTypeAsTag(t Type) {
	k := t.Kind()
	w.write(k)
	switch k {
	case EnumKind, StructKind:
		panic("unreachable")
	case ListKind, MapKind, RefKind, SetKind:
		for _, elemType := range t.Desc.(CompoundDesc).ElemTypes {
			w.writeTypeAsTag(elemType)
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
	case MetaSequenceKind:
		concreteType := t.Desc.(CompoundDesc).ElemTypes[0]
		w.writeTypeAsTag(concreteType)
	}
}

func getNormalizedType(v Value) Type {
	tr := v.Type()
	v = internalValueFromType(v, tr)
	if _, ok := v.(metaSequence); ok {
		return MakeCompoundType(MetaSequenceKind, tr)
	}

	return tr
}

func (w *jsonArrayWriter) writeTopLevelValue(v Value) {
	tr := getNormalizedType(v)
	w.writeTypeAsTag(tr)
	w.writeValue(v, tr, nil)
}

func (w *jsonArrayWriter) writeValue(v Value, tr Type, pkg *Package) {
	switch tr.Kind() {
	case BlobKind:
		w.writeBlob(v.(Blob))
	case BoolKind, Float32Kind, Float64Kind, Int16Kind, Int32Kind, Int64Kind, Int8Kind, Uint16Kind, Uint32Kind, Uint64Kind, Uint8Kind:
		w.write(v.(primitive).ToPrimitive())
	case ListKind:
		w2 := newJsonArrayWriter(w.cs)
		elemType := tr.Desc.(CompoundDesc).ElemTypes[0]
		tr = fixupType(tr, pkg)
		l := internalValueFromType(v, tr)
		l.(List).IterAll(func(v Value, i uint64) {
			w2.writeValue(v, elemType, pkg)
		})
		w.write(w2.toArray())
	case MapKind:
		w2 := newJsonArrayWriter(w.cs)
		elemTypes := tr.Desc.(CompoundDesc).ElemTypes
		tr = fixupType(tr, pkg)
		m := internalValueFromType(v, tr)
		m.(Map).IterAll(func(k, v Value) {
			w2.writeValue(k, elemTypes[0], pkg)
			w2.writeValue(v, elemTypes[1], pkg)
		})
		w.write(w2.toArray())
	case PackageKind:
		ptr := MakePrimitiveType(TypeKind)
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
		tr = fixupType(tr, pkg)
		s := internalValueFromType(v, tr)
		s.(Set).IterAll(func(v Value) {
			w2.writeValue(v, elemType, pkg)
		})
		w.write(w2.toArray())
	case StringKind:
		w.write(v.(String).String())
	case TypeKind:
		w.writeTypeKindValue(v, tr, pkg)
	case UnresolvedKind:
		if tr.HasPackageRef() {
			pkg = LookupPackage(tr.PackageRef())
		}
		w.writeUnresolvedKindValue(v, tr, pkg)
	case ValueKind:
		w.writeTypeAsTag(v.Type())
		w.writeValue(v, v.Type(), pkg)
	case MetaSequenceKind:
		w2 := newJsonArrayWriter(w.cs)
		indexType := indexTypeForMetaSequence(tr)
		tr = fixupType(tr, pkg)
		concreteType := tr.Desc.(CompoundDesc).ElemTypes[0]
		ms := internalValueFromType(v, concreteType) // Dirty: must retrieve internal value by denormalized type
		for _, tuple := range ms.(metaSequence).data() {
			w2.writeRef(tuple.ref)
			w2.writeValue(tuple.value, indexType, pkg)
		}
		w.write(w2.toArray())
	default:
		d.Chk.Fail("Unknown NomsKind")
	}
}

func (w *jsonArrayWriter) writeTypeAsValue(v Type) {
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
	case ListKind, MapKind, RefKind, SetKind, MetaSequenceKind:
		w2 := newJsonArrayWriter(w.cs)
		for _, elemType := range v.Desc.(CompoundDesc).ElemTypes {
			w2.writeTypeAsValue(elemType)
		}
		w.write(w2.toArray())
	case StructKind:
		w.write(v.Name())
		fieldWriter := newJsonArrayWriter(w.cs)
		for _, field := range v.Desc.(StructDesc).Fields {
			fieldWriter.write(field.Name)
			fieldWriter.writeTypeAsValue(field.T)
			fieldWriter.write(field.Optional)
		}
		w.write(fieldWriter.toArray())
		choiceWriter := newJsonArrayWriter(w.cs)
		for _, choice := range v.Desc.(StructDesc).Union {
			choiceWriter.write(choice.Name)
			choiceWriter.writeTypeAsValue(choice.T)
			choiceWriter.write(choice.Optional)
		}
		w.write(choiceWriter.toArray())
	case UnresolvedKind:
		pkgRef := v.PackageRef()
		w.writeRef(pkgRef)
		// Don't use Ordinal() here since we might need to serialize a Type that hasn't gotten a valid ordinal yet.
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
		d.Chk.True(IsPrimitiveKind(k), "Kind: %v Desc: %s\n", v.Kind(), v.Describe())
	}
}

// writeTypeKindValue writes either a struct, enum or a Type value
func (w *jsonArrayWriter) writeTypeKindValue(v Value, tr Type, pkg *Package) {
	d.Chk.IsType(Type{}, v)
	w.writeTypeAsValue(v.(Type))
}

// writeUnresolvedKindValue writes either a struct or an enum
func (w *jsonArrayWriter) writeUnresolvedKindValue(v Value, tr Type, pkg *Package) {
	d.Chk.NotNil(pkg)
	typeDef := pkg.types[tr.Ordinal()]
	switch typeDef.Kind() {
	default:
		d.Chk.Fail("An Unresolved Type can only reference a StructKind or Enum Kind.", "Actually referenced: %+v", typeDef)
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

func (w *jsonArrayWriter) writeStruct(v Value, typ, typeDef Type, pkg *Package) {
	typ = fixupType(typ, pkg)

	i := 0
	values := structReaderForType(v, typ, typeDef)
	desc := typeDef.Desc.(StructDesc)

	for _, f := range desc.Fields {
		if f.Optional {
			ok := bool(values[i].(Bool))
			i++
			w.write(ok)
			if ok {
				w.writeValue(values[i], f.T, pkg)
				i++
			}
		} else {
			w.writeValue(values[i], f.T, pkg)
			i++
		}
	}
	if len(desc.Union) > 0 {
		unionIndex := uint32(values[i].(Uint32))
		i++
		w.write(unionIndex)
		w.writeValue(values[i], desc.Union[unionIndex].T, pkg)
		i++
	}
}

func (w *jsonArrayWriter) writeEnum(v Value, t Type, pkg *Package) {
	t = fixupType(t, pkg)
	i := enumPrimitiveValueFromType(v, t)
	w.write(i)
}
