package types

import (
	"bytes"
	"encoding/base64"
	"io"
	"strconv"
	"strings"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

func encNomsValue(v Value, vw ValueWriter) []interface{} {
	w := newJSONArrayWriter(vw)
	w.writeTopLevelValue(v)
	return w.toArray()
}

type jsonArrayWriter struct {
	a  []interface{}
	vw ValueWriter
}

func newJSONArrayWriter(vw ValueWriter) *jsonArrayWriter {
	return &jsonArrayWriter{vw: vw, a: []interface{}{}}
}

func (w *jsonArrayWriter) write(v interface{}) {
	w.a = append(w.a, v)
}

func (w *jsonArrayWriter) writeBool(b bool) {
	w.write(b)
}

func (w *jsonArrayWriter) writeFloat(v float64) {
	// Make sure we output identical strings in go as in js
	if v < 1e20 {
		w.write(strconv.FormatFloat(v, 'f', -1, 64))
	} else {
		s := strconv.FormatFloat(v, 'e', -1, 64)
		s = strings.Replace(s, "e+0", "e+", 1)
		w.write(s)

	}
}

func (w *jsonArrayWriter) writeInt(v int64) {
	w.write(strconv.FormatInt(v, 10))
}

func (w *jsonArrayWriter) writeUint(v uint64) {
	w.write(strconv.FormatUint(v, 10))
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
	case StructKind:
		panic("unreachable")
	case ListKind, MapKind, RefKind, SetKind:
		for _, elemType := range t.Desc.(CompoundDesc).ElemTypes {
			w.writeTypeAsTag(elemType)
		}
	case UnresolvedKind:
		pkgRef := t.PackageRef()
		d.Chk.NotEqual(ref.Ref{}, pkgRef)
		w.writeRef(pkgRef)
		w.writeInt(int64(t.Ordinal()))

		pkg := LookupPackage(pkgRef)
		if pkg != nil && w.vw != nil {
			w.vw.WriteValue(*pkg)
		}
	}
}

func (w *jsonArrayWriter) writeTopLevelValue(v Value) {
	tr := v.Type()
	w.writeTypeAsTag(tr)
	w.writeValue(v, tr, nil)
}

func (w *jsonArrayWriter) maybeWriteMetaSequence(v Value, tr Type, pkg *Package) bool {
	ms, ok := v.(metaSequence)
	if !ok {
		w.write(false) // not a meta sequence
		return false
	}

	w.write(true) // a meta sequence
	w2 := newJSONArrayWriter(w.vw)
	indexType := indexTypeForMetaSequence(tr)
	for _, tuple := range ms.(metaSequence).data() {
		if tuple.child != nil && w.vw != nil {
			// Write unwritten chunked sequences. Chunks are lazily written so that intermediate chunked structures like NewList().Append(x).Append(y) don't cause unnecessary churn.
			w.vw.WriteValue(tuple.child)
		}
		w2.writeRef(tuple.ChildRef().TargetRef())
		w2.writeValue(tuple.value, indexType, pkg)
		w2.writeUint(tuple.numLeaves)
	}
	w.write(w2.toArray())
	return true
}

func (w *jsonArrayWriter) writeValue(v Value, tr Type, pkg *Package) {
	switch tr.Kind() {
	case BlobKind:
		if w.maybeWriteMetaSequence(v, tr, pkg) {
			return
		}
		w.writeBlob(v.(Blob))
	case BoolKind:
		w.writeBool(bool(v.(Bool)))
	case Float32Kind:
		w.writeFloat(float64(v.(Float32)))
	case Float64Kind:
		w.writeFloat(float64(v.(Float64)))
	case Int16Kind:
		w.writeInt(int64(v.(Int16)))
	case Int32Kind:
		w.writeInt(int64(v.(Int32)))
	case Int64Kind:
		w.writeInt(int64(v.(Int64)))
	case Int8Kind:
		w.writeInt(int64(v.(Int8)))
	case Uint16Kind:
		w.writeUint(uint64(v.(Uint16)))
	case Uint32Kind:
		w.writeUint(uint64(v.(Uint32)))
	case Uint64Kind:
		w.writeUint(uint64(v.(Uint64)))
	case Uint8Kind:
		w.writeUint(uint64(v.(Uint8)))
	case ListKind:
		v = internalValueFromType(v, tr)
		if w.maybeWriteMetaSequence(v, tr, pkg) {
			return
		}

		w2 := newJSONArrayWriter(w.vw)
		elemType := tr.Desc.(CompoundDesc).ElemTypes[0]
		v.(List).IterAll(func(v Value, i uint64) {
			w2.writeValue(v, elemType, pkg)
		})
		w.write(w2.toArray())
	case MapKind:
		v = internalValueFromType(v, tr)
		if w.maybeWriteMetaSequence(v, tr, pkg) {
			return
		}

		w2 := newJSONArrayWriter(w.vw)
		elemTypes := tr.Desc.(CompoundDesc).ElemTypes
		v.(Map).IterAll(func(k, v Value) {
			w2.writeValue(k, elemTypes[0], pkg)
			w2.writeValue(v, elemTypes[1], pkg)
		})
		w.write(w2.toArray())
	case PackageKind:
		ptr := MakePrimitiveType(TypeKind)
		w2 := newJSONArrayWriter(w.vw)
		p := v.(Package)
		for _, t := range p.types {
			w2.writeValue(t, ptr, &p)
		}
		w.write(w2.toArray())
		w3 := newJSONArrayWriter(w.vw)
		for _, r := range v.(Package).dependencies {
			w3.writeRef(r)
		}
		w.write(w3.toArray())
	case RefKind:
		w.writeRef(v.(RefBase).TargetRef())
	case SetKind:
		v = internalValueFromType(v, tr)
		if w.maybeWriteMetaSequence(v, tr, pkg) {
			return
		}

		w2 := newJSONArrayWriter(w.vw)
		elemType := tr.Desc.(CompoundDesc).ElemTypes[0]
		v.(Set).IterAll(func(v Value) {
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
	default:
		d.Chk.Fail("Unknown NomsKind")
	}
}

func (w *jsonArrayWriter) writeTypeAsValue(t Type, pkg *Package) {
	k := t.Kind()
	w.write(k)
	switch k {
	case ListKind, MapKind, RefKind, SetKind:
		w2 := newJSONArrayWriter(w.vw)
		for _, elemType := range t.Desc.(CompoundDesc).ElemTypes {
			w2.writeTypeAsValue(elemType, pkg)
		}
		w.write(w2.toArray())
	case StructKind:
		w.write(t.Name())
		fieldWriter := newJSONArrayWriter(w.vw)
		for _, field := range t.Desc.(StructDesc).Fields {
			fieldWriter.write(field.Name)
			fieldWriter.writeTypeAsValue(field.T, pkg)
			fieldWriter.write(field.Optional)
		}
		w.write(fieldWriter.toArray())
		choiceWriter := newJSONArrayWriter(w.vw)
		for _, choice := range t.Desc.(StructDesc).Union {
			choiceWriter.write(choice.Name)
			choiceWriter.writeTypeAsValue(choice.T, pkg)
			choiceWriter.write(choice.Optional)
		}
		w.write(choiceWriter.toArray())
	case UnresolvedKind:
		pkgRef := t.PackageRef()
		isCurrentPackage := pkg != nil && pkg.ref != nil && *pkg.ref == pkgRef
		if isCurrentPackage {
			w.writeRef(ref.Ref{})
		} else {
			w.writeRef(pkgRef)
		}
		// Don't use Ordinal() here since we might need to serialize a Type that hasn't gotten a valid ordinal yet.
		ordinal := t.Desc.(UnresolvedDesc).ordinal
		w.writeInt(int64(ordinal))
		if ordinal == -1 {
			w.write(t.Namespace())
			w.write(t.Name())
		}

		if !isCurrentPackage {
			pkg := LookupPackage(pkgRef)
			if pkg != nil && w.vw != nil {
				w.vw.WriteValue(*pkg)
			}
		}

	default:
		d.Chk.True(IsPrimitiveKind(k), "Kind: %v Desc: %s\n", t.Kind(), t.Describe())
	}
}

// writeTypeKindValue writes either a struct or a Type value
func (w *jsonArrayWriter) writeTypeKindValue(v Value, tr Type, pkg *Package) {
	d.Chk.IsType(Type{}, v)
	w.writeTypeAsValue(v.(Type), pkg)
}

// writeUnresolvedKindValue writes a struct.
func (w *jsonArrayWriter) writeUnresolvedKindValue(v Value, tr Type, pkg *Package) {
	d.Chk.NotNil(pkg)
	typeDef := pkg.types[tr.Ordinal()]
	switch typeDef.Kind() {
	default:
		d.Chk.Fail("An Unresolved Type can only reference a StructKind.", "Actually referenced: %+v", typeDef)
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
		unionIndex := uint64(values[i].(Uint32))
		i++
		w.writeUint(unionIndex)
		w.writeValue(values[i], desc.Union[unionIndex].T, pkg)
		i++
	}
}
