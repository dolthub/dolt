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

func (w *jsonArrayWriter) writeUint8(v uint8) {
	w.write(v)
}

func (w *jsonArrayWriter) toArray() []interface{} {
	return w.a
}

func (w *jsonArrayWriter) writeRef(r ref.Ref) {
	w.write(r.String())
}

func (w *jsonArrayWriter) writeTypeAsTag(t *Type, backRefs []*Type) {
	k := t.Kind()
	switch k {
	case StructKind:
		w.writeStructType(t, backRefs)
	case ListKind, MapKind, RefKind, SetKind:
		w.write(k)
		for _, elemType := range t.Desc.(CompoundDesc).ElemTypes {
			w.writeTypeAsTag(elemType, backRefs)
		}
	default:
		w.write(k)
	}
}

func (w *jsonArrayWriter) writeTopLevelValue(v Value) {
	tr := v.Type()
	w.writeTypeAsTag(tr, nil)
	w.writeValue(v, tr)
}

func (w *jsonArrayWriter) maybeWriteMetaSequence(v Value, tr *Type) bool {
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
		w2.writeValue(tuple.value, indexType)
		w2.writeUint(tuple.numLeaves)
	}
	w.write(w2.toArray())
	return true
}

func (w *jsonArrayWriter) writeValue(v Value, tr *Type) {
	switch tr.Kind() {
	case BlobKind:
		if w.maybeWriteMetaSequence(v, tr) {
			return
		}
		w.writeBlob(v.(Blob))
	case BoolKind:
		w.writeBool(bool(v.(Bool)))
	case NumberKind:
		w.writeFloat(float64(v.(Number)))
	case ListKind:
		if w.maybeWriteMetaSequence(v, tr) {
			return
		}

		w2 := newJSONArrayWriter(w.vw)
		elemType := tr.Desc.(CompoundDesc).ElemTypes[0]
		v.(List).IterAll(func(v Value, i uint64) {
			w2.writeValue(v, elemType)
		})
		w.write(w2.toArray())
	case MapKind:
		if w.maybeWriteMetaSequence(v, tr) {
			return
		}

		w2 := newJSONArrayWriter(w.vw)
		elemTypes := tr.Desc.(CompoundDesc).ElemTypes
		v.(Map).IterAll(func(k, v Value) {
			w2.writeValue(k, elemTypes[0])
			w2.writeValue(v, elemTypes[1])
		})
		w.write(w2.toArray())
	case RefKind:
		w.writeRef(v.(Ref).TargetRef())
	case SetKind:
		if w.maybeWriteMetaSequence(v, tr) {
			return
		}

		w2 := newJSONArrayWriter(w.vw)
		elemType := tr.Desc.(CompoundDesc).ElemTypes[0]
		v.(Set).IterAll(func(v Value) {
			w2.writeValue(v, elemType)
		})
		w.write(w2.toArray())
	case StringKind:
		w.write(v.(String).String())
	case TypeKind:
		vt := v.(*Type)
		w.writeTypeAsValue(vt, nil)
	case StructKind:
		w.writeStruct(v, tr)
	case ValueKind:
		vt := v.Type()
		w.writeTypeAsTag(vt, nil)
		w.writeValue(v, v.Type())
	case BackRefKind:
		w.writeUint8(uint8(v.(*Type).Desc.(BackRefDesc)))
	default:
		d.Chk.Fail("Unknown NomsKind")
	}
}

func (w *jsonArrayWriter) writeTypeAsValue(t *Type, backRefs []*Type) {
	k := t.Kind()
	switch k {
	case ListKind, MapKind, RefKind, SetKind:
		w.write(k)
		w2 := newJSONArrayWriter(w.vw)
		for _, elemType := range t.Desc.(CompoundDesc).ElemTypes {
			w2.writeTypeAsValue(elemType, backRefs)
		}
		w.write(w2.toArray())
	case StructKind:
		w.writeStructType(t, backRefs)
	default:
		w.write(k)
		d.Chk.True(IsPrimitiveKind(k), "Kind: %v Desc: %s\n", t.Kind(), t.Describe())
	}
}

func indexOfType(t *Type, ts []*Type) int {
	for i, tt := range ts {
		if t == tt {
			return i
		}
	}
	return -1
}

func (w *jsonArrayWriter) writeBackRef(i int) {
	w.write(BackRefKind)
	w.write(uint8(i))
}

func (w *jsonArrayWriter) writeStructType(t *Type, backRefs []*Type) {
	// The runtime representaion of struct types can contain cycles. These cycles are broken when encoding and decoding using special "back ref" placeholders.
	i := indexOfType(t, backRefs)
	if i != -1 {
		w.writeBackRef(len(backRefs) - i - 1)
		return
	}
	backRefs = append(backRefs, t)

	w.write(StructKind)
	w.write(t.Name())
	fieldWriter := newJSONArrayWriter(w.vw)
	for _, field := range t.Desc.(StructDesc).Fields {
		fieldWriter.write(field.Name)
		fieldWriter.writeTypeAsTag(field.T, backRefs)
		fieldWriter.write(field.Optional)
	}
	w.write(fieldWriter.toArray())
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

func (w *jsonArrayWriter) writeStruct(v Value, t *Type) {
	i := 0
	values := structReader(v.(Struct), t)
	desc := t.Desc.(StructDesc)

	for _, f := range desc.Fields {
		if f.Optional {
			ok := bool(values[i].(Bool))
			i++
			w.write(ok)
			if ok {
				w.writeValue(values[i], f.T)
				i++
			}
		} else {
			w.writeValue(values[i], f.T)
			i++
		}
	}
}
