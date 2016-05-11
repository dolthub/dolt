package types

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/attic-labs/noms/d"
)

func encNomsValue(v Value, vw ValueWriter) []interface{} {
	w := newJSONArrayWriter(vw)
	w.writeValue(v)
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

func (w *jsonArrayWriter) writeUint16(v uint16) {
	w.write(v)
}

func (w *jsonArrayWriter) writeKind(kind NomsKind) {
	w.write(kind)
}

func (w *jsonArrayWriter) toArray() []interface{} {
	return w.a
}

func (w *jsonArrayWriter) writeRef(r Ref) {
	w.write(r.TargetRef().String())
	w.writeUint(r.Height())
}

func (w *jsonArrayWriter) writeType(t *Type, parentStructTypes []*Type) {
	k := t.Kind()
	switch k {
	case ListKind, MapKind, RefKind, SetKind:
		w.write(k)
		for _, elemType := range t.Desc.(CompoundDesc).ElemTypes {
			w.writeType(elemType, parentStructTypes)
		}

	case UnionKind:
		w.write(k)
		elemTypes := t.Desc.(CompoundDesc).ElemTypes
		w.writeUint16(uint16(len(elemTypes)))
		for _, elemType := range elemTypes {
			w.writeType(elemType, parentStructTypes)
		}
	case StructKind:
		w.writeStructType(t, parentStructTypes)
	case ParentKind:

	default:
		w.write(k)
		d.Chk.True(IsPrimitiveKind(k), "Kind: %v Desc: %s\n", t.Kind(), t.Describe())
	}
}

func (w *jsonArrayWriter) maybeWriteMetaSequence(seq sequence, tr *Type) bool {
	ms, ok := seq.(metaSequence)
	if !ok {
		w.write(false) // not a meta sequence
		return false
	}

	w.write(true) // a meta sequence
	w2 := newJSONArrayWriter(w.vw)
	for i := 0; i < ms.seqLen(); i++ {
		tuple := ms.getItem(i).(metaTuple)
		if tuple.child != nil && w.vw != nil {
			// Write unwritten chunked sequences. Chunks are lazily written so that intermediate chunked structures like NewList().Append(x).Append(y) don't cause unnecessary churn.
			w.vw.WriteValue(tuple.child)
		}
		w2.writeValue(tuple.ChildRef())
		w2.writeValue(tuple.value)
		w2.writeUint(tuple.numLeaves)
	}
	w.write(w2.toArray())
	return true
}

func (w *jsonArrayWriter) writeValue(v Value) {
	t := v.Type()
	w.writeType(t, nil)
	switch t.Kind() {
	case BlobKind:
		blob := v.(Blob)
		if w.maybeWriteMetaSequence(blob.sequence(), t) {
			return
		}
		w.writeBlob(blob)
	case BoolKind:
		w.writeBool(bool(v.(Bool)))
	case NumberKind:
		w.writeFloat(float64(v.(Number)))
	case ListKind:
		seq := v.(List).sequence()
		if w.maybeWriteMetaSequence(seq, t) {
			return
		}

		w2 := newJSONArrayWriter(w.vw)
		v.(List).IterAll(func(v Value, i uint64) {
			w2.writeValue(v)
		})
		w.write(w2.toArray())
	case MapKind:
		seq := v.(Map).sequence()
		if w.maybeWriteMetaSequence(seq, t) {
			return
		}

		w2 := newJSONArrayWriter(w.vw)
		v.(Map).IterAll(func(k, v Value) {
			w2.writeValue(k)
			w2.writeValue(v)
		})
		w.write(w2.toArray())
	case RefKind:
		w.writeRef(v.(Ref))
	case SetKind:
		seq := v.(Set).sequence()
		if w.maybeWriteMetaSequence(seq, t) {
			return
		}

		w2 := newJSONArrayWriter(w.vw)
		v.(Set).IterAll(func(v Value) {
			w2.writeValue(v)
		})
		w.write(w2.toArray())
	case StringKind:
		w.write(v.(String).String())
	case TypeKind:
		vt := v.(*Type)
		w.writeType(vt, nil)
	case StructKind:
		w.writeStruct(v, t)
	case ParentKind, UnionKind, ValueKind:
		d.Chk.Fail(fmt.Sprintf("A value instance can never have type %s", KindToString[t.Kind()]))
	default:
		d.Chk.Fail("Unknown NomsKind")
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

func (w *jsonArrayWriter) writeParent(i int) {
	w.write(ParentKind)
	w.write(uint8(i))
}

func (w *jsonArrayWriter) writeStructType(t *Type, parentStructTypes []*Type) {
	// The runtime representaion of struct types can contain cycles. These cycles are broken when encoding and decoding using special "back ref" placeholders.
	i := indexOfType(t, parentStructTypes)
	if i != -1 {
		w.writeParent(len(parentStructTypes) - i - 1)
		return
	}
	parentStructTypes = append(parentStructTypes, t)

	w.write(StructKind)
	w.write(t.Name())
	fieldWriter := newJSONArrayWriter(w.vw)
	t.Desc.(StructDesc).IterFields(func(name string, t *Type) {
		fieldWriter.write(name)
		fieldWriter.writeType(t, parentStructTypes)
	})
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
	values := structReader(v.(Struct), t)
	desc := t.Desc.(StructDesc)

	i := 0
	desc.IterFields(func(name string, t *Type) {
		w.writeValue(values[i])
		i++
	})
}
