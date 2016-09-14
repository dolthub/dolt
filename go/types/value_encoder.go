// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"fmt"

	"github.com/attic-labs/noms/go/d"
)

type valueEncoder struct {
	nomsWriter
	vw ValueWriter
}

func newValueEncoder(w nomsWriter, vw ValueWriter) *valueEncoder {
	return &valueEncoder{w, vw}
}

func (w *valueEncoder) writeKind(kind NomsKind) {
	w.writeUint8(uint8(kind))
}

func (w *valueEncoder) writeRef(r Ref) {
	w.writeHash(r.TargetHash())
	w.writeUint64(r.Height())
}

func (w *valueEncoder) writeType(t *Type, parentStructTypes []*Type) {
	k := t.Kind()
	switch k {
	case ListKind, MapKind, RefKind, SetKind:
		w.writeKind(k)
		for _, elemType := range t.Desc.(CompoundDesc).ElemTypes {
			w.writeType(elemType, parentStructTypes)
		}

	case UnionKind:
		w.writeKind(k)
		elemTypes := t.Desc.(CompoundDesc).ElemTypes
		w.writeUint32(uint32(len(elemTypes)))
		for _, elemType := range elemTypes {
			w.writeType(elemType, parentStructTypes)
		}
	case StructKind:
		w.writeStructType(t, parentStructTypes)
	case CycleKind:
		w.writeCycle(uint32(t.Desc.(CycleDesc)))
	default:
		w.writeKind(k)
		d.PanicIfFalse(IsPrimitiveKind(k))
	}
}

func (w *valueEncoder) writeBlobLeafSequence(seq blobLeafSequence) {
	w.writeBytes(seq.data)
}

func (w *valueEncoder) writeValueSlice(values ValueSlice) {
	count := uint32(len(values))
	w.writeUint32(count)

	for i := uint32(0); i < count; i++ {
		w.writeValue(values[i])
	}
}

func (w *valueEncoder) writeListLeafSequence(seq listLeafSequence) {
	w.writeValueSlice(seq.values)
}

func (w *valueEncoder) writeSetLeafSequence(seq setLeafSequence) {
	w.writeValueSlice(seq.data)
}

func (w *valueEncoder) writeMapLeafSequence(seq mapLeafSequence) {
	count := uint32(len(seq.data))
	w.writeUint32(count)

	for i := uint32(0); i < count; i++ {
		w.writeValue(seq.data[i].key)
		w.writeValue(seq.data[i].value)
	}
}

func (w *valueEncoder) maybeWriteMetaSequence(seq sequence) bool {
	ms, ok := seq.(metaSequence)
	if !ok {
		w.writeBool(false) // not a meta sequence
		return false
	}

	w.writeBool(true) // a meta sequence

	count := ms.seqLen()
	w.writeUint32(uint32(count))
	for i := 0; i < count; i++ {
		tuple := ms.getItem(i).(metaTuple)
		if tuple.child != nil && w.vw != nil {
			// Write unwritten chunked sequences. Chunks are lazily written so that intermediate chunked structures like NewList().Append(x).Append(y) don't cause unnecessary churn.
			w.vw.WriteValue(tuple.child)
		}
		w.writeValue(tuple.ref)
		v := tuple.key.v
		if !tuple.key.isOrderedByValue {
			// See https://github.com/attic-labs/noms/issues/1688#issuecomment-227528987
			d.PanicIfTrue(tuple.key.h.IsEmpty())
			v = constructRef(MakeRefType(BoolType), tuple.key.h, 0)
		}
		w.writeValue(v)
		w.writeUint64(tuple.numLeaves)
	}
	return true
}

func (w *valueEncoder) writeValue(v Value) {
	t := v.Type()
	w.appendType(t)
	switch t.Kind() {
	case BlobKind:
		seq := v.(Blob).sequence()
		if w.maybeWriteMetaSequence(seq) {
			return
		}

		w.writeBlobLeafSequence(seq.(blobLeafSequence))
	case BoolKind:
		w.writeBool(bool(v.(Bool)))
	case NumberKind:
		w.writeNumber(v.(Number))
	case ListKind:
		seq := v.(List).sequence()
		if w.maybeWriteMetaSequence(seq) {
			return
		}

		w.writeListLeafSequence(seq.(listLeafSequence))
	case MapKind:
		seq := v.(Map).sequence()
		if w.maybeWriteMetaSequence(seq) {
			return
		}

		w.writeMapLeafSequence(seq.(mapLeafSequence))
	case RefKind:
		w.writeRef(v.(Ref))
	case SetKind:
		seq := v.(Set).sequence()
		if w.maybeWriteMetaSequence(seq) {
			return
		}

		w.writeSetLeafSequence(seq.(setLeafSequence))
	case StringKind:
		w.writeString(string(v.(String)))
	case TypeKind:
		w.writeType(v.(*Type), nil)
	case StructKind:
		w.writeStruct(v, t)
	case CycleKind, UnionKind, ValueKind:
		d.Chk.Fail(fmt.Sprintf("A value instance can never have type %s", KindToString[t.Kind()]))
	default:
		d.Chk.Fail("Unknown NomsKind")
	}
}

func (w *valueEncoder) writeStruct(v Value, t *Type) {
	for _, v := range v.(Struct).values {
		w.writeValue(v)
	}
}

func (w *valueEncoder) writeCycle(i uint32) {
	w.writeKind(CycleKind)
	w.writeUint32(i)
}

func (w *valueEncoder) writeStructType(t *Type, parentStructTypes []*Type) {
	// The runtime representaion of struct types can contain cycles. These cycles are broken when encoding and decoding using special "back ref" placeholders.
	i, found := indexOfType(t, parentStructTypes)
	if found {
		w.writeCycle(uint32(len(parentStructTypes)) - i - 1)
		return
	}
	parentStructTypes = append(parentStructTypes, t)

	w.writeKind(StructKind)
	desc := t.Desc.(StructDesc)
	w.writeString(desc.Name)

	w.writeUint32(uint32(desc.Len()))

	for _, field := range desc.fields {
		w.writeString(field.name)
		w.writeType(field.t, parentStructTypes)
	}
}
