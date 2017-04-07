// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"fmt"
	"math"

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
	w.writeType(r.TargetType(), map[string]*Type{})
	w.writeUint64(r.Height())
}

func (w *valueEncoder) writeType(t *Type, seenStructs map[string]*Type) {
	k := t.TargetKind()
	switch k {
	case ListKind, MapKind, RefKind, SetKind:
		w.writeKind(k)
		for _, elemType := range t.Desc.(CompoundDesc).ElemTypes {
			w.writeType(elemType, seenStructs)
		}

	case UnionKind:
		w.writeKind(k)
		elemTypes := t.Desc.(CompoundDesc).ElemTypes
		w.writeUint32(uint32(len(elemTypes)))
		for _, elemType := range elemTypes {
			w.writeType(elemType, seenStructs)
		}
	case StructKind:
		w.writeStructType(t, seenStructs)
	default:
		d.PanicIfFalse(IsPrimitiveKind(k))
		w.writeKind(k)
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
			v = constructRef(tuple.key.h, BoolType, 0)
		}
		w.writeValue(v)
		w.writeUint64(tuple.numLeaves)
	}
	return true
}

func (w *valueEncoder) writeValue(v Value) {
	k := v.Kind()
	w.writeKind(k)

	switch k {
	case BlobKind:
		seq := v.(Blob).sequence()
		if w.maybeWriteMetaSequence(seq) {
			return
		}

		w.writeBlobLeafSequence(seq.(blobLeafSequence))
	case BoolKind:
		w.writeBool(bool(v.(Bool)))
	case NumberKind:
		n := v.(Number)
		f := float64(n)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			d.Panic("%f is not a supported number", f)
		}
		w.writeNumber(n)
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
		w.writeType(v.(*Type), map[string]*Type{})
	case StructKind:
		w.writeStruct(v.(Struct))
	case CycleKind, UnionKind, ValueKind:
		d.Chk.Fail(fmt.Sprintf("A value instance can never have type %s", k))
	default:
		d.Chk.Fail("Unknown NomsKind")
	}
}

func (w *valueEncoder) writeStruct(s Struct) {
	w.writeString(s.name)
	w.writeUint32(uint32(len(s.fieldNames)))

	// Write field names first because they will compress better together.
	for _, name := range s.fieldNames {
		w.writeString(name)
	}

	for _, v := range s.values {
		w.writeValue(v)
	}
}

func (w *valueEncoder) writeCycle(i uint32) {
	w.writeKind(CycleKind)
	w.writeUint32(i)
}

func (w *valueEncoder) writeStructType(t *Type, seenStructs map[string]*Type) {
	desc := t.Desc.(StructDesc)
	name := desc.Name

	if _, ok := seenStructs[name]; ok {
		w.writeKind(CycleKind)
		w.writeString(name)
		return
	}
	seenStructs[name] = t

	w.writeKind(StructKind)
	w.writeString(desc.Name)
	w.writeUint32(uint32(desc.Len()))

	// Write all names, all types and finally all the optional flags.
	for _, field := range desc.fields {
		w.writeString(field.Name)
	}
	for _, field := range desc.fields {
		w.writeType(field.Type, seenStructs)
	}
	for _, field := range desc.fields {
		w.writeBool(field.Optional)
	}
}
