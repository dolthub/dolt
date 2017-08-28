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
}

func newValueEncoder(w nomsWriter) *valueEncoder {
	return &valueEncoder{w}
}

func (w *valueEncoder) writeKind(kind NomsKind) {
	w.writeUint8(uint8(kind))
}

func (w *valueEncoder) writeRef(r Ref) {
	w.writeHash(r.TargetHash())
	w.writeType(r.TargetType(), map[string]*Type{})
	w.writeCount(r.Height())
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
		w.writeCount(uint64(len(elemTypes)))
		for _, elemType := range elemTypes {
			w.writeType(elemType, seenStructs)
		}
	case StructKind:
		w.writeStructType(t, seenStructs)
	default:
		if !IsPrimitiveKind(k) {
			d.Panic("Expected primitive noms kind, got %s", k.String())
		}
		w.writeKind(k)
	}
}

func (w *valueEncoder) writeBlobLeafSequence(seq blobLeafSequence) {
	w.writeBytes(seq.data)
}

func (w *valueEncoder) writeValueSlice(values ValueSlice) {
	count := uint32(len(values))
	w.writeCount(uint64(count))

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
	w.writeCount(uint64(count))

	for i := uint32(0); i < count; i++ {
		w.writeValue(seq.data[i].key)
		w.writeValue(seq.data[i].value)
	}
}

func (w *valueEncoder) maybeWriteMetaSequence(seq sequence) bool {
	if seq.isLeaf() {
		w.writeCount(0) // leaf
		return false
	}

	ms := seq.(metaSequence)
	d.PanicIfFalse(ms.level > 0)
	w.writeCount(ms.level)

	count := ms.seqLen()
	w.writeCount(uint64(count))
	for i := 0; i < count; i++ {
		tuple := ms.getItem(i).(metaTuple)
		w.writeValue(tuple.ref)
		v := tuple.key.v
		if !tuple.key.isOrderedByValue {
			// See https://github.com/attic-labs/noms/issues/1688#issuecomment-227528987
			d.PanicIfTrue(tuple.key.h.IsEmpty())
			v = constructRef(tuple.key.h, BoolType, 0)
		}
		w.writeValue(v)
		w.writeCount(tuple.numLeaves)
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
	w.writeCount(uint64(len(s.fieldNames)))

	for i, name := range s.fieldNames {
		w.writeString(name)
		w.writeValue(s.values[i])
	}
}

func (w *valueEncoder) writeStructType(t *Type, seenStructs map[string]*Type) {
	desc := t.Desc.(StructDesc)
	name := desc.Name

	if name != "" {
		if _, ok := seenStructs[name]; ok {
			w.writeKind(CycleKind)
			w.writeString(name)
			return
		}
		seenStructs[name] = t
	}

	w.writeKind(StructKind)
	w.writeString(desc.Name)
	w.writeCount(uint64(desc.Len()))

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
