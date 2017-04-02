// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"fmt"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

type valueDecoder struct {
	nomsReader
	vr         ValueReader
	typeDepth  int
	validating bool
}

// |tc| must be locked as long as the valueDecoder is being used
func newValueDecoder(nr nomsReader, vr ValueReader) *valueDecoder {
	return &valueDecoder{nr, vr, 0, false}
}

func newValueDecoderWithValidation(nr nomsReader, vr ValueReader) *valueDecoder {
	return &valueDecoder{nr, vr, 0, true}
}

func (r *valueDecoder) readKind() NomsKind {
	return NomsKind(r.readUint8())
}

func (r *valueDecoder) readRef(t *Type) Ref {
	h := r.readHash()
	height := r.readUint64()
	return constructRef(t, h, height)
}

func (r *valueDecoder) readType() *Type {
	r.typeDepth++
	t := r.readTypeInner()
	r.typeDepth--
	if r.typeDepth == 0 {
		if r.validating {
			checkStructType(t, checkKindValidate)
		}
	}
	return t
}

func (r *valueDecoder) readTypeInner() *Type {
	k := r.readKind()
	switch k {
	case ListKind:
		return makeCompoundType(ListKind, r.readType())
	case MapKind:
		return makeCompoundType(MapKind, r.readType(), r.readType())
	case RefKind:
		return makeCompoundType(RefKind, r.readType())
	case SetKind:
		return makeCompoundType(SetKind, r.readType())
	case StructKind:
		return r.readStructType()
	case UnionKind:
		return r.readUnionType()
	case CycleKind:
		return MakeCycleType(r.readUint32())
	}

	d.PanicIfFalse(IsPrimitiveKind(k))
	return MakePrimitiveType(k)
}

func (r *valueDecoder) readBlobLeafSequence() sequence {
	b := r.readBytes()
	return newBlobLeafSequence(r.vr, b)
}

func (r *valueDecoder) readValueSequence() ValueSlice {
	count := r.readUint32()

	data := ValueSlice{}
	for i := uint32(0); i < count; i++ {
		v := r.readValue()
		data = append(data, v)
	}

	return data
}

func (r *valueDecoder) readListLeafSequence(t *Type) sequence {
	data := r.readValueSequence()
	return listLeafSequence{leafSequence{r.vr, len(data), t}, data}
}

func (r *valueDecoder) readSetLeafSequence(t *Type) orderedSequence {
	data := r.readValueSequence()
	return setLeafSequence{leafSequence{r.vr, len(data), t}, data}
}

func (r *valueDecoder) readMapLeafSequence(t *Type) orderedSequence {
	count := r.readUint32()
	data := []mapEntry{}
	for i := uint32(0); i < count; i++ {
		k := r.readValue()
		v := r.readValue()
		data = append(data, mapEntry{k, v})
	}

	return mapLeafSequence{leafSequence{r.vr, len(data), t}, data}
}

func (r *valueDecoder) readMetaSequence(t *Type) metaSequence {
	count := r.readUint32()

	data := []metaTuple{}
	for i := uint32(0); i < count; i++ {
		ref := r.readValue().(Ref)
		v := r.readValue()
		var key orderedKey
		if r, ok := v.(Ref); ok {
			// See https://github.com/attic-labs/noms/issues/1688#issuecomment-227528987
			key = orderedKeyFromHash(r.TargetHash())
		} else {
			key = newOrderedKey(v)
		}
		numLeaves := r.readUint64()
		data = append(data, newMetaTuple(ref, key, numLeaves, nil))
	}

	return newMetaSequence(data, t, r.vr)
}

func (r *valueDecoder) readValue() Value {
	t := r.readType()
	switch t.TargetKind() {
	case BlobKind:
		isMeta := r.readBool()
		if isMeta {
			return newBlob(r.readMetaSequence(t))
		}

		return newBlob(r.readBlobLeafSequence())
	case BoolKind:
		return Bool(r.readBool())
	case NumberKind:
		return r.readNumber()
	case StringKind:
		return String(r.readString())
	case ListKind:
		isMeta := r.readBool()
		if isMeta {
			return newList(r.readMetaSequence(t))
		}

		return newList(r.readListLeafSequence(t))
	case MapKind:
		isMeta := r.readBool()
		if isMeta {
			return newMap(r.readMetaSequence(t))
		}

		return newMap(r.readMapLeafSequence(t))
	case RefKind:
		return r.readRef(t)
	case SetKind:
		isMeta := r.readBool()
		if isMeta {
			return newSet(r.readMetaSequence(t))
		}

		return newSet(r.readSetLeafSequence(t))
	case StructKind:
		return r.readStruct(t)
	case TypeKind:
		return r.readType()
	case CycleKind, UnionKind, ValueKind:
		d.Chk.Fail(fmt.Sprintf("A value instance can never have type %s", KindToString[t.TargetKind()]))
	}

	panic("not reachable")
}

func (r *valueDecoder) readStruct(t *Type) Value {
	// We've read `[StructKind, name, fields, unions` at this point
	desc := t.Desc.(StructDesc)
	count := desc.Len()
	valueFields := make(structValueFields, count)
	for i, tf := range desc.fields {
		valueFields[i] = structValueField{tf.Name, r.readValue()}
	}

	return Struct{desc.Name, valueFields, t, &hash.Hash{}}
}

func boolToUint32(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}

func (r *valueDecoder) readStructType() *Type {
	name := r.readString()
	count := r.readUint32()

	fields := make(structTypeFields, count)
	for i := uint32(0); i < count; i++ {
		fields[i] = StructField{
			r.readString(),
			r.readType(),
			r.readBool(),
		}
	}

	return makeStructTypeQuickly(name, fields, checkKindNoValidate)
}

func (r *valueDecoder) readUnionType() *Type {
	l := r.readUint32()
	ts := make(typeSlice, l)
	for i := uint32(0); i < l; i++ {
		ts[i] = r.readType()
	}
	return makeCompoundType(UnionKind, ts...)
}
