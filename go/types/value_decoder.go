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
	validating bool
}

// |tc| must be locked as long as the valueDecoder is being used
func newValueDecoder(nr nomsReader, vr ValueReader) *valueDecoder {
	return &valueDecoder{nr, vr, false}
}

func newValueDecoderWithValidation(nr nomsReader, vr ValueReader) *valueDecoder {
	return &valueDecoder{nr, vr, true}
}

func (r *valueDecoder) readKind() NomsKind {
	return NomsKind(r.readUint8())
}

func (r *valueDecoder) readRef() Ref {
	h := r.readHash()
	targetType := r.readType()
	height := r.readCount()
	return constructRef(h, targetType, height)
}

func (r *valueDecoder) readType() *Type {
	t := r.readTypeInner(map[string]*Type{})
	if r.validating {
		validateType(t)
	}
	return t
}

func (r *valueDecoder) readTypeInner(seenStructs map[string]*Type) *Type {
	k := r.readKind()
	switch k {
	case ListKind:
		return makeCompoundType(ListKind, r.readTypeInner(seenStructs))
	case MapKind:
		return makeCompoundType(MapKind, r.readTypeInner(seenStructs), r.readTypeInner(seenStructs))
	case RefKind:
		return makeCompoundType(RefKind, r.readTypeInner(seenStructs))
	case SetKind:
		return makeCompoundType(SetKind, r.readTypeInner(seenStructs))
	case StructKind:
		return r.readStructType(seenStructs)
	case UnionKind:
		return r.readUnionType(seenStructs)
	case CycleKind:
		name := r.readString()
		d.PanicIfTrue(name == "") // cycles to anonymous structs are disallowed
		t, ok := seenStructs[name]
		d.PanicIfFalse(ok)
		return t
	}

	d.PanicIfFalse(IsPrimitiveKind(k))
	return MakePrimitiveType(k)
}

func (r *valueDecoder) readBlobLeafSequence() sequence {
	b := r.readBytes()
	return newBlobLeafSequence(r.vr, b)
}

func (r *valueDecoder) readValueSequence() ValueSlice {
	count := uint32(r.readCount())

	data := ValueSlice{}
	for i := uint32(0); i < count; i++ {
		v := r.readValue()
		data = append(data, v)
	}

	return data
}

func (r *valueDecoder) readListLeafSequence() sequence {
	data := r.readValueSequence()
	return listLeafSequence{leafSequence{r.vr, len(data), ListKind}, data}
}

func (r *valueDecoder) readSetLeafSequence() orderedSequence {
	data := r.readValueSequence()
	return setLeafSequence{leafSequence{r.vr, len(data), SetKind}, data}
}

func (r *valueDecoder) readMapLeafSequence() orderedSequence {
	count := r.readCount()
	data := []mapEntry{}
	for i := uint64(0); i < count; i++ {
		k := r.readValue()
		v := r.readValue()
		data = append(data, mapEntry{k, v})
	}

	return mapLeafSequence{leafSequence{r.vr, len(data), MapKind}, data}
}

func (r *valueDecoder) readMetaSequence(k NomsKind, level uint64) metaSequence {
	count := r.readCount()

	data := []metaTuple{}
	for i := uint64(0); i < count; i++ {
		ref := r.readValue().(Ref)
		v := r.readValue()
		var key orderedKey
		if r, ok := v.(Ref); ok {
			// See https://github.com/attic-labs/noms/issues/1688#issuecomment-227528987
			key = orderedKeyFromHash(r.TargetHash())
		} else {
			key = newOrderedKey(v)
		}
		numLeaves := r.readCount()
		data = append(data, newMetaTuple(ref, key, numLeaves, nil))
	}

	return newMetaSequence(k, level, data, r.vr)
}

func (r *valueDecoder) readValue() Value {
	k := r.readKind()
	switch k {
	case BlobKind:
		level := r.readCount()
		if level > 0 {
			return newBlob(r.readMetaSequence(k, level))
		}

		return newBlob(r.readBlobLeafSequence())
	case BoolKind:
		return Bool(r.readBool())
	case NumberKind:
		return r.readNumber()
	case StringKind:
		return String(r.readString())
	case ListKind:
		level := r.readCount()
		if level > 0 {
			return newList(r.readMetaSequence(k, level))
		}

		return newList(r.readListLeafSequence())
	case MapKind:
		level := r.readCount()
		if level > 0 {
			return newMap(r.readMetaSequence(k, level))
		}

		return newMap(r.readMapLeafSequence())
	case RefKind:
		return r.readRef()
	case SetKind:
		level := r.readCount()
		if level > 0 {
			return newSet(r.readMetaSequence(k, level))
		}

		return newSet(r.readSetLeafSequence())
	case StructKind:
		return r.readStruct()
	case TypeKind:
		return r.readType()
	case CycleKind, UnionKind, ValueKind:
		d.Chk.Fail(fmt.Sprintf("A value instance can never have type %s", k))
	}

	panic("not reachable")
}

func (r *valueDecoder) readStruct() Value {
	name := r.readString()
	count := r.readCount()

	fieldNames := make([]string, count)
	values := make([]Value, count)
	for i := uint64(0); i < count; i++ {
		fieldNames[i] = r.readString()
		values[i] = r.readValue()
	}

	return Struct{name, fieldNames, values, &hash.Hash{}}
}

func boolToUint32(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}

func (r *valueDecoder) readStructType(seenStructs map[string]*Type) *Type {
	name := r.readString()
	count := r.readCount()
	fields := make(structTypeFields, count)

	t := newType(StructDesc{name, fields})
	seenStructs[name] = t

	for i := uint64(0); i < count; i++ {
		t.Desc.(StructDesc).fields[i] = StructField{
			Name: r.readString(),
		}
	}
	for i := uint64(0); i < count; i++ {
		t.Desc.(StructDesc).fields[i].Type = r.readTypeInner(seenStructs)
	}
	for i := uint64(0); i < count; i++ {
		t.Desc.(StructDesc).fields[i].Optional = r.readBool()
	}

	return t
}

func (r *valueDecoder) readUnionType(seenStructs map[string]*Type) *Type {
	l := r.readCount()
	ts := make(typeSlice, l)
	for i := uint64(0); i < l; i++ {
		ts[i] = r.readTypeInner(seenStructs)
	}
	return makeCompoundType(UnionKind, ts...)
}
