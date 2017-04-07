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

func (r *valueDecoder) readRef() Ref {
	h := r.readHash()
	targetType := r.readType(map[string]*Type{})
	height := r.readUint64()
	return constructRef(h, targetType, height)
}

func (r *valueDecoder) readType(seenStructs map[string]*Type) *Type {
	r.typeDepth++
	t := r.readTypeInner(seenStructs)
	r.typeDepth--
	if r.typeDepth == 0 {
		if r.validating {
			checkStructType(t, checkKindValidate)
		}
	}
	return t
}

func (r *valueDecoder) readTypeInner(seenStructs map[string]*Type) *Type {
	k := r.readKind()
	switch k {
	case ListKind:
		return makeCompoundType(ListKind, r.readType(seenStructs))
	case MapKind:
		return makeCompoundType(MapKind, r.readType(seenStructs), r.readType(seenStructs))
	case RefKind:
		return makeCompoundType(RefKind, r.readType(seenStructs))
	case SetKind:
		return makeCompoundType(SetKind, r.readType(seenStructs))
	case StructKind:
		return r.readStructType(seenStructs)
	case UnionKind:
		return r.readUnionType(seenStructs)
	case CycleKind:
		name := r.readString()
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
	count := r.readUint32()

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
	count := r.readUint32()
	data := []mapEntry{}
	for i := uint32(0); i < count; i++ {
		k := r.readValue()
		v := r.readValue()
		data = append(data, mapEntry{k, v})
	}

	return mapLeafSequence{leafSequence{r.vr, len(data), MapKind}, data}
}

func (r *valueDecoder) readMetaSequence(k NomsKind) metaSequence {
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

	return newMetaSequence(data, k, r.vr)
}

func (r *valueDecoder) readValue() Value {
	k := r.readKind()
	switch k {
	case BlobKind:
		isMeta := r.readBool()
		if isMeta {
			return newBlob(r.readMetaSequence(k))
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
			return newList(r.readMetaSequence(k))
		}

		return newList(r.readListLeafSequence())
	case MapKind:
		isMeta := r.readBool()
		if isMeta {
			return newMap(r.readMetaSequence(k))
		}

		return newMap(r.readMapLeafSequence())
	case RefKind:
		return r.readRef()
	case SetKind:
		isMeta := r.readBool()
		if isMeta {
			return newSet(r.readMetaSequence(k))
		}

		return newSet(r.readSetLeafSequence())
	case StructKind:
		return r.readStruct()
	case TypeKind:
		return r.readType(map[string]*Type{})
	case CycleKind, UnionKind, ValueKind:
		d.Chk.Fail(fmt.Sprintf("A value instance can never have type %s", k))
	}

	panic("not reachable")
}

func (r *valueDecoder) readStruct() Value {
	name := r.readString()
	count := r.readUint32()

	fieldNames := make([]string, count)
	for i := uint32(0); i < count; i++ {
		fieldNames[i] = r.readString()
	}

	values := make([]Value, count)
	for i := uint32(0); i < count; i++ {
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
	count := r.readUint32()
	fields := make(structTypeFields, count)

	t := newType(StructDesc{name, fields})
	seenStructs[name] = t

	for i := uint32(0); i < count; i++ {
		t.Desc.(StructDesc).fields[i] = StructField{
			Name: r.readString(),
		}
	}
	for i := uint32(0); i < count; i++ {
		t.Desc.(StructDesc).fields[i].Type = r.readType(seenStructs)
	}
	for i := uint32(0); i < count; i++ {
		t.Desc.(StructDesc).fields[i].Optional = r.readBool()
	}

	return t
}

func (r *valueDecoder) readUnionType(seenStructs map[string]*Type) *Type {
	l := r.readUint32()
	ts := make(typeSlice, l)
	for i := uint32(0); i < l; i++ {
		ts[i] = r.readType(seenStructs)
	}
	return makeCompoundType(UnionKind, ts...)
}
