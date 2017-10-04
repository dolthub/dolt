// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/d"

type valueDecoder struct {
	typedBinaryNomsReader
	vrw ValueReadWriter
}

// typedBinaryNomsReader provides some functionality for reading and skipping types that is shared by both valueDecoder and refWalker.
type typedBinaryNomsReader struct {
	binaryNomsReader
	validating bool
}

func newValueDecoder(buff []byte, vrw ValueReadWriter) valueDecoder {
	nr := binaryNomsReader{buff, 0}
	return valueDecoder{typedBinaryNomsReader{nr, false}, vrw}
}

func newValueDecoderWithValidation(nr binaryNomsReader, vrw ValueReadWriter) valueDecoder {
	return valueDecoder{typedBinaryNomsReader{nr, true}, vrw}
}

func (r *valueDecoder) copyString(w nomsWriter) {
	start := r.pos()
	r.skipString()
	end := r.pos()
	w.writeRaw(r.byteSlice(start, end))
}

func (r *valueDecoder) readRef() Ref {
	return readRef(&(r.typedBinaryNomsReader))
}

func (r *valueDecoder) skipRef() {
	skipRef(&(r.typedBinaryNomsReader))
}

func (r *valueDecoder) skipBlobLeafSequence() []uint32 {
	size := r.readCount()
	valuesPos := r.pos()
	r.offset += uint32(size)
	return []uint32{valuesPos, r.pos()}
}

func (r *valueDecoder) skipValueSequence() []uint32 {
	count := int(r.readCount())
	offsets := make([]uint32, count+1)
	offsets[0] = r.pos()
	for i := 0; i < count; i++ {
		r.skipValue()
		offsets[i+1] = r.pos()
	}
	return offsets
}

func (r *valueDecoder) readListSequence() sequence {
	start := r.pos()
	offsets := []uint32{start}
	r.skipKind()
	offsets = append(offsets, r.pos())
	level := r.readCount()
	offsets = append(offsets, r.pos())
	if level > 0 {
		offsets = append(offsets, r.skipMetaSequence(ListKind, level)...)
	} else {
		offsets = append(offsets, r.skipValueSequence()...)
	}
	end := r.pos()

	if level > 0 {
		return newMetaSequence(r.vrw, r.byteSlice(start, end), offsets)
	}

	return listLeafSequence{newLeafSequence(r.vrw, r.byteSlice(start, end), offsets)}
}

func (r *valueDecoder) readBlobSequence() sequence {
	start := r.pos()
	offsets := []uint32{start}
	r.skipKind()
	offsets = append(offsets, r.pos())
	level := r.readCount()
	offsets = append(offsets, r.pos())
	if level > 0 {
		offsets = append(offsets, r.skipMetaSequence(BlobKind, level)...)
	} else {
		offsets = append(offsets, r.skipBlobLeafSequence()...)
	}
	end := r.pos()

	if level > 0 {
		return newMetaSequence(r.vrw, r.byteSlice(start, end), offsets)
	}

	return blobLeafSequence{newLeafSequence(r.vrw, r.byteSlice(start, end), offsets)}
}

func (r *valueDecoder) readSetSequence() orderedSequence {
	start := r.pos()
	offsets := []uint32{start}
	r.skipKind()
	offsets = append(offsets, r.pos())
	level := r.readCount()
	offsets = append(offsets, r.pos())
	if level > 0 {
		offsets = append(offsets, r.skipMetaSequence(SetKind, level)...)
	} else {
		offsets = append(offsets, r.skipValueSequence()...)
	}
	end := r.pos()

	if level > 0 {
		return newMetaSequence(r.vrw, r.byteSlice(start, end), offsets)
	}

	return setLeafSequence{newLeafSequence(r.vrw, r.byteSlice(start, end), offsets)}
}

func (r *valueDecoder) readMapSequence() orderedSequence {
	start := r.pos()
	offsets := []uint32{start}
	r.skipKind()
	offsets = append(offsets, r.pos())
	level := r.readCount()
	offsets = append(offsets, r.pos())
	if level > 0 {
		offsets = append(offsets, r.skipMetaSequence(MapKind, level)...)
	} else {
		offsets = append(offsets, r.skipMapLeafSequence()...)
	}
	end := r.pos()

	if level > 0 {
		return newMetaSequence(r.vrw, r.byteSlice(start, end), offsets)
	}

	return mapLeafSequence{newLeafSequence(r.vrw, r.byteSlice(start, end), offsets)}
}

func (r *valueDecoder) skipList() {
	r.skipListOrSet(ListKind)
}

func (r *valueDecoder) skipSet() {
	r.skipListOrSet(SetKind)
}

func (r *valueDecoder) skipListOrSet(kind NomsKind) {
	r.skipKind()
	level := r.readCount()
	if level > 0 {
		r.skipMetaSequence(kind, level)
	} else {
		r.skipValueSequence()
	}
}

func (r *valueDecoder) skipMap() {
	r.skipKind()
	level := r.readCount()
	if level > 0 {
		r.skipMetaSequence(MapKind, level)
	} else {
		r.skipMapLeafSequence()
	}
}

func (r *valueDecoder) skipBlob() {
	r.skipKind()
	level := r.readCount()
	if level > 0 {
		r.skipMetaSequence(BlobKind, level)
	} else {
		r.skipBlobLeafSequence()
	}
}

func (r *valueDecoder) skipMapLeafSequence() []uint32 {
	count := r.readCount()
	offsets := make([]uint32, count+1)
	offsets[0] = r.pos()
	for i := uint64(0); i < count; i++ {
		r.skipValue() // k
		r.skipValue() // v
		offsets[i+1] = r.pos()
	}
	return offsets
}

func (r *valueDecoder) skipMetaSequence(k NomsKind, level uint64) []uint32 {
	count := r.readCount()
	offsets := make([]uint32, count+1)
	offsets[0] = r.pos()
	for i := uint64(0); i < count; i++ {
		r.skipRef()
		r.skipValue()
		r.skipCount() // numLeaves
		offsets[i+1] = r.pos()
	}
	return offsets
}

func (r *valueDecoder) readValue() Value {
	k := r.peekKind()
	switch k {
	case BlobKind:
		return newBlob(r.readBlobSequence())
	case BoolKind:
		r.skipKind()
		return Bool(r.readBool())
	case NumberKind:
		r.skipKind()
		return r.readNumber()
	case StringKind:
		r.skipKind()
		return String(r.readString())
	case ListKind:
		return newList(r.readListSequence())
	case MapKind:
		return newMap(r.readMapSequence())
	case RefKind:
		return r.readRef()
	case SetKind:
		return newSet(r.readSetSequence())
	case StructKind:
		return r.readStruct()
	case TypeKind:
		r.skipKind()
		return r.readType()
	case CycleKind, UnionKind, ValueKind:
		d.Panic("A value instance can never have type %s", k)
	}

	panic("not reachable")
}

func (r *valueDecoder) skipValue() {
	k := r.peekKind()
	switch k {
	case BlobKind:
		r.skipBlob()
	case BoolKind:
		r.skipKind()
		r.skipBool()
	case NumberKind:
		r.skipKind()
		r.skipNumber()
	case StringKind:
		r.skipKind()
		r.skipString()
	case ListKind:
		r.skipList()
	case MapKind:
		r.skipMap()
	case RefKind:
		r.skipRef()
	case SetKind:
		r.skipSet()
	case StructKind:
		r.skipStruct()
	case TypeKind:
		r.skipKind()
		r.skipType()
	case CycleKind, UnionKind, ValueKind:
		d.Panic("A value instance can never have type %s", k)
	default:
		panic("not reachable")
	}
}

// readTypeOfValue is basically readValue().typeOf() but it ensures that we do
// not allocate values where we do not need to.
func (r *valueDecoder) readTypeOfValue() *Type {
	k := r.peekKind()
	switch k {
	case BlobKind:
		r.skipBlob()
		return BlobType
	case BoolKind:
		r.skipKind()
		r.skipBool()
		return BoolType
	case NumberKind:
		r.skipKind()
		r.skipNumber()
		return NumberType
	case StringKind:
		r.skipKind()
		r.skipString()
		return StringType
	case ListKind, MapKind, RefKind, SetKind:
		// These do not decode the actual values anyway.
		return r.readValue().typeOf()
	case StructKind:
		return readStructTypeOfValue(r)
	case TypeKind:
		r.skipKind()
		r.skipType()
		return TypeType
	case CycleKind, UnionKind, ValueKind:
		d.Panic("A value instance can never have type %s", k)
	}

	panic("not reachable")
}

func (r *valueDecoder) copyValue(w nomsWriter) {
	start := r.pos()
	r.skipValue()
	end := r.pos()
	w.writeRaw(r.byteSlice(start, end))
}

func (r *valueDecoder) readStruct() Value {
	return readStruct(r)
}

func (r *valueDecoder) skipStruct() {
	skipStruct(r)
}

func boolToUint32(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}

func (r *valueDecoder) readOrderedKey() orderedKey {
	v := r.readValue()
	if r, ok := v.(Ref); ok {
		// See https://github.com/attic-labs/noms/issues/1688#issuecomment-227528987
		return orderedKeyFromHash(r.TargetHash())
	}
	return newOrderedKey(v)
}

func (r *typedBinaryNomsReader) readType() *Type {
	t := r.readTypeInner(map[string]*Type{})
	if r.validating {
		validateType(t)
	}
	return t
}

func (r *typedBinaryNomsReader) skipType() {
	if r.validating {
		r.readType()
		return
	}
	r.skipTypeInner()
}

func (r *typedBinaryNomsReader) readTypeInner(seenStructs map[string]*Type) *Type {
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

func (r *typedBinaryNomsReader) skipTypeInner() {
	k := r.readKind()
	switch k {
	case ListKind, RefKind, SetKind:
		r.skipTypeInner()
	case MapKind:
		r.skipTypeInner()
		r.skipTypeInner()
	case StructKind:
		r.skipStructType()
	case UnionKind:
		r.skipUnionType()
	case CycleKind:
		r.skipString()
	default:
		d.PanicIfFalse(IsPrimitiveKind(k))
	}
}

func (r *typedBinaryNomsReader) readStructType(seenStructs map[string]*Type) *Type {
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

func (r *typedBinaryNomsReader) skipStructType() {
	r.skipString() // name
	count := r.readCount()

	for i := uint64(0); i < count; i++ {
		r.skipString() // name
	}
	for i := uint64(0); i < count; i++ {
		r.skipTypeInner()
	}
	for i := uint64(0); i < count; i++ {
		r.skipBool() // optional
	}
}

func (r *typedBinaryNomsReader) readUnionType(seenStructs map[string]*Type) *Type {
	l := r.readCount()
	ts := make(typeSlice, l)
	for i := uint64(0); i < l; i++ {
		ts[i] = r.readTypeInner(seenStructs)
	}
	return makeCompoundType(UnionKind, ts...)
}

func (r *typedBinaryNomsReader) skipUnionType() {
	l := r.readCount()
	for i := uint64(0); i < l; i++ {
		r.skipTypeInner()
	}
}
