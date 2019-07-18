// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/liquidata-inc/ld/dolt/go/store/d"

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

func (r *valueDecoder) readRef(nbf *NomsBinFormat) Ref {
	return readRef(nbf, &(r.typedBinaryNomsReader))
}

func (r *valueDecoder) skipRef() {
	skipRef(&(r.typedBinaryNomsReader))
}

func (r *valueDecoder) skipBlobLeafSequence(nbf *NomsBinFormat) ([]uint32, uint64) {
	size := r.readCount()
	valuesPos := r.pos()
	r.offset += uint32(size)
	return []uint32{valuesPos, r.pos()}, size
}

func (r *valueDecoder) skipValueSequence(nbf *NomsBinFormat, elementsPerIndex int) ([]uint32, uint64) {
	count := r.readCount()
	offsets := make([]uint32, count+1)
	offsets[0] = r.pos()
	for i := uint64(0); i < count; i++ {
		for j := 0; j < elementsPerIndex; j++ {
			r.skipValue(nbf)
		}
		offsets[i+1] = r.pos()
	}
	return offsets, count
}

func (r *valueDecoder) skipListLeafSequence(nbf *NomsBinFormat) ([]uint32, uint64) {
	return r.skipValueSequence(nbf, getValuesPerIdx(ListKind))
}

func (r *valueDecoder) skipSetLeafSequence(nbf *NomsBinFormat) ([]uint32, uint64) {
	return r.skipValueSequence(nbf, getValuesPerIdx(SetKind))
}

func (r *valueDecoder) skipMapLeafSequence(nbf *NomsBinFormat) ([]uint32, uint64) {
	return r.skipValueSequence(nbf, getValuesPerIdx(MapKind))
}

func (r *valueDecoder) readSequence(nbf *NomsBinFormat, kind NomsKind, leafSkipper func(nbf *NomsBinFormat) ([]uint32, uint64)) sequence {
	start := r.pos()
	offsets := []uint32{start}
	r.skipKind()
	offsets = append(offsets, r.pos())
	level := r.readCount()
	offsets = append(offsets, r.pos())
	var seqOffsets []uint32
	var length uint64
	if level > 0 {
		seqOffsets, length = r.skipMetaSequence(nbf, kind, level)
	} else {
		seqOffsets, length = leafSkipper(nbf)
	}
	offsets = append(offsets, seqOffsets...)
	end := r.pos()

	if level > 0 {
		return newMetaSequence(r.vrw, r.byteSlice(start, end), offsets, length)
	}

	return newLeafSequence(r.vrw, r.byteSlice(start, end), offsets, length)
}

func (r *valueDecoder) readBlobSequence(nbf *NomsBinFormat) sequence {
	seq := r.readSequence(nbf, BlobKind, r.skipBlobLeafSequence)
	if seq.isLeaf() {
		return blobLeafSequence{seq.(leafSequence)}
	}
	return seq
}

func (r *valueDecoder) readListSequence(nbf *NomsBinFormat) sequence {
	seq := r.readSequence(nbf, ListKind, r.skipListLeafSequence)
	if seq.isLeaf() {
		return listLeafSequence{seq.(leafSequence)}
	}
	return seq
}

func (r *valueDecoder) readSetSequence(nbf *NomsBinFormat) orderedSequence {
	seq := r.readSequence(nbf, SetKind, r.skipSetLeafSequence)
	if seq.isLeaf() {
		return setLeafSequence{seq.(leafSequence)}
	}
	return seq.(orderedSequence)
}

func (r *valueDecoder) readMapSequence(nbf *NomsBinFormat) orderedSequence {
	seq := r.readSequence(nbf, MapKind, r.skipMapLeafSequence)
	if seq.isLeaf() {
		return mapLeafSequence{seq.(leafSequence)}
	}
	return seq.(orderedSequence)
}

func (r *valueDecoder) skipList(nbf *NomsBinFormat) {
	r.skipSequence(nbf, ListKind, r.skipListLeafSequence)
}

func (r *valueDecoder) skipSet(nbf *NomsBinFormat) {
	r.skipSequence(nbf, SetKind, r.skipSetLeafSequence)
}

func (r *valueDecoder) skipMap(nbf *NomsBinFormat) {
	r.skipSequence(nbf, MapKind, r.skipMapLeafSequence)
}

func (r *valueDecoder) skipBlob(nbf *NomsBinFormat) {
	r.skipSequence(nbf, BlobKind, r.skipBlobLeafSequence)
}

func (r *valueDecoder) skipSequence(nbf *NomsBinFormat, kind NomsKind, leafSkipper func(nbf *NomsBinFormat) ([]uint32, uint64)) {
	r.skipKind()
	level := r.readCount()
	if level > 0 {
		r.skipMetaSequence(nbf, kind, level)
	} else {
		leafSkipper(nbf)
	}
}

func (r *valueDecoder) skipOrderedKey(nbf *NomsBinFormat) {
	switch r.peekKind() {
	case hashKind:
		r.skipKind()
		r.skipHash()
	default:
		r.skipValue(nbf)
	}
}

func (r *valueDecoder) skipMetaSequence(nbf *NomsBinFormat, k NomsKind, level uint64) ([]uint32, uint64) {
	count := r.readCount()
	offsets := make([]uint32, count+1)
	offsets[0] = r.pos()
	length := uint64(0)
	for i := uint64(0); i < count; i++ {
		r.skipRef()
		r.skipOrderedKey(nbf)
		length += r.readCount()
		offsets[i+1] = r.pos()
	}
	return offsets, length
}

func (r *valueDecoder) readValue(nbf *NomsBinFormat) Value {
	k := r.peekKind()
	switch k {
	case BlobKind:
		return newBlob(r.readBlobSequence(nbf))
	case BoolKind:
		r.skipKind()
		return Bool(r.readBool())
	case FloatKind:
		r.skipKind()
		return r.readFloat(nbf)
	case UUIDKind:
		r.skipKind()
		return r.readUUID()
	case IntKind:
		r.skipKind()
		return r.readInt()
	case UintKind:
		r.skipKind()
		return r.readUint()
	case NullKind:
		r.skipKind()
		return NullValue
	case StringKind:
		r.skipKind()
		return String(r.readString())
	case ListKind:
		return newList(r.readListSequence(nbf))
	case MapKind:
		return newMap(r.readMapSequence(nbf))
	case RefKind:
		return r.readRef(nbf)
	case SetKind:
		return newSet(r.readSetSequence(nbf))
	case StructKind:
		return r.readStruct(nbf)
	case TupleKind:
		return r.readTuple(nbf)
	case TypeKind:
		r.skipKind()
		return r.readType()
	case CycleKind, UnionKind, ValueKind:
		d.Panic("A value instance can never have type %s", k)
	}

	panic("not reachable")
}

func (r *valueDecoder) skipValue(nbf *NomsBinFormat) {
	k := r.peekKind()
	switch k {
	case BlobKind:
		r.skipBlob(nbf)
	case BoolKind:
		r.skipKind()
		r.skipBool()
	case FloatKind:
		r.skipKind()
		r.skipFloat(nbf)
	case UUIDKind:
		r.skipKind()
		r.skipUUID()
	case NullKind:
		r.skipKind()
	case IntKind:
		r.skipKind()
		r.skipInt()
	case UintKind:
		r.skipKind()
		r.skipUint()
	case StringKind:
		r.skipKind()
		r.skipString()
	case ListKind:
		r.skipList(nbf)
	case MapKind:
		r.skipMap(nbf)
	case RefKind:
		r.skipRef()
	case SetKind:
		r.skipSet(nbf)
	case StructKind:
		r.skipStruct(nbf)
	case TupleKind:
		r.skipTuple(nbf)
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
func (r *valueDecoder) readTypeOfValue(nbf *NomsBinFormat) *Type {
	k := r.peekKind()
	switch k {
	case BlobKind:
		r.skipBlob(nbf)
		return BlobType
	case BoolKind:
		r.skipKind()
		r.skipBool()
		return BoolType
	case FloatKind:
		r.skipKind()
		r.skipFloat(nbf)
		return FloaTType
	case UUIDKind:
		r.skipKind()
		r.skipUUID()
		return UUIDType
	case IntKind:
		r.skipKind()
		r.skipInt()
		return IntType
	case UintKind:
		r.skipKind()
		r.skipUint()
		return UintType
	case NullKind:
		r.skipKind()
		return NullType
	case StringKind:
		r.skipKind()
		r.skipString()
		return StringType
	case ListKind, MapKind, RefKind, SetKind:
		// These do not decode the actual values anyway.
		return r.readValue(nbf).typeOf()
	case StructKind:
		return readStructTypeOfValue(nbf, r)
	case TupleKind:
		return r.readValue(nbf).typeOf()
	case TypeKind:
		r.skipKind()
		r.skipType()
		return TypeType
	case CycleKind, UnionKind, ValueKind:
		d.Panic("A value instance can never have type %s", k)
	}

	panic("not reachable")
}

// isValueSameTypeForSure may return false even though the type of the value is
// equal. We do that in cases wherer it would be too expensive to compute the
// type.
// If this returns false the decoder might not have visited the whole value and
// its offset is no longer valid.
func (r *valueDecoder) isValueSameTypeForSure(nbf *NomsBinFormat, t *Type) bool {
	k := r.peekKind()
	if k != t.TargetKind() {
		return false
	}

	switch k {
	case BlobKind, BoolKind, FloatKind, StringKind, UUIDKind, IntKind, UintKind, NullKind:
		r.skipValue(nbf)
		return true
	case ListKind, MapKind, RefKind, SetKind, TupleKind:
		// TODO: Maybe do some simple cases here too. Performance metrics should determine
		// what is going to be worth doing.
		// https://github.com/attic-labs/noms/issues/3776
		return false
	case StructKind:
		return isStructSameTypeForSure(nbf, r, t)
	case TypeKind:
		return false
	case CycleKind, UnionKind, ValueKind:
		d.Panic("A value instance can never have type %s", k)
	}

	panic("not reachable")
}

// isStringSame checks if the next string in the decoder matches string. It
// moves the decoder to after the string in all cases.
func (r *valueDecoder) isStringSame(s string) bool {
	count := r.readCount()
	start := uint64(r.offset)
	r.offset += uint32(count)
	if uint64(len(s)) != count {
		return false
	}

	for i := uint64(0); i < count; i++ {
		if s[i] != r.buff[start+i] {
			return false
		}
	}
	return true
}

func (r *valueDecoder) readStruct(nbf *NomsBinFormat) Value {
	return readStruct(nbf, r)
}

func (r *valueDecoder) readTuple(nbf *NomsBinFormat) Value {
	return readTuple(nbf, r)
}

func (r *valueDecoder) skipStruct(nbf *NomsBinFormat) {
	skipStruct(nbf, r)
}

func (r *valueDecoder) skipTuple(nbf *NomsBinFormat) {
	skipTuple(nbf, r)
}

func (r *valueDecoder) readOrderedKey(nbf *NomsBinFormat) orderedKey {
	switch r.peekKind() {
	case hashKind:
		r.skipKind()
		h := r.readHash()
		return orderedKeyFromHash(h)
	default:
		v := r.readValue(nbf)
		return newOrderedKey(v, nbf)
	}
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
	case TupleKind:
		return makeCompoundType(TupleKind, r.readTypeInner(seenStructs))
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
	case ListKind, RefKind, SetKind, TupleKind:
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
	return makeUnionType(ts...)
}

func (r *typedBinaryNomsReader) skipUnionType() {
	l := r.readCount()
	for i := uint64(0); i < l; i++ {
		r.skipTypeInner()
	}
}
