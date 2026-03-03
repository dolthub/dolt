// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/dolthub/dolt/go/store/d"
)

var ErrUnknownType = errors.New("unknown type $@")

type CodecReader interface {
	PeekKind() NomsKind
	ReadKind() NomsKind
	SkipValue(nbf *NomsBinFormat) error
	ReadUint() uint64
	ReadInt() int64
	ReadFloat(nbf *NomsBinFormat) float64
	ReadBool() bool
	ReadUUID() uuid.UUID
	ReadString() string
	ReadInlineBlob() []byte
	ReadTimestamp() (time.Time, error)
	ReadDecimal() (decimal.Decimal, error)
	ReadGeometry() (Geometry, error)
	ReadPoint() (Point, error)
	ReadLineString() (LineString, error)
	ReadPolygon() (Polygon, error)
	ReadMultiPoint() (MultiPoint, error)
	ReadMultiLineString() (MultiLineString, error)
	ReadMultiPolygon() (MultiPolygon, error)
	ReadGeomColl() (GeomColl, error)
}

var _ CodecReader = (*valueDecoder)(nil)

type valueDecoder struct {
	vrw ValueReadWriter
	typedBinaryNomsReader
}

// typedBinaryNomsReader provides some functionality for reading and skipping types that is shared by both valueDecoder and refWalker.
type typedBinaryNomsReader struct {
	binaryNomsReader
	validating bool
}

func newValueDecoder(buff []byte, vrw ValueReadWriter) valueDecoder {
	nr := binaryNomsReader{buff, 0}
	return valueDecoder{vrw, typedBinaryNomsReader{nr, false}}
}

func newValueDecoderWithValidation(nr binaryNomsReader, vrw ValueReadWriter) valueDecoder {
	return valueDecoder{vrw, typedBinaryNomsReader{nr, true}}
}

func (r *valueDecoder) ReadGeometry() (Geometry, error) {
	return readGeometry(nil, r)
}

func (r *valueDecoder) ReadPoint() (Point, error) {
	return readPoint(nil, r)
}

func (r *valueDecoder) ReadLineString() (LineString, error) {
	return readLineString(nil, r)
}

func (r *valueDecoder) ReadPolygon() (Polygon, error) {
	return readPolygon(nil, r)
}

func (r *valueDecoder) ReadMultiPoint() (MultiPoint, error) {
	return readMultiPoint(nil, r)
}

func (r *valueDecoder) ReadMultiLineString() (MultiLineString, error) {
	return readMultiLineString(nil, r)
}

func (r *valueDecoder) ReadMultiPolygon() (MultiPolygon, error) {
	return readMultiPolygon(nil, r)
}

func (r *valueDecoder) ReadGeomColl() (GeomColl, error) {
	return readGeomColl(nil, r)
}

func (r *valueDecoder) readRef(nbf *NomsBinFormat) (Ref, error) {
	return readRef(nbf, &(r.typedBinaryNomsReader))
}

func (r *valueDecoder) skipRef() error {
	_, err := skipRef(&(r.typedBinaryNomsReader))
	return err
}

func (r *valueDecoder) readValue(nbf *NomsBinFormat) (Value, error) {
	k := r.PeekKind()
	switch k {
	case BlobKind, ListKind, MapKind, SetKind:
		return nil, fmt.Errorf("unsupported collection kind: %s", k)
	// The following primitive kinds, through StringKind, are also
	// able to be processed by the IsPrimitiveKind branch below.
	// But we include them here for efficiency, since it is about
	// 30% faster to dispatch statically to the correct reader for
	// these very common primitive types.
	case BoolKind:
		r.skipKind()
		return Bool(r.ReadBool()), nil
	case FloatKind:
		r.skipKind()
		return Float(r.ReadFloat(nbf)), nil
	case IntKind:
		r.skipKind()
		return Int(r.ReadInt()), nil
	case UintKind:
		r.skipKind()
		return Uint(r.ReadUint()), nil
	case NullKind:
		r.skipKind()
		return NullValue, nil
	case StringKind:
		r.skipKind()
		return String(r.ReadString()), nil
	case RefKind:
		return r.readRef(nbf)
	case StructKind:
		return nil, fmt.Errorf("unsupported kind: %s", k)
	case TupleKind, JSONKind:
		return nil, fmt.Errorf("unsupported kind: %s", k)
	case GeometryKind:
		r.skipKind()
		buf := []byte(r.ReadString())
		srid, _, geomType, err := DeserializeEWKBHeader(buf)
		if err != nil {
			return nil, err
		}
		buf = buf[EWKBHeaderSize:]
		switch geomType {
		case WKBPointID:
			return DeserializeTypesPoint(buf, false, srid), nil
		case WKBLineID:
			return DeserializeTypesLine(buf, false, srid), nil
		case WKBPolyID:
			return DeserializeTypesPoly(buf, false, srid), nil
		case WKBMultiPointID:
			return DeserializeTypesMPoint(buf, false, srid), nil
		default:
			return nil, ErrUnknownType
		}
	case PointKind:
		r.skipKind()
		buf := []byte(r.ReadString())
		srid, _, geomType, err := DeserializeEWKBHeader(buf)
		if err != nil {
			return nil, err
		}
		if geomType != WKBPointID {
			return nil, ErrUnknownType
		}
		buf = buf[EWKBHeaderSize:]
		return DeserializeTypesPoint(buf, false, srid), nil
	case LineStringKind:
		r.skipKind()
		buf := []byte(r.ReadString())
		srid, _, geomType, err := DeserializeEWKBHeader(buf)
		if err != nil {
			return nil, err
		}
		if geomType != WKBLineID {
			return nil, ErrUnknownType
		}
		buf = buf[EWKBHeaderSize:]
		return DeserializeTypesLine(buf, false, srid), nil
	case PolygonKind:
		r.skipKind()
		buf := []byte(r.ReadString())
		srid, _, geomType, err := DeserializeEWKBHeader(buf)
		if err != nil {
			return nil, err
		}
		if geomType != WKBPolyID {
			return nil, ErrUnknownType
		}
		buf = buf[EWKBHeaderSize:]
		return DeserializeTypesPoly(buf, false, srid), nil
	case MultiPointKind:
		r.skipKind()
		buf := []byte(r.ReadString())
		srid, _, geomType, err := DeserializeEWKBHeader(buf)
		if err != nil {
			return nil, err
		}
		if geomType != WKBMultiPointID {
			return nil, ErrUnknownType
		}
		buf = buf[EWKBHeaderSize:]
		return DeserializeTypesMPoint(buf, false, srid), nil
	case MultiLineStringKind:
		r.skipKind()
		buf := []byte(r.ReadString())
		srid, _, geomType, err := DeserializeEWKBHeader(buf)
		if err != nil {
			return nil, err
		}
		if geomType != WKBMultiLineID {
			return nil, ErrUnknownType
		}
		buf = buf[EWKBHeaderSize:]
		return DeserializeTypesMLine(buf, false, srid), nil
	case MultiPolygonKind:
		r.skipKind()
		buf := []byte(r.ReadString())
		srid, _, geomType, err := DeserializeEWKBHeader(buf)
		if err != nil {
			return nil, err
		}
		if geomType != WKBMultiPolyID {
			return nil, ErrUnknownType
		}
		buf = buf[EWKBHeaderSize:]
		return DeserializeTypesMPoly(buf, false, srid), nil
	case TypeKind:
		r.skipKind()
		return r.readType()
	case CycleKind, UnionKind, ValueKind:
		d.Panic("A value instance can never have type %s", k)
	}

	if IsPrimitiveKind(k) {
		if int(k) < len(KindToTypeSlice) {
			emptyVal := KindToTypeSlice[int(k)]
			if emptyVal != nil {
				r.skipKind()
				return emptyVal.readFrom(nbf, &r.binaryNomsReader)
			}
		}
	}
	return nil, ErrUnknownType
}

func (r *valueDecoder) SkipValue(nbf *NomsBinFormat) error {
	k := r.PeekKind()
	switch k {
	case BlobKind, ListKind, MapKind, SetKind, StructKind, TupleKind, JSONKind:
		return fmt.Errorf("unsupported kind: %s", k)
	// The following primitive kinds, through StringKind, are also
	// able to be processed by the IsPrimitiveKind branch below.
	// But we include them here for efficiency, since it is about
	// 30% faster to dispatch statically to the correct reader for
	// these very common primitive types.
	case BoolKind:
		r.skipKind()
		r.skipBool()
	case FloatKind:
		r.skipKind()
		r.skipFloat(nbf)
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
	case GeometryKind:
		r.skipKind()
		r.skipString()
	case PointKind:
		r.skipKind()
		r.skipString()
	case LineStringKind:
		r.skipKind()
		r.skipString()
	case PolygonKind:
		r.skipKind()
		r.skipString()
	case MultiLineStringKind:
		r.skipKind()
		r.skipString()
	case MultiPointKind:
		r.skipKind()
		r.skipString()
	case MultiPolygonKind:
		r.skipKind()
		r.skipString()
	case RefKind:
		err := r.skipRef()
		if err != nil {
			return err
		}
	case TypeKind:
		r.skipKind()
		err := r.skipType()
		if err != nil {
			return err
		}
	case CycleKind, UnionKind, ValueKind:
		d.Panic("A value instance can never have type %s", k)
	default:
		if IsPrimitiveKind(k) {
			if int(k) < len(KindToTypeSlice) {
				emptyVal := KindToTypeSlice[int(k)]
				if emptyVal != nil {
					r.skipKind()
					emptyVal.skip(nbf, &r.binaryNomsReader)
					return nil
				}
			}
		}
		return ErrUnknownType
	}

	return nil
}

func (r *typedBinaryNomsReader) readType() (*Type, error) {
	t, err := r.readTypeInner(map[string]*Type{})

	if err != nil {
		return nil, err
	}

	return t, nil
}

func (r *typedBinaryNomsReader) skipType() error {
	if r.validating {
		_, err := r.readType()

		if err != nil {
			return err
		}
	}
	r.skipTypeInner()
	return nil
}

func (r *typedBinaryNomsReader) readTypeInner(seenStructs map[string]*Type) (*Type, error) {
	k := r.ReadKind()

	if supported := SupportedKinds[k]; !supported {
		return nil, ErrUnknownType
	}

	switch k {
	case ListKind:
		t, err := r.readTypeInner(seenStructs)

		if err != nil {
			return nil, err
		}

		return makeCompoundType(ListKind, t)
	case MapKind:
		kt, err := r.readTypeInner(seenStructs)

		if err != nil {
			return nil, err
		}

		vt, err := r.readTypeInner(seenStructs)

		if err != nil {
			return nil, err
		}

		return makeCompoundType(MapKind, kt, vt)
	case RefKind:
		t, err := r.readTypeInner(seenStructs)

		if err != nil {
			return nil, err
		}

		return makeCompoundType(RefKind, t)
	case SetKind:
		t, err := r.readTypeInner(seenStructs)

		if err != nil {
			return nil, err
		}

		return makeCompoundType(SetKind, t)
	case StructKind:
		return r.readStructType(seenStructs)
	case UnionKind:
		t, err := r.readUnionType(seenStructs)

		if err != nil {
			return nil, err
		}

		return t, nil
	case CycleKind:
		name := r.ReadString()
		d.PanicIfTrue(name == "") // cycles to anonymous structs are disallowed
		t, ok := seenStructs[name]
		d.PanicIfFalse(ok)
		return t, nil
	}

	d.PanicIfFalse(IsPrimitiveKind(k))
	return MakePrimitiveType(k)
}

func (r *typedBinaryNomsReader) skipTypeInner() {
	k := r.ReadKind()
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

func (r *typedBinaryNomsReader) readStructType(seenStructs map[string]*Type) (*Type, error) {
	name := r.ReadString()
	count := r.readCount()
	fields := make(structTypeFields, count)

	t := newType(StructDesc{name, fields})
	seenStructs[name] = t

	for i := uint64(0); i < count; i++ {
		t.Desc.(StructDesc).fields[i] = StructField{
			Name: r.ReadString(),
		}
	}
	for i := uint64(0); i < count; i++ {
		inType, err := r.readTypeInner(seenStructs)

		if err != nil {
			return nil, err
		}

		t.Desc.(StructDesc).fields[i].Type = inType
	}
	for i := uint64(0); i < count; i++ {
		t.Desc.(StructDesc).fields[i].Optional = r.ReadBool()
	}

	return t, nil
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

func (r *typedBinaryNomsReader) readUnionType(seenStructs map[string]*Type) (*Type, error) {
	l := r.readCount()
	ts := make(typeSlice, l)
	for i := uint64(0); i < l; i++ {
		t, err := r.readTypeInner(seenStructs)

		if err != nil {
			return nil, err
		}

		ts[i] = t
	}
	return makeUnionType(ts...)
}

func (r *typedBinaryNomsReader) skipUnionType() {
	l := r.readCount()
	for i := uint64(0); i < l; i++ {
		r.skipTypeInner()
	}
}
