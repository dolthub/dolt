// Copyright 2025 Dolthub, Inc.
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

package val

import (
	"context"
	"database/sql/driver"
	"fmt"
	"unsafe"

	"github.com/dolthub/dolt/go/store/hash"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

const BytePeekLength = 128

// ValueStore is an interface for a key-value store that can store byte sequences, keyed by a content hash.
// The only implementation is tree.NodeStore, but ValueStore can be used without depending on the tree package.
// This is useful for type handlers.
type ValueStore interface {
	ReadBytes(ctx context.Context, h hash.Hash) ([]byte, error)
	WriteBytes(ctx context.Context, val []byte) (hash.Hash, error)
}

// ImmutableValue represents a content-addressed value stored in a ValueStore.
// The contents are loaded lazily and stored in |Buf|
type ImmutableValue struct {
	vs   ValueStore
	Buf  []byte
	Addr hash.Hash
}

func NewImmutableValue(addr hash.Hash, vs ValueStore) ImmutableValue {
	return ImmutableValue{Addr: addr, vs: vs}
}

func (t *ImmutableValue) GetBytes(ctx context.Context) ([]byte, error) {
	if t.Buf == nil {
		if t.Addr.IsEmpty() {
			t.Buf = []byte{}
			return t.Buf, nil
		}
		buf, err := t.vs.ReadBytes(ctx, t.Addr)
		if err != nil {
			return nil, err
		}
		t.Buf = buf
	}
	return t.Buf, nil
}

type TextStorage struct {
	ImmutableValue
	// ctx is a context that can be used in driver.Value
	// Storing a context in a struct is bad practice, so this field should not be used for any other purpose.
	ctx           context.Context
	maxByteLength int64
}

var _ sql.StringWrapper = &TextStorage{}
var _ driver.Valuer = &TextStorage{}

// IsExactLength implements sql.BytesWrapper
func (t *TextStorage) IsExactLength() bool {
	return false
}

// MaxByteLength implements sql.BytesWrapper
func (t *TextStorage) MaxByteLength() int64 {
	return t.maxByteLength
}

// Unwrap implements sql.BytesWrapper
func (t *TextStorage) Unwrap(ctx context.Context) (string, error) {
	buf, err := t.GetBytes(ctx)
	if err != nil {
		return "", err
	}
	return *(*string)(unsafe.Pointer(&buf)), nil
}

func (t *TextStorage) UnwrapAny(ctx context.Context) (interface{}, error) {
	return t.Unwrap(ctx)
}

func (t *TextStorage) WithMaxByteLength(maxByteLength int64) *TextStorage {
	return &TextStorage{
		ImmutableValue: NewImmutableValue(t.Addr, t.vs),
		maxByteLength:  maxByteLength,
	}
}

func (t *TextStorage) Compare(ctx context.Context, other interface{}) (cmp int, comparable bool, err error) {
	otherTextStorage, ok := other.(TextStorage)
	if !ok {
		return 0, false, nil
	}
	if otherTextStorage.Addr == t.Addr {
		return 0, true, nil
	}
	return 0, false, nil
}

// Hash implements sql.AnyWrapper by returning the Dolt hash.
func (t *TextStorage) Hash() interface{} {
	return t.Addr
}

func NewTextStorage(ctx context.Context, addr hash.Hash, vs ValueStore) *TextStorage {
	return &TextStorage{
		ImmutableValue: NewImmutableValue(addr, vs),
		maxByteLength:  types.LongText.MaxByteLength(),
		ctx:            ctx,
	}
}

// Value implements driver.Valuer for interoperability with other go libraries
func (t *TextStorage) Value() (driver.Value, error) {
	buf, err := t.GetBytes(t.ctx)
	if err != nil {
		return "", err
	}
	return *(*string)(unsafe.Pointer(&buf)), nil
}

type ByteArray struct {
	ImmutableValue
	// ctx is a context that can be used in driver.Value
	// Storing a context in a struct is bad practice, so this field should not be used for any other purpose.
	ctx           context.Context
	maxByteLength int64
}

var _ sql.BytesWrapper = &ByteArray{}
var _ driver.Valuer = &ByteArray{}

// IsExactLength implements sql.BytesWrapper
func (b *ByteArray) IsExactLength() bool {
	return false
}

// MaxByteLength implements sql.BytesWrapper
func (b *ByteArray) MaxByteLength() int64 {
	return b.maxByteLength
}

func (b *ByteArray) Compare(ctx context.Context, other interface{}) (cmp int, comparable bool, err error) {
	otherByteArray, ok := other.(ByteArray)
	if !ok {
		return 0, false, nil
	}
	if otherByteArray.Addr == b.Addr {
		return 0, true, nil
	}
	return 0, false, nil
}

func NewByteArray(ctx context.Context, addr hash.Hash, vs ValueStore) *ByteArray {
	return &ByteArray{
		ImmutableValue: NewImmutableValue(addr, vs),
		maxByteLength:  types.LongBlob.MaxByteLength(),
		ctx:            ctx,
	}
}

func (b *ByteArray) ToBytes(ctx context.Context) ([]byte, error) {
	return b.GetBytes(ctx)
}

// UnwrapAny implements sql.Wrapper
func (b *ByteArray) UnwrapAny(ctx context.Context) (interface{}, error) {
	return b.ToBytes(ctx)
}

// Unwrap implements sql.BytesWrapper
func (b *ByteArray) Unwrap(ctx context.Context) ([]byte, error) {
	return b.GetBytes(ctx)
}

func (b *ByteArray) ToString(ctx context.Context) (string, error) {
	buf, err := b.ToBytes(ctx)
	if err != nil {
		return "", err
	}
	toShow := BytePeekLength
	if len(buf) < toShow {
		toShow = len(buf)
	}
	res := buf[:toShow]
	return *(*string)(unsafe.Pointer(&res)), nil
}

// Hash implements sql.AnyWrapper by returning the Dolt hash.
func (b *ByteArray) Hash() interface{} {
	return b.Addr
}

func (b *ByteArray) WithMaxByteLength(maxByteLength int64) *ByteArray {
	return &ByteArray{
		ImmutableValue: b.ImmutableValue,
		maxByteLength:  maxByteLength,
	}
}

// Value implements driver.Valuer for interoperability with other go libraries
func (b *ByteArray) Value() (driver.Value, error) {
	return b.GetBytes(b.ctx)
}

// GeometryStorage wraps serialized geometry bytes and defers deserialization until the value is needed.
// The geometry bytes may be stored inline or out-of-band via a content address.
type GeometryStorage struct {
	// inlineBytes holds the serialized geometry bytes when the value is stored inline.
	// When nil, the value is out-of-band and must be loaded via ImmutableValue.
	inlineBytes []byte
	// outOfBand holds a lazily-loaded out-of-band value. Only used when inlineBytes is nil.
	outOfBand     ImmutableValue
	maxByteLength int64
}

var _ sql.AnyWrapper = &GeometryStorage{}

// NewGeometryStorageInline creates a GeometryStorage from inline serialized bytes.
func NewGeometryStorageInline(buf []byte) *GeometryStorage {
	return &GeometryStorage{
		inlineBytes:   buf,
		maxByteLength: int64(len(buf)),
	}
}

// NewGeometryStorageOutOfBand creates a GeometryStorage that lazily loads bytes from a content-addressed store.
func NewGeometryStorageOutOfBand(ctx context.Context, addr hash.Hash, vs ValueStore, maxByteLength int64) *GeometryStorage {
	return &GeometryStorage{
		outOfBand:     NewImmutableValue(addr, vs),
		maxByteLength: maxByteLength,
	}
}

// GetSerializedBytes returns the raw serialized geometry bytes, loading from storage if necessary.
func (g *GeometryStorage) GetSerializedBytes(ctx context.Context) ([]byte, error) {
	if g.inlineBytes != nil {
		return g.inlineBytes, nil
	}
	return g.outOfBand.GetBytes(ctx)
}

// ToGeometry deserializes the stored bytes into a types.GeometryValue.
func (g *GeometryStorage) ToGeometry(ctx context.Context) (types.GeometryValue, error) {
	buf, err := g.GetSerializedBytes(ctx)
	if err != nil {
		return nil, err
	}
	return deserializeGeometryBytes(buf)
}

// UnwrapAny implements sql.AnyWrapper by deserializing the geometry value.
func (g *GeometryStorage) UnwrapAny(ctx context.Context) (interface{}, error) {
	return g.ToGeometry(ctx)
}

// IsExactLength implements sql.AnyWrapper.
func (g *GeometryStorage) IsExactLength() bool {
	return g.inlineBytes != nil
}

// MaxByteLength implements sql.AnyWrapper.
func (g *GeometryStorage) MaxByteLength() int64 {
	return g.maxByteLength
}

// Compare implements sql.AnyWrapper. Two GeometryStorage values with the same out-of-band address are equal.
func (g *GeometryStorage) Compare(ctx context.Context, other interface{}) (cmp int, comparable bool, err error) {
	otherGeom, ok := other.(*GeometryStorage)
	if !ok {
		return 0, false, nil
	}
	if g.inlineBytes == nil && otherGeom.inlineBytes == nil && g.outOfBand.Addr == otherGeom.outOfBand.Addr {
		return 0, true, nil
	}
	return 0, false, nil
}

// Hash implements sql.AnyWrapper.
func (g *GeometryStorage) Hash() interface{} {
	return g.outOfBand.Addr
}

// Addr returns the content address for out-of-band storage. Only valid when IsExactLength returns false.
func (g *GeometryStorage) Addr() hash.Hash {
	return g.outOfBand.Addr
}

// deserializeGeometryBytes converts raw serialized bytes into a types.GeometryValue.
func deserializeGeometryBytes(buf []byte) (types.GeometryValue, error) {
	srid, _, typ, err := types.DeserializeEWKBHeader(buf)
	if err != nil {
		return nil, err
	}
	buf = buf[types.EWKBHeaderSize:]
	switch typ {
	case types.WKBPointID:
		v, _, err := types.DeserializePoint(buf, false, srid)
		return v, err
	case types.WKBLineID:
		v, _, err := types.DeserializeLine(buf, false, srid)
		return v, err
	case types.WKBPolyID:
		v, _, err := types.DeserializePoly(buf, false, srid)
		return v, err
	case types.WKBMultiPointID:
		v, _, err := types.DeserializeMPoint(buf, false, srid)
		return v, err
	case types.WKBMultiLineID:
		v, _, err := types.DeserializeMLine(buf, false, srid)
		return v, err
	case types.WKBMultiPolyID:
		v, _, err := types.DeserializeMPoly(buf, false, srid)
		return v, err
	case types.WKBGeomCollID:
		v, _, err := types.DeserializeGeomColl(buf, false, srid)
		return v, err
	default:
		return nil, fmt.Errorf("unknown geometry type %d", typ)
	}
}
