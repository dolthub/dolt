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
	"encoding/json"
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
	// ReadBytes reads the bytes associated with the given content hash
	ReadBytes(ctx context.Context, h hash.Hash) ([]byte, error)
	// WriteBytes writes the given bytes and returns the content hash
	WriteBytes(ctx context.Context, val []byte) (hash.Hash, error)
	// CompareAdaptive compares two adaptive values.
	CompareAdaptive(ctx context.Context, l AdaptiveValue, r AdaptiveValue, encoding Encoding) (int, error)
	// CompareAdaptiveCollatedStrings compares two adaptive string values with the given collation.
	CompareAdaptiveCollatedStrings(ctx context.Context, l, r AdaptiveValue, collation sql.CollationID) (int, error)
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

// TextStorage is a sql.AnyWrapper to wrap large string values stored out of band.
type TextStorage struct {
	ImmutableValue
	maxByteLength int64
}

var _ sql.StringWrapper = &TextStorage{}

// NewTextStorage creates a new TextStorage with the given content address and ValueStore.
func NewTextStorage(addr hash.Hash, vs ValueStore) *TextStorage {
	return &TextStorage{
		ImmutableValue: NewImmutableValue(addr, vs),
		maxByteLength:  types.LongText.MaxByteLength(),
	}
}

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

// UnwrapAny implements sql.AnyWrapper by unwrapping to a string.
func (t *TextStorage) UnwrapAny(ctx context.Context) (interface{}, error) {
	return t.Unwrap(ctx)
}

// WithMaxByteLength returns a copy of TextStorage with the given max byte length.
func (t *TextStorage) WithMaxByteLength(maxByteLength int64) *TextStorage {
	return &TextStorage{
		ImmutableValue: NewImmutableValue(t.Addr, t.vs),
		maxByteLength:  maxByteLength,
	}
}

// Compare implements sql.AnyWrapper. Two TextStorage values with the same out-of-band address are equal.
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

// Value implements driver.Valuer for interoperability with other go libraries
func (t *TextStorage) ValueContext(ctx context.Context) (driver.Value, error) {
	buf, err := t.GetBytes(ctx)
	if err != nil {
		return "", err
	}
	return *(*string)(unsafe.Pointer(&buf)), nil
}

// ByteArray is a sql.AnyWrapper to wrap large byte array values stored out of band.
type ByteArray struct {
	ImmutableValue
	maxByteLength int64
}

var _ sql.BytesWrapper = &ByteArray{}

// NewByteArray creates a new ByteArray with the given content address and ValueStore.
func NewByteArray(addr hash.Hash, vs ValueStore) *ByteArray {
	return &ByteArray{
		ImmutableValue: NewImmutableValue(addr, vs),
		maxByteLength:  types.LongBlob.MaxByteLength(),
	}
}

// IsExactLength implements sql.BytesWrapper
func (b *ByteArray) IsExactLength() bool {
	return false
}

// MaxByteLength implements sql.BytesWrapper
func (b *ByteArray) MaxByteLength() int64 {
	return b.maxByteLength
}

// Compare implements sql.AnyWrapper. Two ByteArray values with the same out-of-band address are equal.
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

// ToBytes implements sql.BytesWrapper by loading the bytes from storage.
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

// ToString implements sql.AnyWrapper by loading the bytes from storage and converting to a string.
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

// WithMaxByteLength returns a copy of ByteArray with the given max byte length.
func (b *ByteArray) WithMaxByteLength(maxByteLength int64) *ByteArray {
	return &ByteArray{
		ImmutableValue: b.ImmutableValue,
		maxByteLength:  maxByteLength,
	}
}

func (b *ByteArray) ValueContext(ctx context.Context) (driver.Value, error) {
	return b.GetBytes(ctx)
}

// GeometryStorage is a sql.AnyWrapper for geometry values.
type GeometryStorage struct {
	// outOfBand holds a lazily-loaded out-of-band value. Only used when inlineBytes is nil.
	outOfBand     ImmutableValue
	maxByteLength int64
}

var _ sql.AnyWrapper = &GeometryStorage{}

// NewGeometryStorageOutOfBand creates a GeometryStorage that lazily loads bytes from a content-addressed store.
func NewGeometryStorageOutOfBand(addr hash.Hash, vs ValueStore, maxByteLength int64) *GeometryStorage {
	return &GeometryStorage{
		outOfBand:     NewImmutableValue(addr, vs),
		maxByteLength: maxByteLength,
	}
}

// Unwrap implements sql.Wrapper by loading the bytes from storage and returning them.
func (g *GeometryStorage) Unwrap(ctx context.Context) (result []byte, err error) {
	return g.GetSerializedBytes(ctx)
}

// GetSerializedBytes returns the raw serialized geometry bytes, loading from storage if necessary.
func (g *GeometryStorage) GetSerializedBytes(ctx context.Context) ([]byte, error) {
	return g.outOfBand.GetBytes(ctx)
}

// UnwrapAny implements sql.AnyWrapper by loading bytes and deserializing to a types.GeometryValue.
func (g *GeometryStorage) UnwrapAny(ctx context.Context) (interface{}, error) {
	buf, err := g.GetSerializedBytes(ctx)
	if err != nil {
		return nil, err
	}
	return deserializeGeometryBytes(buf)
}

// IsExactLength implements sql.AnyWrapper.
func (g *GeometryStorage) IsExactLength() bool {
	return true
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
	if g.outOfBand.Addr == otherGeom.outOfBand.Addr {
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

// JsonAdaptiveStorage wraps raw JSON bytes and defers deserialization until the value is needed.
// The bytes may be stored inline or out-of-band via a content address.
// It implements sql.JSONWrapper and types.JSONBytes.
type JsonAdaptiveStorage struct {
	outOfBand     ImmutableValue
	maxByteLength int64
}

var _ sql.JSONWrapper = &JsonAdaptiveStorage{}
var _ sql.AnyWrapper = &JsonAdaptiveStorage{}
var _ types.JSONBytes = &JsonAdaptiveStorage{}

// NewJsonStorageOutOfBand creates a JsonAdaptiveStorage that lazily loads bytes from a content-addressed store.
func NewJsonStorageOutOfBand(addr hash.Hash, vs ValueStore, maxByteLength int64) *JsonAdaptiveStorage {
	return &JsonAdaptiveStorage{
		outOfBand:     NewImmutableValue(addr, vs),
		maxByteLength: maxByteLength,
	}
}

// GetBytes implements types.JSONBytes by returning the raw JSON bytes.
func (j *JsonAdaptiveStorage) GetBytes(ctx context.Context) ([]byte, error) {
	return j.outOfBand.GetBytes(ctx)
}

// Clone implements sql.JSONWrapper.
func (j *JsonAdaptiveStorage) Clone(_ context.Context) sql.JSONWrapper {
	return &JsonAdaptiveStorage{
		outOfBand:     j.outOfBand,
		maxByteLength: j.maxByteLength,
	}
}

// ToInterface implements sql.JSONWrapper by deserializing the raw JSON bytes.
func (j *JsonAdaptiveStorage) ToInterface(ctx context.Context) (interface{}, error) {
	buf, err := j.GetBytes(ctx)
	if err != nil {
		return nil, err
	}
	var val interface{}
	if err = json.Unmarshal(buf, &val); err != nil {
		return nil, err
	}
	return val, nil
}

// IsExactLength returns true when the bytes are stored inline (exact length known without loading).
func (j *JsonAdaptiveStorage) IsExactLength() bool {
	return true
}

// MaxByteLength returns the maximum byte length of the JSON data.
func (j *JsonAdaptiveStorage) MaxByteLength() int64 {
	return j.maxByteLength
}

// Addr returns the content address for out-of-band storage. Only valid when IsExactLength returns false.
func (j *JsonAdaptiveStorage) Addr() hash.Hash {
	return j.outOfBand.Addr
}

// UnwrapAny implements sql.AnyWrapper by loading bytes and deserializing to an interface{}.
func (j *JsonAdaptiveStorage) UnwrapAny(ctx context.Context) (interface{}, error) {
	return j.ToInterface(ctx)
}

// Compare implements sql.AnyWrapper. Two JsonAdaptiveStorage values with the same out-of-band address are equal.
func (j *JsonAdaptiveStorage) Compare(ctx context.Context, other interface{}) (cmp int, comparable bool, err error) {
	otherJson, ok := other.(*JsonAdaptiveStorage)
	if !ok {
		return 0, false, nil
	}
	if j.outOfBand.Addr == otherJson.outOfBand.Addr {
		return 0, true, nil
	}
	return 0, false, nil
}

// Hash implements sql.AnyWrapper by returning the content address for out-of-band storage, or nil for inline storage.
func (j *JsonAdaptiveStorage) Hash() interface{} {
	return j.outOfBand.Addr
}

// Unwrap implements sql.JSONWrapper by loading the bytes and returning them.
func (j *JsonAdaptiveStorage) Unwrap(ctx context.Context) (result []byte, err error) {
	return j.GetBytes(ctx)
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
