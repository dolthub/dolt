// Copyright 2021 Dolthub, Inc.
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
	"bytes"
	"context"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/mohae/uvarint"

	"github.com/dolthub/dolt/go/store/hash"
)

// A AdaptiveValue is a byte sequence that can represent:
// - An inlined string or bytes value
// - An address to a string or bytes value
// - NULL
//
// NULL is represented by an empty byte sequence. Otherwise, the encoding is as follows:
//
//   - Inlined:
//     +------------+-------------------------+
//     | 0 (1 byte) | inlined string or bytes |
//     +------------+-------------------------+
//
//   - Addressed:
//     +-----------------------------------------------------------------+--------------------+
//     | size of addressed string or bytes (SQLite4 variable-length int) | address (20 bytes) |
//     +-----------------------------------------------------------------+--------------------+
//
// See: https://sqlite.org/src4/doc/trunk/www/varint.wiki
//
// We only store an address when if the address is shorter than the string.
// Since addresses are always 20 bytes, the size is always greater than 20 when storing an address.
// Thus we can always distinguish representations by the first byte.
type AdaptiveValue []byte

// getMessageLength returns the length of the underlying value.
func (v AdaptiveValue) getMessageLength() int64 {
	if v.IsNull() {
		return 0
	}
	if v.isInlined() {
		return int64(len(v)) - 1
	}
	length, _ := uvarint.Uvarint(v)
	return int64(length)
}

// outOfBandSize computes the size of the value if it were stored out of band.
func (v AdaptiveValue) outOfBandSize() int64 {
	if v.IsOutOfBand() {
		return int64(len(v))
	}
	_, lengthSize := uvarint.Uvarint(v)
	return int64(lengthSize) + hash.ByteLen // variable length + address
}

// inlineSize computes the size of the value if it were inlined.
func (v AdaptiveValue) inlineSize() int64 {
	if v.isInlined() {
		return int64(len(v))
	}
	blobLength := v.getMessageLength()
	return 1 + blobLength // header + message
}

// IsNull returns whether this AdaptiveValue represents a NULL value.
func (v AdaptiveValue) IsNull() bool {
	return len(v) == 0
}

// IsOutOfBand returns whether this AdaptiveValue represents a addressable value stored out-of-band.
func (v AdaptiveValue) IsOutOfBand() bool {
	if v.IsNull() {
		return false
	}
	return v[0] != 0
}

var maxVarIntLength ByteSize = 9
var maxOutOfBandAdaptiveValueLength = maxVarIntLength + hash.ByteLen

func makeVarInt(x uint64, dest []byte) (bytesWritten int, output []byte) {
	if dest == nil {
		dest = make([]byte, maxVarIntLength)
	}
	length := uvarint.Encode(dest, x)
	return length, dest[:length]
}

// If a conversion is necessary, the converted value will be copied into `dest`. This is a performance
// optimization when there is a pre-allocated buffer.
func (v AdaptiveValue) convertToOutOfBand(ctx context.Context, vs ValueStore, dest []byte) (AdaptiveValue, error) {
	if v.IsOutOfBand() {
		return v, nil
	}
	maxSize := hash.ByteLen + maxVarIntLength
	if cap(dest) < int(maxOutOfBandAdaptiveValueLength) {
		dest = make([]byte, maxSize)
	}
	blob := v[1:]
	blobLength := uint64(len(blob))
	lengthSize, dest := makeVarInt(blobLength, dest)
	blobHash, err := vs.WriteBytes(ctx, blob)
	if err != nil {
		return nil, err
	}

	dest = append(dest[:lengthSize], blobHash[:]...)
	return dest, nil
}

// isInlined returns whether this AdaptiveValue represents an inlined value.
func (v AdaptiveValue) isInlined() bool {
	if v.IsNull() {
		return false
	}
	return v[0] == 0
}

// If a conversion is necessary, the converted value will be written into `dest`. This is a performance
// optimization when there is a pre-allocated buffer.
func (v AdaptiveValue) convertToInline(ctx context.Context, vs ValueStore, dest []byte) (AdaptiveValue, error) {
	if v.isInlined() {
		return v, nil
	}
	_, lengthBytes := uvarint.Uvarint(v)
	addr := v[lengthBytes:]
	blob, err := vs.ReadBytes(ctx, hash.New(addr))
	if err != nil {
		return nil, err
	}
	outputSize := 1 + len(blob)
	if cap(dest) < outputSize {
		dest = make([]byte, outputSize)
	}
	dest = dest[:1]
	dest[0] = 0
	dest = append(dest, blob...)
	return dest, nil
}

// getUnderlyingBytes extracts the underlying value that this AdaptiveValue represents.
func (v AdaptiveValue) getUnderlyingBytes(ctx context.Context, vs ValueStore) ([]byte, error) {
	if v.IsNull() {
		return nil, nil
	}
	if v.isInlined() {
		return v[1:], nil
	}
	// else value is stored out-of-band
	_, lengthBytes := uvarint.Uvarint(v)
	addr := v[lengthBytes:]
	return vs.ReadBytes(ctx, hash.New(addr))
}

func (v AdaptiveValue) convertToByteArray(ctx context.Context, vs ValueStore, buf []byte) (*ByteArray, error) {
	// Only out-of-band values can be converted to a ByteArray
	outOfBandValue, err := v.convertToOutOfBand(ctx, vs, buf)
	if err != nil {
		return &ByteArray{}, err
	}
	length, lengthBytes := uvarint.Uvarint(outOfBandValue)
	address := hash.New(outOfBandValue[lengthBytes:])
	return NewByteArray(address, vs).WithMaxByteLength(int64(length)), nil
}

func (v AdaptiveValue) convertToTextStorage(ctx context.Context, vs ValueStore, buf []byte) (*TextStorage, error) {
	// Only out-of-band values can be converted to a TextStorage
	outOfBandValue, err := v.convertToOutOfBand(ctx, vs, buf)
	if err != nil {
		return &TextStorage{}, err
	}
	length, lengthBytes := uvarint.Uvarint(outOfBandValue)
	address := hash.New(outOfBandValue[lengthBytes:])
	return NewTextStorage(address, vs).WithMaxByteLength(int64(length)), nil
}

// AdaptiveEncodingTypeHandler is an implementation of TypeHandler for adaptive encoding types,
// that is, values that can be either a content-address or an inline value.
// This TypeHandler converts between the address and the underlying value as needed, allowing these columns
// to be used in contexts that need access to the underlying value, such as in primary indexes.
// The |childHandler| field allows this behavior to be composed with other type handlers.
type AdaptiveEncodingTypeHandler struct {
	vs           ValueStore
	childHandler TupleTypeHandler
}

func NewAdaptiveTypeHandler(vs ValueStore, childHandler TupleTypeHandler) AdaptiveEncodingTypeHandler {
	return AdaptiveEncodingTypeHandler{
		vs:           vs,
		childHandler: childHandler,
	}
}

func (handler AdaptiveEncodingTypeHandler) SerializedCompare(ctx context.Context, v1 []byte, v2 []byte) (int, error) {
	// order NULLs first
	if v1 == nil || v2 == nil {
		if bytes.Equal(v1, v2) {
			return 0, nil
		} else if v1 == nil {
			return -1, nil
		} else {
			return 1, nil
		}
	}
	adaptiveValue1 := AdaptiveValue(v1)
	adaptiveValue2 := AdaptiveValue(v2)
	// Fast-path: two out-of-band values with equal hashes are equal.
	if adaptiveValue1.IsOutOfBand() && adaptiveValue2.IsOutOfBand() && bytes.Equal(adaptiveValue1, adaptiveValue2) {
		return 0, nil
	}
	var err error
	if adaptiveValue1.IsOutOfBand() {
		adaptiveValue1, err = adaptiveValue1.convertToInline(ctx, handler.vs, nil)
		if err != nil {
			return 0, err
		}
	}
	if adaptiveValue2.IsOutOfBand() {
		adaptiveValue1, err = adaptiveValue2.convertToInline(ctx, handler.vs, nil)
		if err != nil {
			return 0, err
		}
	}
	return handler.childHandler.SerializedCompare(ctx, adaptiveValue1[1:], adaptiveValue2[1:])
}

func (handler AdaptiveEncodingTypeHandler) SerializeValue(ctx context.Context, val any) ([]byte, error) {
	b, err := handler.childHandler.SerializeValue(ctx, val)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, nil
	}
	// Initially create an inline version of the value. If subsequently written to a tuple, this may get replaced
	// with an out-of-band version.
	dest := make([]byte, len(b)+1)
	copy(dest[1:], b)
	return dest, nil
}

func (handler AdaptiveEncodingTypeHandler) DeserializeValue(ctx context.Context, val []byte) (any, error) {
	adaptiveValue := AdaptiveValue(val)
	if adaptiveValue.IsNull() {
		return nil, nil
	}
	if adaptiveValue.isInlined() {
		return handler.childHandler.DeserializeValue(ctx, adaptiveValue[1:])
	}
	// else adaptiveValue is stored out-of-band
	length, lengthBytes := uvarint.Uvarint(adaptiveValue)
	addr := hash.New(adaptiveValue[lengthBytes:])
	return &ExtendedValueWrapper{
		ImmutableValue:  NewImmutableValue(addr, handler.vs),
		outOfBandLength: int64(length),
		typeHandler:     handler.childHandler,
	}, nil
}

func (handler AdaptiveEncodingTypeHandler) FormatValue(val any) (string, error) {
	return handler.childHandler.FormatValue(val)
}

type ExtendedValueWrapper struct {
	ImmutableValue
	outOfBandLength int64
	typeHandler     TupleTypeHandler
}

func (e *ExtendedValueWrapper) Unwrap(ctx context.Context) (result string, err error) {
	b, err := e.UnwrapAny(ctx)
	if err != nil {
		return "", err
	}
	return b.(string), nil
}

func (e *ExtendedValueWrapper) UnwrapAny(ctx context.Context) (interface{}, error) {
	if e.ImmutableValue.Buf == nil {
		buf, err := e.vs.ReadBytes(ctx, e.ImmutableValue.Addr)
		if err != nil {
			return nil, err
		}
		e.ImmutableValue.Buf = buf
	}
	return e.typeHandler.DeserializeValue(ctx, e.ImmutableValue.Buf)
}

func (e ExtendedValueWrapper) IsExactLength() bool {
	return true
}

func (e ExtendedValueWrapper) MaxByteLength() int64 {
	return e.outOfBandLength
}

func (e ExtendedValueWrapper) Compare(ctx context.Context, other interface{}) (cmp int, comparable bool, err error) {
	//TODO implement me
	panic("implement me")
}

func (e ExtendedValueWrapper) Hash() interface{} {
	return e.ImmutableValue.Addr
}

var _ sql.Wrapper[string] = &ExtendedValueWrapper{}
