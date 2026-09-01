// Copyright 2026 Dolthub, Inc.
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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
)

type mockTupleTypeHandler struct{}

func (m mockTupleTypeHandler) SerializedCompare(ctx context.Context, v1, v2 []byte) (int, error) {
	return 0, nil
}
func (m mockTupleTypeHandler) SerializeValue(ctx context.Context, val any) ([]byte, error) {
	return nil, nil
}
func (m mockTupleTypeHandler) DeserializeValue(ctx context.Context, val []byte) (any, error) {
	return string(val), nil
}
func (m mockTupleTypeHandler) FormatValue(val any) (string, error)                 { return "", nil }
func (m mockTupleTypeHandler) SerializationCompatible(other TupleTypeHandler) bool { return true }
func (m mockTupleTypeHandler) ConvertSerialized(ctx context.Context, other TupleTypeHandler, val []byte) ([]byte, error) {
	return val, nil
}

type mockValueStore struct{}

func (m *mockValueStore) ReadBytes(ctx context.Context, h hash.Hash) ([]byte, error) { return nil, nil }
func (m *mockValueStore) WriteBytes(ctx context.Context, val []byte) (hash.Hash, error) {
	return hash.Hash{1}, nil
}
func (m *mockValueStore) CompareAdaptive(ctx context.Context, l, r AdaptiveValue, encoding Encoding) (int, error) {
	return 0, nil
}
func (m *mockValueStore) CompareAdaptiveCollatedStrings(ctx context.Context, l, r AdaptiveValue, collation sql.CollationID) (int, error) {
	return 0, nil
}

func TestAdaptiveValueMalformedAddress(t *testing.T) {
	ctx := context.Background()
	vs := &mockValueStore{}
	buffPool := pool.NewBuffPool()
	handler := NewAdaptiveTypeHandler(vs, mockTupleTypeHandler{})
	stringDesc := NewTupleDescriptor(Type{Enc: StringAdaptiveEnc})

	testCases := []struct {
		name        string
		data        []byte
		expectedErr error
	}{
		{
			name:        "3-byte address tail",
			data:        []byte{0x15, 0x01, 0x02, 0x03},
			expectedErr: ErrInvalidAddressLen,
		},
		{
			name:        "19-byte address tail",
			data:        append([]byte{0x15}, make([]byte, 19)...),
			expectedErr: ErrInvalidAddressLen,
		},
		{
			name:        "21-byte address tail",
			data:        append([]byte{0x15}, make([]byte, 21)...),
			expectedErr: ErrInvalidAddressLen,
		},
		{
			name:        "truncated varint header",
			data:        []byte{241},
			expectedErr: ErrTruncatedVarint,
		},
	}

	decodeOps := []struct {
		name string
		call func(v AdaptiveValue) error
	}{
		{"OutOfBandAddr", func(v AdaptiveValue) error {
			_, err := v.OutOfBandAddr()
			return err
		}},
		{"convertToInline", func(v AdaptiveValue) error {
			_, err := v.convertToInline(ctx, vs, nil)
			return err
		}},
		{"getUnderlyingBytes", func(v AdaptiveValue) error {
			_, err := v.getUnderlyingBytes(ctx, vs)
			return err
		}},
		{"convertToByteArray", func(v AdaptiveValue) error {
			_, err := v.convertToByteArray(ctx, vs, nil)
			return err
		}},
		{"convertToTextStorage", func(v AdaptiveValue) error {
			_, err := v.convertToTextStorage(ctx, vs, nil)
			return err
		}},
		{"convertToGeometryStorage", func(v AdaptiveValue) error {
			_, err := v.convertToGeometryStorage(ctx, vs)
			return err
		}},
		{"convertToJsonStorage", func(v AdaptiveValue) error {
			_, err := v.convertToJsonStorage(ctx, vs)
			return err
		}},
		{"DeserializeValue", func(v AdaptiveValue) error {
			_, err := handler.DeserializeValue(ctx, v)
			return err
		}},
		{"GetBytesAdaptiveValue", func(v AdaptiveValue) error {
			_, _, err := GetBytesAdaptiveValue(ctx, vs, v)
			return err
		}},
		{"GetStringAdaptiveValue", func(v AdaptiveValue) error {
			_, _, err := stringDesc.GetStringAdaptiveValue(ctx, 0, vs, NewTuple(buffPool, v))
			return err
		}},
		{"GetJsonAdaptiveValue", func(v AdaptiveValue) error {
			_, _, err := GetJsonAdaptiveValue(ctx, vs, v)
			return err
		}},
		{"getGeomAdaptiveValue", func(v AdaptiveValue) error {
			_, _, err := getGeomAdaptiveValue(ctx, vs, v)
			return err
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			v := AdaptiveValue(tc.data)
			require.False(t, v.IsNull())
			require.True(t, v.IsOutOfBand())

			for _, op := range decodeOps {
				t.Run(op.name, func(t *testing.T) {
					err := op.call(v)
					assert.ErrorIs(t, err, tc.expectedErr)
				})
			}
		})
	}
}

func TestAdaptiveValueOutOfBandAddrInvalidCases(t *testing.T) {
	var nullVal AdaptiveValue
	addr, err := nullVal.OutOfBandAddr()
	assert.ErrorIs(t, err, ErrNullAdaptiveValue)
	assert.True(t, addr.IsEmpty())

	inlineVal := AdaptiveValue([]byte{0x00, 0x01, 0x02})
	addr, err = inlineVal.OutOfBandAddr()
	assert.ErrorIs(t, err, ErrInlineAdaptiveValue)
	assert.True(t, addr.IsEmpty())
}

func TestAdaptiveValueOutOfBandAddrValid(t *testing.T) {
	ctx := context.Background()
	vs := &mockValueStore{}
	v, err := NewOutOfBandAdaptiveValue(ctx, vs, []byte("valid content"))
	require.NoError(t, err)
	addr, err := v.OutOfBandAddr()
	require.NoError(t, err)
	assert.False(t, addr.IsEmpty())
}
