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

package diff

import (
	"context"
	"errors"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

type textValueStore struct {
	val.ValueStore
	values map[hash.Hash][]byte
	reads  int
	err    error
}

// ReadBytes returns the stored text and records each read.
func (s *textValueStore) ReadBytes(_ context.Context, addr hash.Hash) ([]byte, error) {
	s.reads++
	return s.values[addr], s.err
}

func TestValuesEqual(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/12262
	ctx := sql.NewEmptyContext()
	lower := []byte("a")
	upper := []byte("A")
	lowerHash, upperHash := hash.Of(lower), hash.Of(upper)

	t.Run("out-of-band text with equal content addresses is equal without reads", func(t *testing.T) {
		store := &textValueStore{err: errors.New("unexpected read")}
		from := val.NewTextStorage(lowerHash, store)
		to := val.NewTextStorage(lowerHash, store)
		equal, err := ValuesEqual(ctx, types.Text, from, to)
		require.NoError(t, err)
		require.True(t, equal)
		require.Zero(t, store.reads)
	})

	t.Run("out-of-band text with different content addresses loads both values to compare", func(t *testing.T) {
		store := &textValueStore{values: map[hash.Hash][]byte{lowerHash: lower, upperHash: upper}}
		from := val.NewTextStorage(lowerHash, store)
		to := val.NewTextStorage(upperHash, store)
		equal, err := ValuesEqual(ctx, types.Text, from, to)
		require.NoError(t, err)
		require.False(t, equal)
		require.Equal(t, 2, store.reads)
	})

	t.Run("out-of-band text with different content addresses propagates read errors", func(t *testing.T) {
		readErr := errors.New("read failed")
		store := &textValueStore{err: readErr}
		from := val.NewTextStorage(lowerHash, store)
		to := val.NewTextStorage(upperHash, store)
		_, err := ValuesEqual(ctx, types.Text, from, to)
		require.ErrorIs(t, err, readErr)
	})

	t.Run("out-of-band binary with equal content addresses is equal without reads", func(t *testing.T) {
		store := &textValueStore{err: errors.New("unexpected read")}
		from := val.NewByteArray(lowerHash, store)
		to := val.NewByteArray(lowerHash, store)
		equal, err := ValuesEqual(ctx, types.Blob, from, to)
		require.NoError(t, err)
		require.True(t, equal)
		require.Zero(t, store.reads)
	})

	t.Run("out-of-band json with equal content addresses is equal without reads", func(t *testing.T) {
		store := &textValueStore{err: errors.New("unexpected read")}
		addr := hash.Of([]byte(`{"a":1}`))
		from := val.NewJsonStorageOutOfBand(addr, store, 7)
		to := val.NewJsonStorageOutOfBand(addr, store, 7)
		equal, err := ValuesEqual(ctx, types.JSON, from, to)
		require.NoError(t, err)
		require.True(t, equal)
		require.Zero(t, store.reads)
	})

	t.Run("out-of-band geometry with equal content addresses is equal without reads", func(t *testing.T) {
		store := &textValueStore{err: errors.New("unexpected read")}
		from := val.NewGeometryStorageOutOfBand(lowerHash, store, 1)
		to := val.NewGeometryStorageOutOfBand(lowerHash, store, 1)
		equal, err := ValuesEqual(ctx, types.GeometryType{}, from, to)
		require.NoError(t, err)
		require.True(t, equal)
		require.Zero(t, store.reads)
	})

	t.Run("different storage types do not match by content address", func(t *testing.T) {
		store := &textValueStore{err: errors.New("read failed")}
		from := val.NewTextStorage(lowerHash, store)
		to := val.NewByteArray(lowerHash, store)
		_, err := ValuesEqual(ctx, types.Blob, from, to)
		require.Error(t, err)
	})

	t.Run("text with empty content addresses compares values instead of matching", func(t *testing.T) {
		store := &textValueStore{}
		from := val.NewTextStorage(hash.Hash{}, store)
		to := val.NewTextStorage(hash.Hash{}, store)
		equal, err := ValuesEqual(ctx, types.Text, from, to)
		require.NoError(t, err)
		require.True(t, equal)
		require.NotNil(t, from.Buf)
		require.NotNil(t, to.Buf)
	})

	t.Run("out-of-band json numbers with different content addresses compare equal by value", func(t *testing.T) {
		one := []byte(`1`)
		oneDecimal := []byte(`1.0`)
		oneHash, decimalHash := hash.Of(one), hash.Of(oneDecimal)
		store := &textValueStore{values: map[hash.Hash][]byte{oneHash: one, decimalHash: oneDecimal}}
		from := val.NewJsonStorageOutOfBand(oneHash, store, int64(len(one)))
		to := val.NewJsonStorageOutOfBand(decimalHash, store, int64(len(oneDecimal)))
		equal, err := ValuesEqual(ctx, types.JSON, from, to)
		require.NoError(t, err)
		require.True(t, equal)
		require.Equal(t, 2, store.reads)
	})
}
