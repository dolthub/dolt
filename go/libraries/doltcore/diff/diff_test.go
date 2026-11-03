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

	t.Run("equal text addresses avoid reads", func(t *testing.T) {
		store := &textValueStore{err: errors.New("unexpected read")}
		from := val.NewTextStorage(lowerHash, store)
		to := val.NewTextStorage(lowerHash, store)
		equal, err := ValuesEqual(ctx, types.Text, from, to)
		require.NoError(t, err)
		require.True(t, equal)
		require.Zero(t, store.reads)
	})

	t.Run("different text addresses compare contents", func(t *testing.T) {
		store := &textValueStore{values: map[hash.Hash][]byte{lowerHash: lower, upperHash: upper}}
		from := val.NewTextStorage(lowerHash, store)
		to := val.NewTextStorage(upperHash, store)
		equal, err := ValuesEqual(ctx, types.Text, from, to)
		require.NoError(t, err)
		require.False(t, equal)
		require.Equal(t, 2, store.reads)
	})

	t.Run("different text addresses propagate read errors", func(t *testing.T) {
		readErr := errors.New("read failed")
		store := &textValueStore{err: readErr}
		from := val.NewTextStorage(lowerHash, store)
		to := val.NewTextStorage(upperHash, store)
		_, err := ValuesEqual(ctx, types.Text, from, to)
		require.ErrorIs(t, err, readErr)
	})
}
