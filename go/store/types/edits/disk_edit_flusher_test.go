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

package edits

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestFlushingNoEdits(t *testing.T) {
	ctx := context.Background()
	ef := NewDiskEditFlusher(ctx, "", types.Format_Default, nil)
	eps, err := ef.Wait(ctx)
	require.NoError(t, err)
	require.Zero(t, len(eps))
}

func TestEditFlusher(t *testing.T) {
	const numEditors = 10
	ctx := context.Background()
	nbf := types.Format_Default
	vrw := types.NewMemoryValueStore()
	ef := NewDiskEditFlusher(ctx, os.TempDir(), nbf, vrw)
	for i := 0; i < numEditors; i++ {
		ea := types.NewDumbEditAccumulator(nbf)
		for j := 0; j < 100; j++ {
			k, err := types.NewTuple(nbf, types.Int(i))
			require.NoError(t, err)
			v, err := types.NewTuple(nbf, types.Int(i*100+j))
			require.NoError(t, err)
			ea.AddEdit(k, v)
		}

		ef.Flush(ea, uint64(i))
	}

	eps, err := ef.Wait(ctx)
	require.NoError(t, err)
	require.Len(t, eps, numEditors)

	for i := 0; i < numEditors; i++ {
		require.Equal(t, uint64(i), eps[i].ID)
		kvp, err := eps[i].Edits.Next()
		require.NoError(t, err)
		key, err := kvp.Key.Value(ctx)
		require.NoError(t, err)
		keyTuplvals, err := key.(types.Tuple).AsSlice()
		require.NoError(t, err)
		require.Equal(t, i, int(keyTuplvals[0].(types.Int)))
		err = eps[i].Edits.Close(ctx)
		require.NoError(t, err)
	}
}
