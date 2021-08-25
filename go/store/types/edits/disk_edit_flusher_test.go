package edits

import (
	"context"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
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
		key, err := kvp.Key.Value(ctx)
		require.NoError(t, err)
		keyTuplvals, err := key.(types.Tuple).AsSlice()
		require.NoError(t, err)
		require.Equal(t, i, int(keyTuplvals[0].(types.Int)))
		err = eps[i].Edits.Close(ctx)
		require.NoError(t, err)
	}
}