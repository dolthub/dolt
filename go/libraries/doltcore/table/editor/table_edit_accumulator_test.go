package editor

import (
	"context"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

var emptyTpl = types.EmptyTuple(types.Format_Default)

func newTestTEAF() *teaFactoryImpl {
	dir := os.TempDir()

	return &teaFactoryImpl{
		directory: dir,
		vrw:       types.NewMemoryValueStore(),
	}
}

func newTuple(t *testing.T, vals ...types.Value) types.Tuple {
	tpl, err := types.NewTuple(types.Format_Default, vals...)
	require.NoError(t, err)

	return tpl
}

func teaInsert(t *testing.T, tea TableEditAccumulator, key types.Tuple) {
	h, err := key.Hash(types.Format_Default)
	require.NoError(t, err)

	tea.Insert(h, key, emptyTpl)
}

func teaDelete(t *testing.T, tea TableEditAccumulator, key types.Tuple) {
	h, err := key.Hash(types.Format_Default)
	require.NoError(t, err)

	tea.Delete(h, key)
}

func requireGet(ctx context.Context, t *testing.T, tea TableEditAccumulator, key types.Tuple, expected bool) {
	h, err := key.Hash(types.Format_Default)
	require.NoError(t, err)
	_, has, err := tea.Get(ctx, h, key)
	require.NoError(t, err)
	require.Equal(t, expected, has)
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	nbf := types.Format_Default
	teaf := newTestTEAF()
	m, err := types.NewMap(ctx, teaf.vrw)
	require.NoError(t, err)
	tea := teaf.NewTEA(ctx, m).(*tableEditAccumulatorImpl)

	key1 := newTuple(t, types.Int(1))
	key2 := newTuple(t, types.Int(2))
	key3 := newTuple(t, types.Int(3))
	key4 := newTuple(t, types.Int(4))
	key5 := newTuple(t, types.Int(5))
	key6 := newTuple(t, types.Int(6))

	// test uncommitted
	requireGet(ctx, t, tea, key1, false)
	teaInsert(t, tea, key1)
	requireGet(ctx, t, tea, key1, true)
	err = tea.Rollback(ctx)
	require.NoError(t, err)
	requireGet(ctx, t, tea, key1, false)

	// test uncommitted flushed
	teaInsert(t, tea, key1)
	requireGet(ctx, t, tea, key1, true)
	tea.flushUncommitted()
	requireGet(ctx, t, tea, key1, true)
	err = tea.Rollback(ctx)
	require.NoError(t, err)
	requireGet(ctx, t, tea, key1, false)

	// test commmitted
	teaInsert(t, tea, key1)
	err = tea.Commit(ctx, nbf)
	require.NoError(t, err)
	requireGet(ctx, t, tea, key1, true)

	// edits in committed and uncommitted
	requireGet(ctx, t, tea, key2, false)
	teaInsert(t, tea, key2)
	requireGet(ctx, t, tea, key1, true)
	requireGet(ctx, t, tea, key2, true)
	err = tea.Rollback(ctx)
	require.NoError(t, err)
	requireGet(ctx, t, tea, key1, true)
	requireGet(ctx, t, tea, key2, false)

	// edits in committed and uncommitted flushed
	teaInsert(t, tea, key2)
	requireGet(ctx, t, tea, key1, true)
	requireGet(ctx, t, tea, key2, true)
	tea.flushUncommitted()
	requireGet(ctx, t, tea, key1, true)
	requireGet(ctx, t, tea, key2, true)
	err = tea.Rollback(ctx)
	require.NoError(t, err)
	requireGet(ctx, t, tea, key1, true)
	requireGet(ctx, t, tea, key2, false)

	// edits in committed, uncommitted and uncommitted flushed
	requireGet(ctx, t, tea, key3, false)
	teaInsert(t, tea, key2)
	tea.flushUncommitted()
	teaInsert(t, tea, key3)
	requireGet(ctx, t, tea, key1, true)
	requireGet(ctx, t, tea, key2, true)
	requireGet(ctx, t, tea, key3, true)
	err = tea.Rollback(ctx)
	require.NoError(t, err)
	requireGet(ctx, t, tea, key1, true)
	requireGet(ctx, t, tea, key2, false)
	requireGet(ctx, t, tea, key3, false)

	// edits everywhere materialized
	teaInsert(t, tea, key2)
	tea.flushUncommitted()
	teaInsert(t, tea, key3)
	requireGet(ctx, t, tea, key1, true)
	requireGet(ctx, t, tea, key2, true)
	requireGet(ctx, t, tea, key3, true)

	// edits in materialized data
	_, err = tea.MaterializeEdits(ctx, nbf)
	requireGet(ctx, t, tea, key1, true)
	requireGet(ctx, t, tea, key2, true)
	requireGet(ctx, t, tea, key3, true)

	// edits everywhere
	teaDelete(t, tea, key1)
	teaInsert(t, tea, key4)
	err = tea.Commit(ctx, nbf)
	require.NoError(t, err)
	requireGet(ctx, t, tea, key1, false)
	requireGet(ctx, t, tea, key4, true)
	teaDelete(t, tea, key2)
	teaInsert(t, tea, key5)
	tea.flushUncommitted()
	requireGet(ctx, t, tea, key2, false)
	requireGet(ctx, t, tea, key5, true)
	teaInsert(t, tea, key6)
	requireGet(ctx, t, tea, key1, false)
	requireGet(ctx, t, tea, key2, false)
	requireGet(ctx, t, tea, key3, true)
	requireGet(ctx, t, tea, key4, true)
	requireGet(ctx, t, tea, key5, true)
	requireGet(ctx, t, tea, key6, true)

	_, err = tea.MaterializeEdits(ctx, nbf)
	requireGet(ctx, t, tea, key1, false)
	requireGet(ctx, t, tea, key2, false)
	requireGet(ctx, t, tea, key3, true)
	requireGet(ctx, t, tea, key4, true)
	requireGet(ctx, t, tea, key5, true)
	requireGet(ctx, t, tea, key6, true)
}
