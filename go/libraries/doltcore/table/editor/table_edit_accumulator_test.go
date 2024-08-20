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

package editor

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

var emptyTpl = types.EmptyTuple(types.Format_LD_1)

func newTestTEAF() *dbEaFactory {
	dir := os.TempDir()

	return &dbEaFactory{
		directory: dir,
		vrw:       types.NewMemoryValueStore(),
	}
}

func newTuple(t *testing.T, vals ...types.Value) types.Tuple {
	tpl, err := types.NewTuple(types.Format_LD_1, vals...)
	require.NoError(t, err)

	return tpl
}

func teaInsert(t *testing.T, tea TableEditAccumulator, key types.Tuple) {
	h, err := key.Hash(types.Format_LD_1)
	require.NoError(t, err)

	tea.Insert(h, key, emptyTpl)
}

func teaDelete(t *testing.T, tea TableEditAccumulator, key types.Tuple) {
	h, err := key.Hash(types.Format_LD_1)
	require.NoError(t, err)

	tea.Delete(h, key)
}

func requireGet(ctx context.Context, t *testing.T, tea TableEditAccumulator, key types.Tuple, expected bool) {
	h, err := key.Hash(types.Format_LD_1)
	require.NoError(t, err)
	_, has, err := tea.Get(ctx, h, key)
	require.NoError(t, err)
	require.Equal(t, expected, has)
}

func TestIndexEditAccumulatorStableOrder(t *testing.T) {
	origFlushThreshold := flushThreshold
	defer func() {
		indexFlushThreshold = origFlushThreshold
	}()
	indexFlushThreshold = 1

	ctx := context.Background()
	nbf := types.Format_LD_1
	teaf := newTestTEAF()
	m, err := types.NewMap(ctx, teaf.vrw)
	require.NoError(t, err)
	iea := teaf.NewIndexEA(ctx, m).(*indexEditAccumulatorImpl)

	h := func(k types.Tuple) hash.Hash {
		h, err := k.Hash(nbf)
		require.NoError(t, err)
		return h
	}

	k1 := newTuple(t, types.Int(0))
	k2 := newTuple(t, types.Int(1))

	err = iea.Insert(ctx, h(k1), h(k1), k1, emptyTpl)
	require.NoError(t, err)
	err = iea.Insert(ctx, h(k2), h(k1), k2, emptyTpl)
	require.NoError(t, err)

	err = iea.Delete(ctx, h(k1), h(k1), k1, k1)
	require.NoError(t, err)
	err = iea.Delete(ctx, h(k2), h(k2), k2, k2)
	require.NoError(t, err)

	err = iea.Insert(ctx, h(k1), h(k1), k1, k1)
	require.NoError(t, err)

	err = iea.Commit(ctx, nbf)
	require.NoError(t, err)

	m, err = iea.MaterializeEdits(ctx, nbf)
	require.NoError(t, err)
	require.Equal(t, uint64(1), m.Len())
}

func TestTableEditAccumulatorStableOrder(t *testing.T) {
	origFlushThreshold := flushThreshold
	defer func() {
		flushThreshold = origFlushThreshold
	}()
	flushThreshold = 2

	ctx := context.Background()
	nbf := types.Format_LD_1
	teaf := newTestTEAF()
	m, err := types.NewMap(ctx, teaf.vrw)
	require.NoError(t, err)
	tea := teaf.NewTableEA(ctx, m).(*tableEditAccumulatorImpl)

	h := func(k types.Tuple) hash.Hash {
		h, err := k.Hash(nbf)
		require.NoError(t, err)
		return h
	}

	k1 := newTuple(t, types.Int(0))
	k2 := newTuple(t, types.Int(1))
	err = tea.Delete(h(k1), k1)
	require.NoError(t, err)
	err = tea.Delete(h(k2), k2)
	require.NoError(t, err)

	err = tea.Insert(h(k1), k1, emptyTpl)
	require.NoError(t, err)
	err = tea.Insert(h(k2), k2, emptyTpl)
	require.NoError(t, err)

	err = tea.Commit(ctx, nbf)
	require.NoError(t, err)

	m, err = tea.MaterializeEdits(ctx, nbf)
	require.NoError(t, err)
	require.Equal(t, uint64(2), m.Len())
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	nbf := types.Format_LD_1
	teaf := newTestTEAF()
	m, err := types.NewMap(ctx, teaf.vrw)
	require.NoError(t, err)
	tea := teaf.NewTableEA(ctx, m).(*tableEditAccumulatorImpl)

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
	require.NoError(t, err)
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
	require.NoError(t, err)
	requireGet(ctx, t, tea, key1, false)
	requireGet(ctx, t, tea, key2, false)
	requireGet(ctx, t, tea, key3, true)
	requireGet(ctx, t, tea, key4, true)
	requireGet(ctx, t, tea, key5, true)
	requireGet(ctx, t, tea, key6, true)
}
