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
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func statTestSchema() schema.Schema {
	return schema.MustSchemaFromCols(schema.NewColCollection(
		schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("v", 1, types.IntKind, false),
	))
}

func statTestIndex(t testing.TB, sch schema.Schema, n int, value int64) durable.Index {
	t.Helper()
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	kd, vd := sch.GetMapDescriptors(ns)
	kb, vb := val.NewTupleBuilder(kd, ns), val.NewTupleBuilder(vd, ns)
	tuples := make([]val.Tuple, 0, n*2)
	for i := 0; i < n; i++ {
		kb.PutInt64(0, int64(i))
		vb.PutInt64(0, value)
		k, err := kb.Build(ctx, ns.Pool())
		require.NoError(t, err)
		v, err := vb.Build(ctx, ns.Pool())
		require.NoError(t, err)
		tuples = append(tuples, k, v)
	}
	m, err := prolly.NewMapFromTuples(ctx, ns, kd, vd, tuples...)
	require.NoError(t, err)
	return durable.IndexFromProllyMap(m)
}

func collectStat(ctx context.Context, from, to durable.Index, fromSch, toSch schema.Schema) (DiffStatProgress, int, error) {
	ch := make(chan DiffStatProgress)
	done := make(chan error, 1)
	go func() {
		defer close(ch)
		done <- diffProllyTrees(ctx, ch, false, from, to, fromSch, toSch)
	}()
	var total DiffStatProgress
	messages := 0
	for p := range ch {
		total.Adds += p.Adds
		total.Removes += p.Removes
		total.Changes += p.Changes
		total.CellChanges += p.CellChanges
		total.OldRowSize += p.OldRowSize
		total.NewRowSize += p.NewRowSize
		total.OldCellSize += p.OldCellSize
		total.NewCellSize += p.NewCellSize
		messages++
	}
	return total, messages, <-done
}

func TestDiffStatTotals(t *testing.T) {
	s := statTestSchema()
	for _, n := range []int{0, 1, 1023, 1024, 1025, 3000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			before, after := statTestIndex(t, s, n, 0), statTestIndex(t, s, n, 1)
			for _, tc := range []struct {
				name     string
				from, to durable.Index
				want     DiffStatProgress
			}{
				{"updates", before, after, DiffStatProgress{Changes: uint64(n), CellChanges: uint64(n), OldRowSize: uint64(n), NewRowSize: uint64(n), OldCellSize: uint64(2 * n), NewCellSize: uint64(2 * n)}},
				{"adds", nil, after, DiffStatProgress{Adds: uint64(n), NewRowSize: uint64(n), NewCellSize: uint64(2 * n)}},
				{"deletes", before, nil, DiffStatProgress{Removes: uint64(n), OldRowSize: uint64(n), OldCellSize: uint64(2 * n)}},
				{"unchanged", before, before, DiffStatProgress{OldRowSize: uint64(n), NewRowSize: uint64(n), OldCellSize: uint64(2 * n), NewCellSize: uint64(2 * n)}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got, messages, err := collectStat(context.Background(), tc.from, tc.to, s, s)
					require.NoError(t, err)
					require.Equal(t, tc.want, got)
					require.LessOrEqual(t, messages, 1+(n+diffStatBatchSize-1)/diffStatBatchSize)
				})
			}
		})
	}
}

// The fast path must need only Count(), without unwrapping or scanning rows.
type countOnlyStatIndex struct {
	durable.Index
	count uint64
}

func (i countOnlyStatIndex) Count() (uint64, error) { return i.count, nil }

func TestDiffStatEmptySideUsesCounts(t *testing.T) {
	s := statTestSchema()
	for _, tc := range []struct{ from, to uint64 }{{0, 1000000}, {1000000, 0}, {0, 0}} {
		got, messages, err := collectStat(context.Background(), countOnlyStatIndex{count: tc.from}, countOnlyStatIndex{count: tc.to}, s, s)
		require.NoError(t, err)
		require.Equal(t, 1, messages)
		require.Equal(t, DiffStatProgress{Adds: tc.to, Removes: tc.from, OldRowSize: tc.from, NewRowSize: tc.to, OldCellSize: 2 * tc.from, NewCellSize: 2 * tc.to}, got)
	}
}

func TestDiffStatCanceledWithoutConsumer(t *testing.T) {
	s := statTestSchema()
	for _, counts := range [][2]uint64{{0, 100}, {100, 100}} {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- diffProllyTrees(ctx, make(chan DiffStatProgress), false, countOnlyStatIndex{count: counts[0]}, countOnlyStatIndex{count: counts[1]}, s, s)
		}()
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("canceled statistics blocked sending progress")
		}
	}
}

func TestDiffStatKeylessCardinalities(t *testing.T) {
	ctx := context.Background()
	s := schema.MustSchemaFromCols(schema.NewColCollection(schema.NewColumn("v", 1, types.IntKind, false)))
	makeIndex := func(values map[int64]uint64) durable.Index {
		ns := tree.NewTestNodeStore()
		kd, vd := s.GetMapDescriptors(ns)
		vb := val.NewTupleBuilder(vd, ns)
		var pairs [][2]val.Tuple
		for value, cardinality := range values {
			vb.PutUint64(0, cardinality)
			vb.PutInt64(1, value)
			v, err := vb.Build(ctx, ns.Pool())
			require.NoError(t, err)
			pairs = append(pairs, [2]val.Tuple{val.HashTupleFromValue(ns.Pool(), v), v})
		}
		sort.Slice(pairs, func(i, j int) bool {
			c, err := kd.Compare(ctx, pairs[i][0], pairs[j][0])
			require.NoError(t, err)
			return c < 0
		})
		var tuples []val.Tuple
		for _, p := range pairs {
			tuples = append(tuples, p[0], p[1])
		}
		m, err := prolly.NewMapFromTuples(ctx, ns, kd, vd, tuples...)
		require.NoError(t, err)
		return durable.IndexFromProllyMap(m)
	}
	before, after := makeIndex(map[int64]uint64{1: 10, 2: 4}), makeIndex(map[int64]uint64{1: 3, 3: 20})
	for _, tc := range []struct {
		from, to      durable.Index
		adds, removes uint64
	}{
		{before, after, 20, 11}, {nil, after, 23, 0}, {before, nil, 0, 14},
	} {
		ch := make(chan DiffStatProgress)
		done := make(chan error, 1)
		go func() {
			defer close(ch)
			done <- diffProllyTrees(ctx, ch, true, tc.from, tc.to, s, s)
		}()
		var adds, removes uint64
		for p := range ch {
			adds += p.Adds
			removes += p.Removes
		}
		require.NoError(t, <-done)
		require.Equal(t, tc.adds, adds)
		require.Equal(t, tc.removes, removes)
	}
}

func TestDiffStatMixedChanges(t *testing.T) {
	ctx := context.Background()
	s := statTestSchema()
	before := statTestIndex(t, s, 3000, 0)
	after, err := durable.ProllyMapFromIndex(statTestIndex(t, s, 3000, 1))
	require.NoError(t, err)
	ns := after.NodeStore()
	kd, vd := s.GetMapDescriptors(ns)
	kb, vb := val.NewTupleBuilder(kd, ns), val.NewTupleBuilder(vd, ns)
	kb.PutInt64(0, 0)
	firstKey, err := kb.Build(ctx, ns.Pool())
	require.NoError(t, err)
	mut := after.Mutate()
	require.NoError(t, mut.Delete(ctx, firstKey))
	kb.PutInt64(0, 3000)
	lastKey, err := kb.Build(ctx, ns.Pool())
	require.NoError(t, err)
	vb.PutInt64(0, 1)
	value, err := vb.Build(ctx, ns.Pool())
	require.NoError(t, err)
	require.NoError(t, mut.Put(ctx, lastKey, value))
	after, err = mut.Map(ctx)
	require.NoError(t, err)
	got, _, err := collectStat(ctx, before, durable.IndexFromProllyMap(after), s, s)
	require.NoError(t, err)
	require.Equal(t, DiffStatProgress{Adds: 1, Removes: 1, Changes: 2999, CellChanges: 2999,
		OldRowSize: 3000, NewRowSize: 3000, OldCellSize: 6000, NewCellSize: 6000}, got)
}

func BenchmarkDiffStat(b *testing.B) {
	s := statTestSchema()
	before, after := statTestIndex(b, s, 100000, 0), statTestIndex(b, s, 100000, 1)
	for _, tc := range []struct {
		name     string
		from, to durable.Index
	}{
		{"modified", before, after}, {"added_table", nil, after}, {"deleted_table", before, nil},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _, err := collectStat(context.Background(), tc.from, tc.to, s, s)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
