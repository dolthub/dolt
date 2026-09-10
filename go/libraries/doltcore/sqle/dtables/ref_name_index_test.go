// Copyright 2022 Dolthub, Inc.
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

package dtables

import (
	"fmt"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestRefNameLookup(t *testing.T) {
	ctx := sql.NewEmptyContext()
	loads := 0
	table := &refIndexedTable{load: func(*sql.Context) ([]ref.DoltRef, hash.Hash, error) {
		loads++
		return []ref.DoltRef{ref.NewTagRef("gamma"), ref.NewTagRef("alpha"), ref.NewTagRef("beta")}, hash.Hash{}, nil
	}}
	tests := []struct {
		ranges sql.MySQLRangeCollection
		names  []string
	}{
		{sql.MySQLRangeCollection{{sql.ClosedRangeColumnExpr("beta", "beta", types.Text)}}, []string{"beta"}},
		{sql.MySQLRangeCollection{{sql.ClosedRangeColumnExpr("missing", "missing", types.Text)}}, nil},
		{sql.MySQLRangeCollection{{sql.OpenRangeColumnExpr("alpha", "gamma", types.Text)}}, []string{"beta"}},
		{sql.MySQLRangeCollection{{sql.AllRangeColumnExpr(types.Text)}}, []string{"alpha", "beta", "gamma"}},
		{sql.MySQLRangeCollection{{sql.NullRangeColumnExpr(types.Text)}}, nil},
		{sql.MySQLRangeCollection{{sql.EmptyRangeColumnExpr(types.Text)}}, nil},
		{sql.MySQLRangeCollection{{sql.LessOrEqualRangeColumnExpr("", types.Text)}}, nil},
		{sql.MySQLRangeCollection{{sql.ClosedRangeColumnExpr("alpha", "beta", types.Text)}, {sql.ClosedRangeColumnExpr("beta", "gamma", types.Text)}}, []string{"alpha", "beta", "gamma"}},
	}
	for _, test := range tests {
		parts, err := table.LookupPartitions(ctx, sql.IndexLookup{Ranges: test.ranges})
		require.NoError(t, err)
		var names []string
		for {
			part, err := parts.Next(ctx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			for _, r := range part.(*refPartition).refs {
				names = append(names, r.GetPath())
			}
		}
		require.NoError(t, parts.Close(ctx))
		require.Equal(t, test.names, names)
	}
	require.Equal(t, 1, loads, "repeated join lookups must reuse the ref snapshot")
}

func TestRemoteRefNameLookup(t *testing.T) {
	ctx := sql.NewEmptyContext()
	table := &refIndexedTable{load: func(*sql.Context) ([]ref.DoltRef, hash.Hash, error) {
		return []ref.DoltRef{ref.NewRemoteRef("origin", "alpha"), ref.NewRemoteRef("origin", "beta")}, hash.Hash{}, nil
	}}
	for _, name := range []string{"remotes/origin/alpha", "origin/alpha", "alpha"} {
		parts, err := table.LookupPartitions(ctx, sql.IndexLookup{Ranges: sql.MySQLRangeCollection{{sql.ClosedRangeColumnExpr(name, name, types.Text)}}})
		require.NoError(t, err)
		p, err := parts.Next(ctx)
		if name == "remotes/origin/alpha" {
			require.NoError(t, err)
			require.Equal(t, []ref.DoltRef{ref.NewRemoteRef("origin", "alpha")}, p.(*refPartition).refs)
		} else {
			require.Equal(t, io.EOF, err)
		}
		require.NoError(t, parts.Close(ctx))
	}
}

func BenchmarkRefNameLookup(b *testing.B) {
	for _, size := range []int{100, 14000} {
		for _, point := range []bool{true, false} {
			b.Run(fmt.Sprintf("refs=%d/point=%t", size, point), func(b *testing.B) {
				ctx := sql.NewEmptyContext()
				table := &refIndexedTable{load: func(*sql.Context) ([]ref.DoltRef, hash.Hash, error) {
					refs := make([]ref.DoltRef, size)
					for i := range refs {
						refs[i] = ref.NewTagRef(fmt.Sprintf("tag%06d", i))
					}
					return refs, hash.Hash{}, nil
				}}
				upper := "tag000050"
				if !point {
					upper = "tag000060"
				}
				lookup := sql.IndexLookup{Ranges: sql.MySQLRangeCollection{{sql.ClosedRangeColumnExpr("tag000050", upper, types.Text)}}}
				// Exclude building the shared snapshot: measure repeated join probes.
				parts, err := table.LookupPartitions(ctx, lookup)
				require.NoError(b, err)
				require.NoError(b, parts.Close(ctx))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					parts, err := table.LookupPartitions(ctx, lookup)
					if err != nil {
						b.Fatal(err)
					}
					if err = parts.Close(ctx); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
