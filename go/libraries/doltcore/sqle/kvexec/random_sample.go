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

package kvexec

import (
	"io"
	"math/rand"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/iters"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
)

// randomSampleState contains the resolved storage map, schema, and
// sample limit needed to construct a randomSampleIter.
type randomSampleState struct {
	srcMap    prolly.Map
	srcSchema schema.Schema
	srcTags   []uint64
	limit     int64
	count     uint64
}

// canRandomSampleOrderByRand reports whether |n| is an unseeded
// ORDER BY RAND() LIMIT k query on a keyed table that can be
// accelerated by sampling random row positions from Prolly storage.
//
// Returns false if |r| is non-empty, |n| has filters or seeds, or the
// limit exceeds sequential scan cost or memory limits in |ctx|.
func canRandomSampleOrderByRand(
	ctx *sql.Context,
	n *plan.TopN,
	r sql.Row,
) (randomSampleState, bool) {
	if len(r) != 0 || n.CalcFoundRows || len(n.SortConditions) != 1 {
		return randomSampleState{}, false
	}
	randFn, ok := n.SortConditions[0].Expr.(*function.Rand)
	if !ok || randFn.Child != nil {
		return randomSampleState{}, false
	}

	srcMap, _, _, srcSchema, srcTags, srcFilter, err := getSourceKv(ctx, n.Child, true)
	if err != nil || srcSchema == nil || srcFilter != nil || schema.IsKeyless(srcSchema) {
		return randomSampleState{}, false
	}

	limit, err := iters.GetInt64Value(ctx, n.Limit)
	if err != nil || limit <= 0 {
		return randomSampleState{}, false
	}

	count, err := srcMap.Count()
	if err != nil || count == 0 {
		return randomSampleState{}, false
	}

	// Enforce dual upper bounds: memory budget and I/O crossover.
	maxLimit := maxMemorySampleLimit(ctx)
	if ioLimit := maxIoSampleLimit(uint64(count)); ioLimit < maxLimit {
		maxLimit = ioLimit
	}
	if limit > maxLimit {
		return randomSampleState{}, false
	}

	return randomSampleState{
		srcMap:    srcMap,
		srcSchema: srcSchema,
		srcTags:   srcTags,
		limit:     limit,
		count:     uint64(count),
	}, true
}

// maxMemorySampleLimit returns the sample limit allowed by the
// session's [sort_buffer_size] variable in |ctx|.
//
// Each selection map entry consumes ~24 bytes. Dividing
// [sort_buffer_size] by 24 keeps the working set within the sort
// buffer budget.
//
// [sort_buffer_size]: https://dev.mysql.com/doc/refman/8.4/en/server-system-variables.html#sysvar_sort_buffer_size
func maxMemorySampleLimit(ctx *sql.Context) int64 {
	const defaultLimit = 1024
	if ctx == nil || ctx.Session == nil {
		return defaultLimit
	}
	val, err := ctx.GetSessionVariable(ctx, "sort_buffer_size")
	if err != nil || val == nil {
		return defaultLimit
	}
	bufSize, _, err := types.Int64.Convert(ctx, val)
	if err != nil {
		return defaultLimit
	}
	bytes, ok := bufSize.(int64)
	if !ok || bytes <= 0 {
		return defaultLimit
	}
	limit := bytes / 24
	if limit <= 0 {
		return 1
	}
	return limit
}

// maxIoSampleLimit returns the sample limit where random ordinal
// seeks on [prolly.Map] outperform a sequential table scan of
// |totalRows|.
//
// Leaf chunks in a Prolly Tree average 20-80 rows based on Dolt's
// chunk boundaries (see [prolly.Map.IterOrdinalRange]). A sequential
// scan reads ceil(N / 20) chunks once. If k >= N / 20, random probes
// visit more chunks than exist in the table.
func maxIoSampleLimit(totalRows uint64) int64 {
	ioLimit := int64(totalRows / 20)
	if ioLimit < 1 {
		return 1
	}
	return ioLimit
}

// newRandomSampleIter returns a [sql.RowIter] that samples uniform
// random rows from |s.srcMap| using Prolly tree ordinal seeks.
func newRandomSampleIter(
	ctx *sql.Context,
	s randomSampleState,
) (sql.RowIter, error) {
	// Filtering duplicates directly keeps the output order random.
	ranks := make([]uint64, 0, s.limit)
	seen := make(map[uint64]struct{}, s.limit)
	for len(ranks) < int(s.limit) {
		rnd := uint64(rand.Int63n(int64(s.count)))
		if _, exists := seen[rnd]; !exists {
			seen[rnd] = struct{}{}
			ranks = append(ranks, rnd)
		}
	}

	rowJoiner := newRowJoiner(ctx, []schema.Schema{s.srcSchema}, []int{len(s.srcTags)}, s.srcTags, s.srcMap.NodeStore())
	return &randomSampleIter{
		srcMap:    s.srcMap,
		rowJoiner: rowJoiner,
		ranks:     ranks,
	}, nil
}

type randomSampleIter struct {
	srcMap    prolly.Map
	rowJoiner *prollyToSqlJoiner
	ranks     []uint64
	idx       int
}

var _ sql.RowIter = (*randomSampleIter)(nil)

// Next retrieves the next sampled [sql.Row] by seeking its rank in
// [prolly.Map] via [prolly.Map.IterOrdinalRange].
func (r *randomSampleIter) Next(ctx *sql.Context) (sql.Row, error) {
	if r.idx >= len(r.ranks) {
		return nil, io.EOF
	}
	rank := r.ranks[r.idx]
	r.idx++

	iter, err := r.srcMap.IterOrdinalRange(ctx, rank, rank+1)
	if err != nil {
		return nil, err
	}
	k, v, err := iter.Next(ctx)
	if err != nil {
		return nil, err
	}
	return r.rowJoiner.buildRow(ctx, k, v)
}

// Close implements [sql.RowIter].
func (r *randomSampleIter) Close(_ *sql.Context) error {
	return nil
}
