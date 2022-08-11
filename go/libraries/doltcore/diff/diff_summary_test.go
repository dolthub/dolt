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

package diff

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

var (
	summarySch   schema.Schema
	summaryPool  = pool.NewBuffPool()
	summaryStore = tree.NewTestNodeStore()
	keyDesc      = val.NewTupleDescriptor(val.Type{
		Enc:      val.Int32Enc,
		Nullable: false,
	})
	valDesc = keyDesc
)

func init() {
	summarySch, _ = schema.SchemaFromCols(schema.NewColCollection(
		schema.NewColumn("pk", 1, types.IntKind, true),
		schema.NewColumn("c0", 2, types.IntKind, false),
	))

}

func TestDiffSummary(t *testing.T) {
	ctx := context.Background()
	from, to := getProllyIndexes(t, ctx)

	ch := make(chan DiffSummaryProgress)
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer close(ch)
		return Summary(ctx, ch, from, to, summarySch, summarySch)
	})
	eg.Go(func() error {
		for p := range ch {
			assert.True(t, p.OldSize == p.NewSize)
		}
		return nil
	})
	assert.NoError(t, eg.Wait())
}

const scale = 10000

func getProllyIndexes(t *testing.T, ctx context.Context) (from, to durable.Index) {
	tuples := ascendingTuples()
	fromMap, err := prolly.NewMapFromTuples(ctx, summaryStore, keyDesc, valDesc, tuples...)
	assert.NoError(t, err)

	// shuffle values and update
	rand.Shuffle(scale, func(i, j int) {
		i, j = (i*2)+1, (j*2)+1
		tuples[i], tuples[j] = tuples[j], tuples[i]
	})
	mut := fromMap.Mutate()
	for i := 0; i < scale; i++ {
		k, v := tuples[i*2], tuples[(i*2)+1]
		err = mut.Put(ctx, k, v)
		assert.NoError(t, err)
	}
	toMap, err := mut.Map(ctx)
	assert.NoError(t, err)

	from = durable.IndexFromProllyMap(fromMap)
	to = durable.IndexFromProllyMap(toMap)
	return
}

func ascendingTuples() []val.Tuple {
	bld := val.NewTupleBuilder(keyDesc)
	tt := make([]val.Tuple, scale*2)
	for i := range tt {
		bld.PutInt32(0, int32(i))
		tt[i] = bld.Build(summaryPool)
	}
	return tt
}
