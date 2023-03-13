// Copyright 2023 Dolthub, Inc.
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

package doltdb

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

var ConcGCMapEditsSleepMillis = flag.Int("conc_gc_map_edits_sleep_millis", 100, "number of seconds to sleep between GCs")
var ConcGCMapEditsNumIters = flag.Int("conc_gc_map_edits_num_iters", 16, "number of GC iterations to run")
var ConcGCMapEditsOldGenPerc = flag.Float64("conc_gc_map_edits_old_gen_perc", 1, "percentage of refs to put in the old gen")

func TestConcurrentMapEditsGC(t *testing.T) {
	scales := []int{
		1,
		2,
		4,
		8,
		16,
		32,
		64,
		128,
		256,
		512,
		1024,
		2048,
		4096,
	}

	tmpdir, err := os.MkdirTemp("", "prolly_concurrent_gc_test-*")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tmpdir)
	})

	ctx := context.Background()

	db, _, ns, err := dbfactory.FileFactory{}.CreateDB(ctx, types.Format_DOLT, &url.URL{
		Scheme: "file",
		Path:   tmpdir,
	}, nil)
	require.NoError(t, err)

	var mu sync.RWMutex
	roots := make([]hash.Hash, len(scales))
	js := make([]int, len(scales))

	sharedPool := pool.NewBuffPool()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(len(scales))

	for i := range scales {
		i := i

		ctx := context.Background()

		ds, err := db.GetDataset(ctx, fmt.Sprintf("scale_%d", i))
		require.NoError(t, err)

		kd := val.NewTupleDescriptor(
			val.Type{Enc: val.Int64Enc, Nullable: false},
		)
		vd := val.NewTupleDescriptor(
			val.Type{Enc: val.Int64Enc, Nullable: true},
		)

		var mutKeyBuilder = val.NewTupleBuilder(vd)
		var mutValBuilder = val.NewTupleBuilder(kd)

		serializer := message.NewProllyMapSerializer(vd, ns.Pool())
		chunker, err := tree.NewEmptyChunker(ctx, ns, serializer)
		require.NoError(t, err)

		for j := 0; j < scales[i]; j++ {
			newNumber := int64(j)

			mutKeyBuilder.PutInt64(0, newNumber)
			k := mutKeyBuilder.Build(sharedPool)

			mutValBuilder.PutInt64(0, newNumber)
			v := mutValBuilder.Build(sharedPool)
			err := chunker.AddPair(ctx, tree.Item(k), tree.Item(v))
			require.NoError(t, err)
		}
		root, err := chunker.Done(ctx)
		require.NoError(t, err)

		m := prolly.NewMap(root, ns, kd, vd)

		cm, err := datas.NewCommitMeta("testing", "testing@testing.com", "some commit")
		require.NoError(t, err)
		ds, err = db.Commit(ctx, ds, tree.ValueFromNode(m.Node()), datas.CommitOptions{
			Meta: cm,
		})
		require.NoError(t, err)

		addr, ok := ds.MaybeHeadAddr()
		require.True(t, ok)
		roots[i] = addr

		go func() {
			defer wg.Done()
			j := 1
			for {
				select {
				case <-stop:
					t.Logf("scales[i]: %d, js[i]: %d", scales[i], j)
					js[i] = j
					return
				default:
				}

				newNumber := int64(scales[i] + j)

				mutKeyBuilder.PutInt64(0, newNumber)
				k := mutKeyBuilder.Build(sharedPool)

				mutValBuilder.PutInt64(0, newNumber)
				v := mutValBuilder.Build(sharedPool)

				mut := m.Mutate()
				err := mut.Put(ctx, k, v)
				require.NoError(t, err)
				j++

				m, err = mut.Map(ctx)
				require.NoError(t, err)

				cm, err := datas.NewCommitMeta("testing", "testing@testing.com", "some commit")
				require.NoError(t, err)
				ds, err = db.Commit(ctx, ds, tree.ValueFromNode(m.Node()), datas.CommitOptions{
					Meta: cm,
				})
				require.NoError(t, err)

				addr, ok := ds.MaybeHeadAddr()
				require.True(t, ok)
				mu.RLock()
				roots[i] = addr
				mu.RUnlock()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < *ConcGCMapEditsNumIters; i++ {
			time.Sleep(time.Duration(*ConcGCMapEditsSleepMillis) * time.Millisecond)
			ctx := context.Background()
			newhashes := make(hash.HashSet)
			oldhashes := make(hash.HashSet)
			cutoff := int(float64(len(roots)-1) * (*ConcGCMapEditsOldGenPerc))
			mu.Lock()
			for i, h := range roots {
				if i >= cutoff {
					newhashes.Insert(h)
				} else {
					oldhashes.Insert(h)
				}
			}
			mu.Unlock()
			t.Logf("%v: running gc", time.Now())
			i := 0
			err := db.(datas.GarbageCollector).GC(ctx, oldhashes, newhashes, nil)
			t.Logf("%v: finished gc with err: %v", i, err)
		}
		close(stop)
	}()

	wg.Wait()

	// TODO: Load maps at roots and iterate them...
}
