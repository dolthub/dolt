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

package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/val"
)

func BenchmarkMapGet(b *testing.B) {
	b.Run("benchmark maps 10k", func(b *testing.B) {
		benchmarkProllyMapGet(b, 10_000)
		benchmarkTypesMapGet(b, 10_000)
	})
	b.Run("benchmark maps 100k", func(b *testing.B) {
		benchmarkProllyMapGet(b, 100_000)
		benchmarkTypesMapGet(b, 100_000)
	})
	b.Run("benchmark maps 1M", func(b *testing.B) {
		benchmarkProllyMapGet(b, 1_000_000)
		benchmarkTypesMapGet(b, 1_000_000)
	})
}

func BenchmarkStepMapGet(b *testing.B) {
	b.Skip()
	step := uint64(100_000)
	for sz := step; sz < step*20; sz += step {
		nm := fmt.Sprintf("benchmark maps %d", sz)
		b.Run(nm, func(b *testing.B) {
			benchmarkProllyMapGet(b, sz)
			benchmarkTypesMapGet(b, sz)
		})
	}
}

func BenchmarkParallelMapGet(b *testing.B) {
	b.Run("benchmark maps 10k", func(b *testing.B) {
		benchmarkProllyMapGetParallel(b, 10_000)
		benchmarkTypesMapGetParallel(b, 10_000)
	})
	b.Run("benchmark maps 100k", func(b *testing.B) {
		benchmarkProllyMapGetParallel(b, 100_000)
		benchmarkTypesMapGetParallel(b, 100_000)
	})
	b.Run("benchmark maps 1M", func(b *testing.B) {
		benchmarkProllyMapGetParallel(b, 1_000_000)
		benchmarkTypesMapGetParallel(b, 1_000_000)
	})
}

func BenchmarkStepParallelMapGet(b *testing.B) {
	b.Skip()
	step := uint64(100_000)
	for sz := step; sz < step*20; sz += step {
		nm := fmt.Sprintf("benchmark maps parallel %d", sz)
		b.Run(nm, func(b *testing.B) {
			benchmarkProllyMapGetParallel(b, sz)
			benchmarkTypesMapGetParallel(b, sz)
		})
	}
}

func BenchmarkGetLargeProlly(b *testing.B) {
	benchmarkProllyMapGet(b, 1_000_000)
}

func BenchmarkGetLargeNoms(b *testing.B) {
	benchmarkTypesMapGet(b, 1_000_000)
}

func BenchmarkGetLargeBBolt(b *testing.B) {
	benchmarkBBoltMapGet(b, 1_000_000)
}

func BenchmarkProllyParallelGetLarge(b *testing.B) {
	benchmarkProllyMapGetParallel(b, 1_000_000)
}

func BenchmarkNomsParallelGetLarge(b *testing.B) {
	benchmarkTypesMapGetParallel(b, 1_000_000)
}

func benchmarkProllyMapGet(b *testing.B, size uint64) {
	bench := generateProllyBench(b, size)
	b.ResetTimer()
	b.Run("benchmark new format reads", func(b *testing.B) {
		ctx := context.Background()

		for i := 0; i < b.N; i++ {
			idx := rand.Uint64() % uint64(len(bench.tups))
			key := bench.tups[idx][0]
			_ = bench.m.Get(ctx, key, func(_, _ val.Tuple) (e error) {
				return
			})
		}
		b.ReportAllocs()
	})
}

func benchmarkTypesMapGet(b *testing.B, size uint64) {
	bench := generateTypesBench(b, size)
	b.ResetTimer()
	b.Run("benchmark old format reads", func(b *testing.B) {
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			idx := rand.Uint64() % uint64(len(bench.tups))
			_, _, _ = bench.m.MaybeGet(ctx, bench.tups[idx][0])
		}
		b.ReportAllocs()
	})
}

func benchmarkBBoltMapGet(b *testing.B, size uint64) {
	bench := generateBBoltBench(b, size)
	b.ResetTimer()
	b.Run("benchmark bbolt reads", func(b *testing.B) {
		tx, err := bench.db.Begin(false)
		require.NoError(b, err)
		bck := tx.Bucket(bucket)

		for i := 0; i < b.N; i++ {
			idx := rand.Uint64() % uint64(len(bench.tups))
			key := bench.tups[idx][0]
			_ = bck.Get(key)
		}
		b.ReportAllocs()
	})
}

func benchmarkProllyMapGetParallel(b *testing.B, size uint64) {
	bench := generateProllyBench(b, size)
	b.Run(fmt.Sprintf("benchmark new format %d", size), func(b *testing.B) {
		b.RunParallel(func(b *testing.PB) {
			ctx := context.Background()
			rnd := rand.NewSource(0)
			for b.Next() {
				idx := int(rnd.Int63()) % len(bench.tups)
				key := bench.tups[idx][0]
				_ = bench.m.Get(ctx, key, func(_, _ val.Tuple) (e error) {
					return
				})
			}
		})
		b.ReportAllocs()
	})
}

func benchmarkTypesMapGetParallel(b *testing.B, size uint64) {
	bench := generateTypesBench(b, size)
	b.Run(fmt.Sprintf("benchmark old format %d", size), func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			ctx := context.Background()
			rnd := rand.NewSource(0)
			for pb.Next() {
				idx := int(rnd.Int63()) % len(bench.tups)
				_, _, _ = bench.m.MaybeGet(ctx, bench.tups[idx][0])
			}
		})
		b.ReportAllocs()
	})
}

const mapScale = 4096

func BenchmarkGoMapGet(b *testing.B) {
	b.Skip()
	kv1 := makeGoMap(mapScale)
	kv2 := makeSyncMap(mapScale)
	b.ResetTimer()

	b.Run("test golang map", func(b *testing.B) {
		for j := 0; j < b.N; j++ {
			_, ok := kv1[uint64(j%mapScale)]
			if !ok {
				b.Fail()
			}
		}
		b.ReportAllocs()
	})
	b.Run("test golang sync map", func(b *testing.B) {
		for j := 0; j < b.N; j++ {
			_, ok := kv2.Load(uint64(j % mapScale))
			if !ok {
				b.Fail()
			}
		}
		b.ReportAllocs()
	})
}

func BenchmarkParallelGoMapGet(b *testing.B) {
	b.Skip()
	kv1 := makeGoMap(mapScale)
	kv2 := makeSyncMap(mapScale)
	b.ResetTimer()

	b.Run("test golang map", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			j := 0
			for pb.Next() {
				_, _ = kv1[uint64(j%mapScale)]
				j++
			}
		})
		b.ReportAllocs()
	})
	b.Run("test golang sync map", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			v, _ := kv2.Load(uint64(1234))
			tup := v.(val.Tuple)
			j := 0
			for pb.Next() {
				k := uint64(j % mapScale)
				if j%10 == 0 {
					kv2.Store(k, tup)
				} else {
					_, _ = kv2.Load(k)
				}
				j++
			}
		})
		b.ReportAllocs()
	})
}

func makeGoMap(scale uint64) map[uint64]val.Tuple {
	src := rand.NewSource(0)
	vb := val.NewTupleBuilder(val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
	), nil)

	kv := make(map[uint64]val.Tuple, scale)
	for i := uint64(0); i < scale; i++ {
		vb.PutInt64(0, src.Int63())
		vb.PutInt64(1, src.Int63())
		vb.PutInt64(2, src.Int63())
		vb.PutInt64(3, src.Int63())
		vb.PutInt64(4, src.Int63())
		kv[i], _ = vb.Build(shared)
	}
	return kv
}

func makeSyncMap(scale uint64) *sync.Map {
	src := rand.NewSource(0)
	vb := val.NewTupleBuilder(val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
	), nil)
	kv := &sync.Map{}

	for i := uint64(0); i < scale; i++ {
		vb.PutInt64(0, src.Int63())
		vb.PutInt64(1, src.Int63())
		vb.PutInt64(2, src.Int63())
		vb.PutInt64(3, src.Int63())
		vb.PutInt64(4, src.Int63())
		kv.Store(i, vb.Build(shared))
	}
	return kv
}
