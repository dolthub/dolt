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
	"testing"

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

func BenchmarkProllyGetLarge(b *testing.B) {
	benchmarkProllyMapGet(b, 1_000_000)
}

func BenchmarkNomsGetLarge(b *testing.B) {
	benchmarkTypesMapGet(b, 1_000_000)
}

func BenchmarkProllyParallelGetLarge(b *testing.B) {
	benchmarkProllyMapGetParallel(b, 1_000_000)
}

func BenchmarkNomsParallelGetLarge(b *testing.B) {
	benchmarkTypesMapGetParallel(b, 1_000_000)
}

func benchmarkProllyMapGet(b *testing.B, size uint64) {
	bench := generateProllyBench(b, size)
	b.Run(fmt.Sprintf("benchmark new format reads"), func(b *testing.B) {
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
	b.Run(fmt.Sprintf("benchmark old format reads"), func(b *testing.B) {
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			idx := rand.Uint64() % uint64(len(bench.tups))
			_, _, _ = bench.m.MaybeGet(ctx, bench.tups[idx][0])
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
