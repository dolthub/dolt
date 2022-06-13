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
	"math/rand"
	"testing"
)

func BenchmarkMapUpdate(b *testing.B) {
	b.Run("benchmark maps 10k", func(b *testing.B) {
		benchmarkProllyMapUpdate(b, 10_000, 1)
		benchmarkTypesMapUpdate(b, 10_000, 1)
	})
	b.Run("benchmark maps 100k", func(b *testing.B) {
		benchmarkProllyMapUpdate(b, 100_000, 1)
		benchmarkTypesMapUpdate(b, 100_000, 1)
	})
	b.Run("benchmark maps 1M", func(b *testing.B) {
		benchmarkProllyMapUpdate(b, 1_000_000, 1)
		benchmarkTypesMapUpdate(b, 1_000_000, 1)
	})
}

func BenchmarkProllySmallWrites(b *testing.B) {
	benchmarkProllyMapUpdate(b, 10_000, 1)
}

func BenchmarkTypesSmallWrites(b *testing.B) {
	benchmarkTypesMapUpdate(b, 10_000, 1)
}

func BenchmarkProllyMediumWrites(b *testing.B) {
	benchmarkProllyMapUpdate(b, 100_000, 1)
}

func BenchmarkTypesMediumWrites(b *testing.B) {
	benchmarkTypesMapUpdate(b, 100_000, 1)
}

func BenchmarkProllyLargeWrites(b *testing.B) {
	benchmarkProllyMapUpdate(b, 1_000_000, 1)
}

func benchmarkProllyMapUpdate(b *testing.B, size, k uint64) {
	bench := generateProllyBench(b, size)
	b.ReportAllocs()
	b.ResetTimer()

	b.Run("benchmark new format writes", func(b *testing.B) {
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			mut := bench.m.Mutate()
			for j := 0; j < int(k); j++ {
				idx := rand.Uint64() % uint64(len(bench.tups))
				key := bench.tups[idx][0]
				idx = rand.Uint64() % uint64(len(bench.tups))
				value := bench.tups[idx][0]

				_ = mut.Put(ctx, key, value)
			}
			_, _ = mut.Map(ctx)
		}
		b.ReportAllocs()
	})
}

func benchmarkTypesMapUpdate(b *testing.B, size, k uint64) {
	bench := generateTypesBench(b, size)
	b.ResetTimer()

	b.Run("benchmark old format writes", func(b *testing.B) {
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			edit := bench.m.Edit()
			for j := 0; j < int(k); j++ {
				idx := rand.Uint64() % uint64(len(bench.tups))
				key := bench.tups[idx][0]
				idx = rand.Uint64() % uint64(len(bench.tups))
				value := bench.tups[idx][0]
				edit.Set(key, value)
			}
			_, _ = edit.Map(ctx)
		}
		b.ReportAllocs()
	})
}
