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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkAllWrites(b *testing.B) {
	benchmarkProllyMapUpdate(b, 10_000, 1)
	benchmarkTypesMapUpdate(b, 10_000, 1)
	benchmarkProllyMapUpdate(b, 100_000, 1)
	benchmarkTypesMapUpdate(b, 100_000, 1)
}

func BenchmarkProllySmallWrites(b *testing.B) {
	benchmarkProllyMapUpdate(b, 10_000, 10)
}

func BenchmarkTypesSmallWrites(b *testing.B) {
	benchmarkTypesMapUpdate(b, 10_000, 10)
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
	bench := generateProllyBench(size)
	b.ReportAllocs()
	b.ResetTimer()

	b.Run("benchmark prolly map writes", func(b *testing.B) {
		ctx := context.Background()
		iters := int(size) / 10

		for i := 0; i < iters; i++ {
			mut := bench.m.Mutate()
			for j := 0; j < int(k); j++ {
				idx := rand.Uint64() % uint64(len(bench.tups))
				key := bench.tups[idx][0]
				idx = rand.Uint64() % uint64(len(bench.tups))
				value := bench.tups[idx][0]

				err := mut.Put(ctx, key, value)
				require.NoError(b, err)
			}
			mm, err := mut.Map(ctx)
			require.NoError(b, err)
			assert.False(b, mm.Empty())
		}
		b.ReportAllocs()
	})
}

func benchmarkTypesMapUpdate(b *testing.B, size, k uint64) {
	bench := generateTypesBench(size)
	b.ResetTimer()

	b.Run("benchmark types map writes", func(b *testing.B) {
		ctx := context.Background()
		iters := int(bench.m.Len()) / 10

		for i := 0; i < iters; i++ {
			edit := bench.m.Edit()
			for j := 0; j < int(k); j++ {
				idx := rand.Uint64() % uint64(len(bench.tups))
				key := bench.tups[idx][0]
				idx = rand.Uint64() % uint64(len(bench.tups))
				value := bench.tups[idx][0]
				edit.Set(key, value)
			}
			mm, err := edit.Map(ctx)
			require.NoError(b, err)
			assert.Equal(b, bench.m.Len(), mm.Len())
		}
		b.ReportAllocs()
	})
}
