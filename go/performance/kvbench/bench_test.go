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

package kvbench

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkMemoryStore(b *testing.B) {
	benchmarkKVStore(b, newMemStore())
}

func BenchmarkProllyStore(b *testing.B) {
	ctx := context.Background()
	benchmarkKVStore(b, newMemoryProllyStore(ctx))
}

func benchmarkKVStore(b *testing.B, store keyValStore) {
	b.StopTimer()
	keys := loadStore(b, store)
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	b.ResetTimer()
	b.StartTimer()

	runBenchmark(b, store, keys)
}

func loadStore(b *testing.B, store keyValStore) (keys [][]byte) {
	return loadStoreWithParams(b, store, loadParams{
		cardinality: 100_000,
		keySize:     16,
		valSize:     128,
	})
}

type loadParams struct {
	cardinality uint32
	keySize     uint32
	valSize     uint32
}

func loadStoreWithParams(b *testing.B, store keyValStore, p loadParams) (keys [][]byte) {
	keys = make([][]byte, p.cardinality)

	// generate 10K rows at a time
	const batchSize = uint32(10_000)
	numBatches := p.cardinality / batchSize

	pairSize := p.keySize + p.valSize
	bufSize := pairSize * batchSize
	buf := make([]byte, bufSize)

	k := 0
	for i := uint32(0); i < numBatches; i++ {
		_, err := rand.Read(buf)
		require.NoError(b, err)

		for j := uint32(0); j < batchSize; j++ {
			offset := j * pairSize
			key := buf[offset : offset+p.keySize]
			val := buf[offset+p.keySize : offset+pairSize]
			store.load(key, val)
			keys[k] = key
			k++
		}
	}
	return
}

func runBenchmark(b *testing.B, store keyValStore, keys [][]byte) {
	runBenchmarkWithParams(b, store, keys, benchParams{
		numReads: 1000,
	})
}

type benchParams struct {
	numReads uint32
}

func runBenchmarkWithParams(b *testing.B, store keyValStore, keys [][]byte, p benchParams) {
	for _, k := range keys[:p.numReads] {
		_, ok := store.get(k)
		require.True(b, ok)
	}
}
