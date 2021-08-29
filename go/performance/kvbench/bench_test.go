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
	"fmt"
	"math/rand"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// usage: `go test -bench BenchmarkMemoryStore`
func BenchmarkMemoryStore(b *testing.B) {
	benchmarkKVStore(b, newMemStore())
}

// usage: `go test -bench BenchmarkProllyStore`
func BenchmarkProllyStore(b *testing.B) {
	ctx := context.Background()
	benchmarkKVStore(b, newMemoryProllyStore(ctx))
}

func BenchmarkBoltStore(b *testing.B) {
	benchmarkKVStore(b, newBoltStore(os.TempDir()))
}

func benchmarkKVStore(b *testing.B, store keyValStore) {
	keys := loadStore(b, store)

	f := makePprofFile(b)
	err := pprof.StartCPUProfile(f)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		pprof.StopCPUProfile()
		if err = f.Close(); err != nil {
			b.Fatal(err)
		}
		fmt.Printf("\twriting CPU profile for %s: %s\n", b.Name(), f.Name())
	}()

	b.Run("point reads", func(b *testing.B) {
		runBenchmark(b, store, keys)
	})
}

func loadStore(b *testing.B, store keyValStore) (keys [][]byte) {
	return loadStoreWithParams(b, store, loadParams{
		cardinality: 1_000_000,
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

	if fs, ok := store.(flushingKeyValStore); ok {
		fs.flush()
	}

	return
}

func runBenchmark(b *testing.B, store keyValStore, keys [][]byte) {
	runBenchmarkWithParams(b, store, keys, benchParams{
		numReads: 10_000,
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

func makePprofFile(b *testing.B) *os.File {
	_, testFile, _, _ := runtime.Caller(0)

	name := fmt.Sprintf("%s_%d.pprof", b.Name(), time.Now().Unix())
	f, err := os.Create(path.Join(path.Dir(testFile), name))
	if err != nil {
		b.Fatal(err)
	}
	return f
}
