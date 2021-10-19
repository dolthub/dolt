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
	"bytes"
	"context"
	"encoding/binary"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func init() {
	prollyMapSmall = generateProllyBench(10_000)
	prollyMapMedium = generateProllyBench(100_000)

	typesMapSmall = generateTypesBench(10_000)
	typesMapMedium = generateTypesBench(100_000)
}

type prollyBench struct {
	m    prolly.Map
	tups [][2]val.Tuple
}

var prollyMapSmall prollyBench
var prollyMapMedium prollyBench
var prollyMapLarge prollyBench

type typesBench struct {
	m    types.Map
	tups [][2]types.Tuple
}

var typesMapSmall typesBench
var typesMapMedium typesBench
var typesMapLarge typesBench

func BenchmarkAll(b *testing.B) {
	b.Run("prolly map small", func(b *testing.B) {
		benchmarkProllyMap(b, prollyMapSmall)
	})
	b.Run("types map small", func(b *testing.B) {
		benchmarkTypesMap(b, typesMapSmall)
	})
	b.Run("prolly map medium", func(b *testing.B) {
		benchmarkProllyMap(b, prollyMapMedium)
	})
	b.Run("types map medium", func(b *testing.B) {
		benchmarkTypesMap(b, typesMapMedium)
	})
}

func BenchmarkProllySmall(b *testing.B) {
	benchmarkProllyMap(b, prollyMapSmall)
}

func BenchmarkProllyMedium(b *testing.B) {
	benchmarkProllyMap(b, prollyMapMedium)
}

func BenchmarkTypesSmall(b *testing.B) {
	benchmarkTypesMap(b, typesMapSmall)
}

func BenchmarkTypesMedium(b *testing.B) {
	benchmarkTypesMap(b, typesMapMedium)
}

func benchmarkProllyMap(b *testing.B, bench prollyBench) {
	ctx := context.Background()
	for i := 0; i < len(bench.tups)/10; i++ {
		idx := rand.Uint64() % uint64(len(bench.tups))
		err := bench.m.Get(ctx, bench.tups[idx][0], func(key, value val.Tuple) (e error) {
			assert.NotNil(b, key)
			assert.Equal(b, bench.tups[idx][0], key)
			assert.Equal(b, bench.tups[idx][1], value)
			return
		})
		assert.NoError(b, err)
	}
}

func benchmarkTypesMap(b *testing.B, bench typesBench) {
	ctx := context.Background()
	for i := 0; i < len(bench.tups)/10; i++ {
		idx := rand.Uint64() % uint64(len(bench.tups))
		_, ok, err := bench.m.MaybeGet(ctx, bench.tups[idx][0])
		assert.NoError(b, err)
		assert.True(b, ok)
	}
}

func generateProllyBench(size uint64) prollyBench {
	ctx := context.Background()
	nrw := newTestNRW()

	kd := val.NewTupleDescriptor(
		val.Type{Coll: val.ByteOrderCollation, Nullable: false},
	)
	vd := val.NewTupleDescriptor(
		val.Type{Coll: val.ByteOrderCollation, Nullable: true},
		val.Type{Coll: val.ByteOrderCollation, Nullable: true},
		val.Type{Coll: val.ByteOrderCollation, Nullable: true},
		val.Type{Coll: val.ByteOrderCollation, Nullable: true},
		val.Type{Coll: val.ByteOrderCollation, Nullable: true},
	)

	tups := generateProllyTuples(size)

	tt := make([]val.Tuple, 0, len(tups)*2)
	for i := range tups {
		tt = append(tt, tups[i][0], tups[i][1])
	}

	m, err := prolly.MakeNewMap(ctx, nrw, kd, vd, tt...)
	if err != nil {
		panic(err)
	}

	return prollyBench{m: m, tups: tups}
}

func newTestNRW() prolly.NodeReadWriter {
	ts := &chunks.TestStorage{}
	return prolly.NewNodeStore(ts.NewView())
}

var shared = pool.NewBuffPool()

func generateProllyTuples(size uint64) [][2]val.Tuple {
	src := rand.NewSource(0)

	tups := make([][2]val.Tuple, size)
	for i := range tups {
		// key
		var k [8]byte
		binary.LittleEndian.PutUint64(k[:], uint64(i))
		tups[i][0] = val.NewTuple(shared, k[:])

		// val
		var vv [5][]byte
		for i := range vv {
			vv[i] = make([]byte, 8)
			binary.LittleEndian.PutUint64(vv[i], uint64(src.Int63()))
		}
		tups[i][1] = val.NewTuple(shared, vv[:]...)
	}

	sort.Slice(tups, func(i, j int) bool {
		cmp := bytes.Compare(tups[i][0].GetField(0), tups[j][0].GetField(0))
		return cmp == -1
	})

	return tups
}

func generateTypesBench(size uint64) typesBench {
	ctx := context.Background()
	tups := generateTypesTuples(size)

	tt := make([]types.Value, len(tups)*2)
	for i := range tups {
		tt[i*2] = tups[i][0]
		tt[(i*2)+1] = tups[i][1]
	}

	m, err := types.NewMap(ctx, newTestVRW(), tt...)
	if err != nil {
		panic(err)
	}

	return typesBench{m: m, tups: tups}
}

func newTestVRW() types.ValueReadWriter {
	ts := &chunks.TestStorage{}
	return types.NewValueStore(ts.NewView())
}

func generateTypesTuples(size uint64) [][2]types.Tuple {
	src := rand.NewSource(0)

	// tags
	t0, t1, t2 := types.Uint(0), types.Uint(1), types.Uint(2)
	t3, t4, t5 := types.Uint(3), types.Uint(4), types.Uint(5)

	tups := make([][2]types.Tuple, size)
	for i := range tups {

		// key
		k := types.Int(i)
		tups[i][0], _ = types.NewTuple(types.Format_Default, t0, k)

		// val
		var vv [5 * 2]types.Value
		for i := 1; i < 10; i += 2 {
			vv[i] = types.Uint(uint64(src.Int63()))
		}
		vv[0], vv[2], vv[4], vv[6], vv[8] = t1, t2, t3, t4, t5

		tups[i][1], _ = types.NewTuple(types.Format_Default, vv[:]...)
	}

	return tups
}
