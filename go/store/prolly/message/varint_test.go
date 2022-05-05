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

package message

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testRand = rand.New(rand.NewSource(1))

func TestRoundTripVarints(t *testing.T) {
	for k := 0; k < 1000; k++ {
		n := testRand.Intn(145) + 5

		counts := make([]uint64, n)
		sum := uint64(0)
		for i := range counts {
			c := testRand.Uint64() % math.MaxUint32
			counts[i] = c
			sum += c
		}
		assert.Equal(t, sum, sumSubtrees(counts))

		// round trip the array
		buf := make([]byte, maxEncodedSize(len(counts)))
		buf = encodeVarints(counts, buf)
		actual := decodeVarints(buf, make([]uint64, n))

		assert.Equal(t, counts, actual)
	}
}

func BenchmarkVarint(b *testing.B) {
	k := 150
	b.Run("level 1 subtree counts", func(b *testing.B) {
		benchmarkVarint(b, 100, 200, k)
	})
	b.Run("level 2 subtree counts", func(b *testing.B) {
		benchmarkVarint(b, uint64(100*k), uint64(200*k), k)
	})
	b.Run("level 3 subtree counts", func(b *testing.B) {
		benchmarkVarint(b, uint64(100*k*k), uint64(200*k*k), k)
	})
	b.Run("level 4 subtree counts", func(b *testing.B) {
		benchmarkVarint(b, uint64(100*k*k*k), uint64(200*k*k*k), k)
	})
}

func benchmarkVarint(b *testing.B, lo, hi uint64, k int) {
	const n = 10_000
	ints, bufs := makeBenchmarkData(lo, hi, k, n)

	name := fmt.Sprintf("benchmark encode [%d:%d]", lo, hi)
	b.Run(name, func(b *testing.B) {
		buf := make([]byte, maxEncodedSize(k))
		for i := 0; i < b.N; i++ {
			_ = encodeVarints(ints[i%n][:], buf)
		}
	})
	name = fmt.Sprintf("benchmark decode (mean size: %f)", meanSize(bufs))
	b.Run(name, func(b *testing.B) {
		ints := make([]uint64, k)
		for i := 0; i < b.N; i++ {
			_ = decodeVarints(bufs[i%n], ints)
		}
	})
}

func makeBenchmarkData(lo, hi uint64, k, n int) (ints [][]uint64, bufs [][]byte) {
	ints = make([][]uint64, n)
	bufs = make([][]byte, n)

	for i := range ints {
		ints[i] = make([]uint64, k)
		for j := range ints[i] {
			ints[i][j] = (testRand.Uint64() % (hi - lo)) + lo
		}
	}
	for i := range bufs {
		bufs[i] = make([]byte, maxEncodedSize(k))
		bufs[i] = encodeVarints(ints[i], bufs[i])
	}
	return
}

func meanSize(encoded [][]byte) float64 {
	var sumSz int
	for i := range encoded {
		sumSz += len(encoded[i])
	}
	return float64(sumSz) / float64(len(encoded))
}
