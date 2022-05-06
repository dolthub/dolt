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
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testRand = rand.New(rand.NewSource(1))

func TestVarint(t *testing.T) {
	t.Run("min delta varint", func(t *testing.T) {
		testRoundTripVarints(t, minDeltaCodec{})
	})
	t.Run("mean delta varint", func(t *testing.T) {
		testRoundTripVarints(t, meanDeltaCodec{})
	})
	t.Run("direct varint", func(t *testing.T) {
		testRoundTripVarints(t, directCodec{})
	})
	t.Run("signed delta varint", func(t *testing.T) {
		testRoundTripVarints(t, signedDeltaCodec{})
	})
}

func BenchmarkVarint(b *testing.B) {
	b.Run("signed delta varint", func(b *testing.B) {
		benchmarkVarintCodec(b, signedDeltaCodec{})
	})
	b.Run("min delta varint", func(b *testing.B) {
		benchmarkVarintCodec(b, minDeltaCodec{})
	})
	b.Run("mean delta varint", func(b *testing.B) {
		benchmarkVarintCodec(b, meanDeltaCodec{})
	})
	b.Run("direct varint", func(tb *testing.B) {
		benchmarkVarintCodec(b, directCodec{})
	})

}

type codec interface {
	encode(ints []uint64, buf []byte) []byte
	decode(buf []byte, ints []uint64) []uint64
	maxSize(n int) int
}

type minDeltaCodec struct{}

func (d minDeltaCodec) encode(ints []uint64, buf []byte) []byte {
	return encodeMinDeltas(ints, buf)
}

func (d minDeltaCodec) decode(buf []byte, ints []uint64) []uint64 {
	return decodeMinDeltas(buf, ints)
}

func (d minDeltaCodec) maxSize(n int) int {
	return maxEncodedSize(n)
}

type meanDeltaCodec struct{}

func (d meanDeltaCodec) encode(ints []uint64, buf []byte) []byte {
	return encodeMeanDeltas(ints, buf)
}

func (d meanDeltaCodec) decode(buf []byte, ints []uint64) []uint64 {
	return decodeMeanDeltas(buf, ints)
}

func (d meanDeltaCodec) maxSize(n int) int {
	return maxEncodedSize(n)
}

type directCodec struct{}

func (d directCodec) encode(ints []uint64, buf []byte) []byte {
	return encodeVarintDirect(ints, buf)
}

func (d directCodec) decode(buf []byte, ints []uint64) []uint64 {
	return decodeVarintDirect(buf, ints)
}

func (d directCodec) maxSize(n int) int {
	return n * binary.MaxVarintLen64
}

type signedDeltaCodec struct{}

func (d signedDeltaCodec) encode(ints []uint64, buf []byte) []byte {
	return endcodeSignedDeltas(ints, buf)
}

func (d signedDeltaCodec) decode(buf []byte, ints []uint64) []uint64 {
	return decodeSignedDeltas(buf, ints)
}

func (d signedDeltaCodec) maxSize(n int) int {
	return n * binary.MaxVarintLen64
}

func testRoundTripVarints(t *testing.T, c codec) {
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
		buf := make([]byte, c.maxSize(len(counts)))
		buf = c.encode(counts, buf)
		actual := c.decode(buf, make([]uint64, n))

		assert.Equal(t, counts, actual)
	}
}

func benchmarkVarintCodec(b *testing.B, c codec) {
	k := 150 // branching factor

	b.Run("level 1 subtree counts", func(b *testing.B) {
		mean := uint64(k)
		benchmarkVarint(b, mean, mean/4, k, c)
	})
	b.Run("level 2 subtree counts", func(b *testing.B) {
		mean := uint64(k * k)
		benchmarkVarint(b, mean, mean/4, k, c)
	})
	b.Run("level 3 subtree counts", func(b *testing.B) {
		mean := uint64(k * k * k)
		benchmarkVarint(b, mean, mean/4, k, c)
	})
	b.Run("level 4 subtree counts", func(b *testing.B) {
		mean := uint64(k * k * k * k)
		benchmarkVarint(b, mean, mean/4, k, c)
	})
}

func benchmarkVarint(b *testing.B, mean, std uint64, k int, c codec) {
	const n = 1000
	ints, bufs := makeBenchmarkData(mean, std, k, n, c)

	name := fmt.Sprintf("benchmark encode (mean: %d std: %d)", mean, std)
	b.Run(name, func(b *testing.B) {
		buf := make([]byte, c.maxSize(k))
		for i := 0; i < b.N; i++ {
			_ = c.encode(ints[i%n][:], buf)
		}
	})
	name = fmt.Sprintf("benchmark decode (mean size: %f)", meanSize(bufs))
	b.Run(name, func(b *testing.B) {
		ints := make([]uint64, k)
		for i := 0; i < b.N; i++ {
			_ = c.decode(bufs[i%n], ints)
		}
	})
}

func makeBenchmarkData(mean, std uint64, k, n int, c codec) (ints [][]uint64, bufs [][]byte) {
	ints = make([][]uint64, n)
	bufs = make([][]byte, n)

	for i := range ints {
		ints[i] = make([]uint64, k)
		for j := range ints[i] {
			ints[i][j] = gaussian(float64(mean), float64(std))
		}
	}
	for i := range bufs {
		bufs[i] = make([]byte, c.maxSize(k))
		bufs[i] = c.encode(ints[i], bufs[i])
	}
	return
}

func gaussian(mean, std float64) uint64 {
	return uint64(testRand.NormFloat64()*std + mean)
}

func meanSize(encoded [][]byte) float64 {
	var sumSz int
	for i := range encoded {
		sumSz += len(encoded[i])
	}
	return float64(sumSz) / float64(len(encoded))
}

func encodeVarintDirect(ints []uint64, buf []byte) []byte {
	pos := 0
	for i := range ints {
		pos += binary.PutUvarint(buf[pos:], ints[i])
	}
	return buf[:pos]
}

func decodeVarintDirect(buf []byte, ints []uint64) []uint64 {
	for i := range ints {
		var n int
		ints[i], n = binary.Uvarint(buf)
		buf = buf[n:]
	}
	assertTrue(len(buf) == 0)
	return ints
}

// encodeMinDeltas encodes an unsorted array |ints|.
// The encoding format attempts to minimize encoded size by
// first finding and encoding the minimum value of |ints|
// and then encoding the difference between each value and
// that minimum.
func encodeMinDeltas(ints []uint64, buf []byte) []byte {
	min := uint64(math.MaxUint64)
	for i := range ints {
		if min > ints[i] {
			min = ints[i]
		}
	}

	pos := 0
	pos += binary.PutUvarint(buf[pos:], min)

	for _, count := range ints {
		delta := count - min
		pos += binary.PutUvarint(buf[pos:], delta)
	}
	return buf[:pos]
}

// decodeMinDeltas decodes an array of ints that were
// previously encoded with encodeMinDeltas.
func decodeMinDeltas(buf []byte, ints []uint64) []uint64 {
	min, k := binary.Uvarint(buf)
	buf = buf[k:]
	for i := range ints {
		delta, k := binary.Uvarint(buf)
		buf = buf[k:]
		ints[i] = min + delta
	}
	assertTrue(len(buf) == 0)
	return ints
}

func encodeMeanDeltas(ints []uint64, buf []byte) []byte {
	var sum int64
	for i := range ints {
		sum += int64(ints[i])
	}
	mean := sum / int64(len(ints))

	pos := 0
	pos += binary.PutVarint(buf[pos:], mean)

	for _, count := range ints {
		delta := int64(count) - mean
		pos += binary.PutVarint(buf[pos:], delta)
	}
	return buf[:pos]
}

func decodeMeanDeltas(buf []byte, ints []uint64) []uint64 {
	mean, k := binary.Varint(buf)
	buf = buf[k:]
	for i := range ints {
		delta, k := binary.Varint(buf)
		buf = buf[k:]
		ints[i] = uint64(mean + delta)
	}
	assertTrue(len(buf) == 0)
	return ints
}
