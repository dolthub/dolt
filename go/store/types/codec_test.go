// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"encoding/binary"
	"math/rand"
	"testing"

	//"github.com/dennwc/varint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnrolledDecode(t *testing.T) {
	const NumDecodes = 100000
	masks := []uint64{
		0xFF,
		0xFFFF,
		0xFFFFFF,
		0xFFFFFFFF,
		0xFFFFFFFFFF,
		0xFFFFFFFFFFFF,
		0xFFFFFFFFFFFFFF,
		0xFFFFFFFFFFFFFFFF}

	buf := make([]byte, 10)
	r := rand.New(rand.NewSource(0))
	for i := 0; i < NumDecodes; i++ {
		expectedVal := r.Uint64() & masks[i%8]
		expectedSize := binary.PutUvarint(buf, expectedVal)

		res, size := unrolledDecodeUVarint(buf)
		require.Equal(t, expectedSize, size)
		require.Equal(t, expectedVal, res)
	}

	for i := 0; i < NumDecodes; i++ {
		//non-negative
		expectedVal := int64(uint64(r.Int63()) & masks[i%8])
		expectedSize := binary.PutVarint(buf, expectedVal)

		res, size := unrolledDecodeVarint(buf)
		require.Equal(t, expectedSize, size)
		require.Equal(t, expectedVal, res)

		// negative
		expectedVal = -expectedVal
		expectedSize = binary.PutVarint(buf, expectedVal)

		res, size = unrolledDecodeVarint(buf)
		require.Equal(t, expectedSize, size)
		require.Equal(t, expectedVal, res)
	}
}

type ve struct {
	encoding []byte
	val      uint64
}

func initToDecode(b *testing.B, numItems int) []ve {
	toDecode := make([]ve, b.N*numItems)

	r := rand.New(rand.NewSource(0))
	for i := 0; i < b.N*numItems; i++ {
		desiredSize := (i % 10) + 1
		min := uint64(0)
		max := uint64(0x80)

		if desiredSize < 10 {
			for j := 0; j < desiredSize-1; j++ {
				min = max
				max <<= 7
			}
		} else {
			min = 0x8000000000000000
			max = 0xffffffffffffffff
		}

		val := min + (r.Uint64() % (max - min))
		buf := make([]byte, 10)
		size := binary.PutUvarint(buf, val)
		require.Equal(b, desiredSize, size, "%d. min: %x, val: %x, expected_size: %d, size: %d", i, min, val, desiredSize, size)

		toDecode[i] = ve{val: val, encoding: buf}
	}

	return toDecode
}

func BenchmarkUnrolledDecodeUVarint(b *testing.B) {
	const DecodesPerTest = 1000000
	toDecode := initToDecode(b, DecodesPerTest)

	type result struct {
		size int
		val  uint64
	}

	decodeBenchmark := []struct {
		name       string
		decodeFunc func([]byte) (uint64, int)
		results    []result
	}{
		{"binary.UVarint", binary.Uvarint, make([]result, len(toDecode))},
		{"unrolled", unrolledDecodeUVarint, make([]result, len(toDecode))},
		//{"dennwc.varint.UVarint", varint.Uvarint, make([]result, len(toDecode))},
		{"noBranch", varuintNoBranch, make([]result, len(toDecode))},
	}

	b.ResetTimer()
	for _, decodeBench := range decodeBenchmark {
		b.Run(decodeBench.name, func(b *testing.B) {
			for i, valAndEnc := range toDecode {
				val, size := decodeBench.decodeFunc(valAndEnc.encoding)
				decodeBench.results[i] = result{size, val}
			}
		})
	}
	b.StopTimer()

	for _, decodeBench := range decodeBenchmark {
		for i, valAndEnc := range toDecode {
			assert.Equal(b, valAndEnc.val, decodeBench.results[i].val)
			assert.Equal(b, i%10+1, decodeBench.results[i].size)
		}
	}
}

func varuintNoBranch(buf []byte) (uint64, int) {
	count := uint64(1)
	more := uint64(1)

	b := uint64(buf[0])
	x := more * (b & 0x7f)
	more &= (b & 0x80) >> 7

	count += more
	b = uint64(buf[1])
	x |= more * ((b & 0x7f) << 7)
	more &= (b & 0x80) >> 7

	count += more
	b = uint64(buf[2])
	x |= more * ((b & 0x7f) << 14)
	more &= (b & 0x80) >> 7

	count += more
	b = uint64(buf[3])
	x |= more * ((b & 0x7f) << 21)
	more &= (b & 0x80) >> 7

	count += more
	b = uint64(buf[4])
	x |= more * ((b & 0x7f) << 28)
	more &= (b & 0x80) >> 7

	count += more
	b = uint64(buf[5])
	x |= more * ((b & 0x7f) << 35)
	more &= (b & 0x80) >> 7

	count += more
	b = uint64(buf[6])
	x |= more * ((b & 0x7f) << 42)
	more &= (b & 0x80) >> 7

	count += more
	b = uint64(buf[7])
	x |= more * ((b & 0x7f) << 49)
	more &= (b & 0x80) >> 7

	count += more
	b = uint64(buf[8])
	x |= more * ((b & 0x7f) << 56)
	more &= (b & 0x80) >> 7

	count += more
	b = uint64(buf[9])
	x |= (more & b & 0x1) << 63
	more &= (b & 0x80) >> 7

	retCount := int(count) * (-2*int(more) + 1)
	return x, retCount
}
