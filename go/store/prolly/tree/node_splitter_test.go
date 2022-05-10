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

package tree

import (
	"context"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/prolly/message"
)

func init() {
	benchData = make([][24]byte, 100_000)
	buf := make([]byte, 24*100_000)
	rand.Read(buf)
	for i := range benchData {
		start, stop := i*24, (i+1)*24
		copy(benchData[i][:], buf[start:stop])
	}
}

var benchData [][24]byte

func BenchmarkRollingHashSplitter(b *testing.B) {
	benchmarkNodeSplitter(b, newRollingHashSplitter(0))
}

func BenchmarkKeySplitter(b *testing.B) {
	benchmarkNodeSplitter(b, newKeySplitter(0))
}

func benchmarkNodeSplitter(b *testing.B, split nodeSplitter) {
	for i := 0; i < b.N; i++ {
		j := i % len(benchData)
		err := split.Append(benchData[j][:8], benchData[j][8:])
		assert.NoError(b, err)
		if split.CrossedBoundary() {
			split.Reset()
		}
	}
}

func TestKeySplitterDistribution(t *testing.T) {
	t.Skip("unskip for metrics")

	factory := newKeySplitter
	t.Run("plot node distribution for item Size 24", func(t *testing.T) {
		scale := 1_000_000
		nd, ns := makeProllyTreeWithSizes(t, factory, scale, 8, 16)
		PrintTreeSummaryByLevel(t, nd, ns)
		plotNodeSizeDistribution(t, "prolly_8_16.png", nd, ns)
	})
	t.Run("summarize node distribution for item sizes (8,54)", func(t *testing.T) {
		for sz := 8; sz <= 54; sz++ {
			fmt.Println(fmt.Sprintf("Summary for map Size %d", sz))
			nd, ns := makeProllyTreeWithSizes(t, factory, 100_000, sz, sz)
			PrintTreeSummaryByLevel(t, nd, ns)
			fmt.Println()
		}
	})
	t.Run("plot node distribution for item sizes (8,54)", func(t *testing.T) {
		var cumulative Samples
		for sz := 8; sz <= 54; sz++ {
			nd, ns := makeProllyTreeWithSizes(t, factory, 100_000, sz, sz)
			data, err := measureTreeNodes(nd, ns)
			require.NoError(t, err)
			cumulative = append(cumulative, data...)
		}
		fmt.Println(cumulative.Summary())
		plotIntHistogram("cumulative_node_sizes_8-54.png", cumulative)
	})
}

func makeProllyTreeWithSizes(t *testing.T, fact splitterFactory, scale, keySz, valSz int) (nd Node, ns NodeStore) {
	pro := gaussianItems{
		keyMean: float64(keySz),
		keyStd:  float64(keySz) / 4,
		valMean: float64(valSz),
		valStd:  float64(valSz) / 4,
		r:       testRand,
	}

	ctx := context.Background()
	ns = NewTestNodeStore()
	serializer := message.ProllyMapSerializer{Pool: ns.Pool()}
	chunker, err := newEmptyChunker(ctx, ns, serializer)
	require.NoError(t, err)

	for i := 0; i < scale; i++ {
		k, v := pro.Next()
		_, err = chunker.append(ctx, k, v, 1)
		require.NoError(t, err)
	}

	nd, err = chunker.Done(ctx)
	require.NoError(t, err)
	return
}

type itemProvider interface {
	Next() (key, value Item)
}

type gaussianItems struct {
	keyMean, keyStd float64
	valMean, valStd float64
	r               *rand.Rand
}

func (g gaussianItems) Next() (key, value Item) {
	key = make(Item, g.sample(g.keyMean, g.keyStd))
	value = make(Item, g.sample(g.valMean, g.valStd))
	rand.Read(key)
	rand.Read(value)
	return
}

func (g gaussianItems) sample(mean, std float64) (s int) {
	s = int(math.Round(g.r.NormFloat64()*std + mean))
	if s < 0 {
		s = 0
	}
	return
}

type staticItems struct {
	key, value int
}

func (s staticItems) Next() (key, value Item) {
	key = make(Item, s.key)
	value = make(Item, s.value)
	rand.Read(key)
	rand.Read(value)
	return
}

func TestRoundLog2(t *testing.T) {
	for i := 1; i < 16384; i++ {
		exp := int(math.Round(math.Log2(float64(i))))
		act := int(roundLog2(uint32(i)))
		assert.Equal(t, exp, act)
	}
}

const (
	// log2MidPoint is 2^31.5
	log2MidPoint = 0b10110101000001001111001100110011
)

// roundLog2 is an optimized version of
// uint32(math.Round(math.Log2(sz)))
// note: not currently used in any splitter
func roundLog2(sz uint32) (lg uint32) {
	// invariant: |sz| > 1
	lg = uint32(bits.Len32(sz) - 1)
	if sz > (log2MidPoint >> (31 - lg)) {
		lg++
	}
	return
}
