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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package prolly

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

const keySplitterScale = 100_000

func TestKeySplitterDistribution(t *testing.T) {
	factory := newKeySplitter
	t.Run("24", func(t *testing.T) {
		nd, ns := makeProllyTreeWithSizes(t, factory, 8, 16)
		printTreeSummaryByLevel(t, nd, ns)
		//plotNodeSizeDistribution(t, "prolly_8_16.png", nd, ns)
	})
	t.Run("sizes (8,54)", func(t *testing.T) {
		for sz := 8; sz <= 54; sz++ {
			fmt.Println(fmt.Sprintf("summary for map size %d", sz))
			nd, ns := makeProllyTreeWithSizes(t, factory, sz, 0)
			printTreeSummaryByLevel(t, nd, ns)
			fmt.Println()
		}
	})
}

func TestRoundLog2(t *testing.T) {
	for i := 1; i < 16384; i++ {
		exp := int(math.Round(math.Log2(float64(i))))
		act := int(roundLog2(uint32(i)))
		assert.Equal(t, exp, act)
	}
}

func makeProllyTreeWithSizes(t *testing.T, fact splitterFactory, keySz, valSz int) (nd Node, ns NodeStore) {
	pro := gaussianItems{
		keyMean: float64(keySz),
		keyStd:  float64(keySz) / 4,
		valMean: float64(valSz),
		valStd:  float64(valSz) / 4,
		r:       testRand,
	}

	ctx := context.Background()
	ns = newTestNodeStore()
	chunker, err := newEmptyTreeChunker(ctx, ns, fact)
	require.NoError(t, err)

	for i := 0; i < keySplitterScale; i++ {
		k, v := pro.Next()
		_, err = chunker.append(ctx, k, v, 1)
		require.NoError(t, err)
	}

	nd, err = chunker.Done(ctx)
	require.NoError(t, err)
	return
}

func printTreeSummaryByLevel(t *testing.T, nd Node, ns NodeStore) {
	ctx := context.Background()

	sizeByLevel := make([]samples, nd.level()+1)
	cardByLevel := make([]samples, nd.level()+1)
	err := WalkNodes(ctx, nd, ns, func(ctx context.Context, nd Node) error {
		lvl := nd.level()
		sizeByLevel[lvl] = append(sizeByLevel[lvl], nd.size())
		cardByLevel[lvl] = append(cardByLevel[lvl], int(nd.count))
		return nil
	})
	require.NoError(t, err)

	fmt.Println("pre-edit map summary: ")
	fmt.Println("| level | count | avg size \t  p50 \t  p90 \t p100 | avg card \t  p50 \t  p90 \t p100 |")
	for i := nd.level(); i >= 0; i-- {
		sizes, cards := sizeByLevel[i], cardByLevel[i]
		sp50, _, sp90, _, sp100 := sizes.percentiles()
		cp50, _, cp90, _, cp100 := cards.percentiles()
		fmt.Printf("| %5d | %5d | %8.2f \t %4d \t %4d \t %4d | %8.2f \t %4d \t %4d \t %4d |\n",
			i, len(cards), sizes.mean(), sp50, sp90, sp100, cards.mean(), cp50, cp90, cp100)
	}
	fmt.Println()
}

func plotNodeSizeDistribution(t *testing.T, name string, nd Node, ns NodeStore) {
	ctx := context.Background()
	data := make([]int, 0, 1024)
	err := WalkNodes(ctx, nd, ns, func(ctx context.Context, nd Node) error {
		data = append(data, nd.size())
		return nil
	})
	require.NoError(t, err)
	plotIntHistogram(name, data)
}

func plotIntHistogram(name string, data []int) {
	var values plotter.Values
	for _, d := range data {
		values = append(values, float64(d))
	}

	p := plot.New()
	p.Title.Text = "histogram plot"

	hist, err := plotter.NewHist(values, 100)
	if err != nil {
		panic(err)
	}
	p.Add(hist)

	if err := p.Save(3*vg.Inch, 3*vg.Inch, name); err != nil {
		panic(err)
	}
}

type itemProvider interface {
	Next() (key, value nodeItem)
}

type gaussianItems struct {
	keyMean, keyStd float64
	valMean, valStd float64
	r               *rand.Rand
}

func (g gaussianItems) Next() (key, value nodeItem) {
	key = make(nodeItem, g.sample(g.keyMean, g.keyStd))
	value = make(nodeItem, g.sample(g.valMean, g.valStd))
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
