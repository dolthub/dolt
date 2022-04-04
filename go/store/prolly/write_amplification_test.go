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

package prolly

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

// mutation is a single point edit
type mutation struct {
	key, value val.Tuple
}

// mutationProvider creates a set of mutations from a given leaf node.
type mutationProvider interface {
	makeMutations(ctx context.Context, leaf Node) ([]mutation, error)
}

type deleteLastKey struct{}

func (lk deleteLastKey) makeMutations(ctx context.Context, leaf Node) ([]mutation, error) {
	c := int(leaf.count)
	return []mutation{{
		key:   val.Tuple(leaf.getKey(c - 1)),
		value: nil,
	}}, nil
}

type deleteSingleKey struct{}

func (rk deleteSingleKey) makeMutations(ctx context.Context, leaf Node) ([]mutation, error) {
	idx := testRand.Int() % int(leaf.count)
	return []mutation{{
		key:   val.Tuple(leaf.getKey(idx)),
		value: nil,
	}}, nil
}

func TestWriteAmplification(t *testing.T) {
	t.Run("Map<(uint),(uint,uint,uint)>", func(t *testing.T) {
		pm, _ := makeProllyMap(t, 100_000)
		before := pm.(Map)
		t.Run("delete last key", func(t *testing.T) {
			testWriteAmplification(t, before, deleteLastKey{})
		})
		t.Run("delete random key", func(t *testing.T) {
			testWriteAmplification(t, before, deleteSingleKey{})
		})
	})
}

func testWriteAmplification(t *testing.T, before Map, method mutationProvider) {
	ctx := context.Background()
	mutations := collectMutations(t, before, method)

	var counts, sizes samples
	for _, mut := range mutations {
		mm := before.Mutate()
		err := mm.Put(ctx, mut.key, mut.value)
		require.NoError(t, err)
		after, err := mm.Map(ctx)
		require.NoError(t, err)
		c, s := measureWriteAmplification(t, before, after)
		counts = append(counts, c)
		sizes = append(sizes, s)
	}
	countSummary, cardSummary, sizeSummary := summarizeTree(t, before)
	fmt.Printf("node count summary \t %s \n", countSummary)
	fmt.Printf("node card  summary \t %s \n", cardSummary)
	fmt.Printf("node size  summary \t %s \n\n", sizeSummary)
	fmt.Printf("node counts %s \n", counts.summary())
	fmt.Printf("node sizes  %s \n\n", sizes.summary())
}

func collectMutations(t *testing.T, before Map, method mutationProvider) (muts []mutation) {
	ctx := context.Background()
	err := before.WalkNodes(ctx, func(ctx context.Context, nd Node) error {
		if nd.leafNode() {
			mm, err := method.makeMutations(ctx, nd)
			require.NoError(t, err)
			muts = append(muts, mm...)
		}
		return nil
	})
	require.NoError(t, err)
	return
}

func measureWriteAmplification(t *testing.T, before, after Map) (count, size int) {
	ctx := context.Background()
	exclude := hash.NewHashSet()
	err := before.WalkAddresses(ctx, func(_ context.Context, addr hash.Hash) error {
		exclude.Insert(addr)
		return nil
	})
	require.NoError(t, err)

	novel := hash.NewHashSet()
	err = after.WalkAddresses(ctx, func(_ context.Context, addr hash.Hash) error {
		if !exclude.Has(addr) {
			novel.Insert(addr)
		}
		return nil
	})
	require.NoError(t, err)

	for addr := range novel {
		n, err := after.ns.Read(ctx, addr)
		require.NoError(t, err)
		size += n.size()
	}
	size += after.root.size()
	count = novel.Size() + 1
	return
}

func summarizeTree(t *testing.T, m Map) (string, string, string) {
	ctx := context.Background()

	sizeByLevel := make([]samples, m.Height())
	cardByLevel := make([]samples, m.Height())
	err := WalkNodes(ctx, m.root, m.ns, func(ctx context.Context, nd Node) error {
		lvl := nd.level()
		sizeByLevel[lvl] = append(sizeByLevel[lvl], nd.size())
		cardByLevel[lvl] = append(cardByLevel[lvl], int(nd.count))
		return nil
	})
	require.NoError(t, err)

	var sizes, cards, counts strings.Builder
	seenOne := false
	for i := m.root.level(); i >= 0; i-- {
		if seenOne {
			sizes.WriteString("\t")
			cards.WriteString("\t")
			counts.WriteString("\t")
		}
		seenOne = true
		sz, card := sizeByLevel[i], cardByLevel[i]
		counts.WriteString(fmt.Sprintf("level %d: %8d", i, int(len(card))))
		cards.WriteString(fmt.Sprintf("level %d: %8.2f", i, card.mean()))
		sizes.WriteString(fmt.Sprintf("level %d: %8.2f", i, sz.mean()))
	}
	return counts.String(), cards.String(), sizes.String()
}

type samples []int

func (s samples) count() float64 {
	return float64(len(s))
}

func (s samples) sum() (total float64) {
	for _, v := range s {
		total += float64(v)
	}
	return
}

func (s samples) mean() float64 {
	return s.sum() / float64(len(s))
}

func (s samples) stdDev() float64 {
	var acc float64
	u := s.mean()
	for _, v := range s {
		d := float64(v) - u
		acc += d * d
	}
	return math.Sqrt(acc / s.count())
}

func (s samples) sort() {
	sort.Ints(s)
}

func (s samples) percentiles() (p50, p90, p99, p999, p100 int) {
	s.sort()
	l := len(s)
	p50 = s[l/2]
	p90 = s[(l*9)/10]
	p99 = s[(l*99)/100]
	p999 = s[(l*999)/1000]
	p100 = s[l-1]
	return
}

func (s samples) summary() string {
	f := "mean: %8.2f \t stddev: %8.2f \t p50: %5d \t p90: %5d \t p99: %5d \t p99.9: %5d \t p100: %5d"
	p50, p90, p99, p999, p100 := s.percentiles()
	return fmt.Sprintf(f, s.mean(), s.stdDev(), p50, p90, p99, p999, p100)
}

func TestSamples(t *testing.T) {
	tests := []struct {
		data samples
		sum  float64
		mean float64
		std  float64
	}{
		{
			data: samples{1},
			sum:  1.0,
			mean: 1.0,
			std:  0.0,
		},
		{
			data: samples{1, 2, 3, 4, 5},
			sum:  15.0,
			mean: 3.0,
			std:  math.Sqrt(2),
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.sum, test.data.sum())
		assert.Equal(t, test.mean, test.data.mean())
		assert.Equal(t, test.std, test.data.stdDev())
	}
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
