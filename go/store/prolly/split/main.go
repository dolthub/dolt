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

package main

import (
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"sort"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

const (
	minChunkSize = 1 << 10
	maxChunkSize = 1 << 14
)

func collectData(rowSize, numSamples int) (data []int) {
	data = make([]int, numSamples)
	for i := range data {
		data[i] = sample(rowSize)
	}
	sort.Ints(data)
	return
}

func sample(rowSize int) int {
	count := minChunkSize / rowSize
	for {
		if count > (maxChunkSize / rowSize) {
			break
		}

		hash := randUint32()
		patt := patternFromCount(count, rowSize)
		if hash&patt == patt {
			break
		}

		count++
	}
	return count * rowSize
}

func randUint32() uint32 {
	return uint32(rand.Uint64())
}

func patternFromCount(count, rowSize int) uint32 {
	hi := uint32(16 - ceilingLog2(rowSize))
	lo := uint32(10 - ceilingLog2(rowSize))
	shift := hi - (uint32(count) >> lo)
	return 1<<shift - 1
}

func mean(data []int) float64 {
	sum := float64(0)
	for _, d := range data {
		sum += float64(d)
	}
	return sum / float64(len(data))
}

// ceil(Log2(sz))
func ceilingLog2(sz int) int {
	// invariant: |sz| > 1
	return bits.Len32(uint32(sz - 1))
}

func stddev(data []int) float64 {
	avg := mean(data)
	acc := float64(0)
	for _, d := range data {
		delta := float64(d) - avg
		acc += (delta * delta)
	}
	variance := acc / float64(len(data))
	return math.Sqrt(variance)
}

func main() {
	const numSamples = 10_000

	sizes := []int{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
		11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
		21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
		31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
	}

	for _, sz := range sizes {
		data := collectData(sz, numSamples)
		m, s := mean(data), stddev(data)
		fmt.Printf("row size: %d, mean: %f \t std: %f \n", sz, m, s)
	}

	plotIntHistogram("row_size_24.png", collectData(24, numSamples))
	plotIntHistogram("row_size_180.png", collectData(180, numSamples))
	plotIntHistogram("row_size_500.png", collectData(500, numSamples))
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
