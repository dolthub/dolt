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
	"math/rand"
	"sort"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

const (
	n            = 100_000
	minChunkSize = 1 << 10
	maxChunkSize = 1 << 14
)

type sampler struct {
	offset uint32
}

func (s *sampler) sample() (sz int) {
	for {
		if s.step() {
			break
		}
	}
	sz = int(s.offset)
	s.reset()
	return
}

func (s *sampler) step() bool {
	s.offset++

	if s.offset < minChunkSize {
		return false
	}
	if s.offset > maxChunkSize {
		return true
	}

	hash := randUint32()
	patt := patternFromOffset(s.offset)
	return hash&patt == patt
}

func (s *sampler) reset() {
	s.offset = 0
}

func patternFromOffset(offset uint32) uint32 {
	shift := 15 - (offset >> 10)
	return 1<<shift - 1
}

func randUint32() uint32 {
	return uint32(rand.Uint64())
}

func average(data []int) float64 {
	sum := float64(0)
	for _, d := range data {
		sum += float64(d)
	}
	return sum / float64(len(data))
}

func stddev(data []int) float64 {
	avg := average(data)
	acc := float64(0)
	for _, d := range data {
		delta := float64(d) - avg
		acc += (delta * delta)

	}
	variance := acc / float64(len(data))
	return math.Sqrt(variance)
}

func plotIntHistogram(data []int) {
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

	if err := p.Save(3*vg.Inch, 3*vg.Inch, "hist.png"); err != nil {
		panic(err)
	}
}

func main() {
	s := &sampler{}
	data := make([]int, n)
	for i := range data {
		data[i] = s.sample()
	}
	sort.Ints(data)
	plotIntHistogram(data)
	fmt.Println("mean: %f", average(data))
	fmt.Println("std: %f", stddev(data))
}
