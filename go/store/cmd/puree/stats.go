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

package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/dolthub/dolt/go/store/types"
)

func NewWriteAmplificationStats(height uint64) *WriteAmplificationStats {
	w := &WriteAmplificationStats{
		chunksByLevel: make([]*Hist, height+1),
		bytesByLevel:  make([]*Hist, height+1),
		chunks:        &Hist{},
		bytes:         &Hist{},
	}

	for i := range w.chunksByLevel {
		w.chunksByLevel[i] = &Hist{}
		w.bytesByLevel[i] = &Hist{}
	}

	return w
}

// WriteAmplificationStats records sequenceChunker write writeSizes by tree level
type WriteAmplificationStats struct {
	chunksByLevel []*Hist
	bytesByLevel  []*Hist

	chunks, bytes *Hist
}

func (was *WriteAmplificationStats) Sample(stats []types.WriteStats) {
	for i := len(was.chunksByLevel); i < len(stats); i++ {
		was.chunksByLevel = append(was.chunksByLevel, &Hist{})
		was.bytesByLevel = append(was.bytesByLevel, &Hist{})
	}

	var chunks, bytes int
	for i, level := range stats {
		c := len(level)
		was.chunksByLevel[i].add(c)
		chunks += c

		var b int
		for _, write := range level {
			b += int(write)
		}
		was.bytesByLevel[i].add(b)
		bytes += b
	}
	was.chunks.add(chunks)
	was.bytes.add(bytes)
}

func (was WriteAmplificationStats) Summary() string {
	var s strings.Builder
	s.WriteString("chunks written:")
	s.WriteString(was.chunks.String())
	s.WriteRune('\n')
	s.WriteString("bytes  written:")
	s.WriteString(was.bytes.String())
	s.WriteRune('\n')
	return s.String()
}

func (was WriteAmplificationStats) SummaryByLevel() string {
	var s strings.Builder

	for level, h := range was.chunksByLevel {
		s.WriteString(fmt.Sprintf("level %d chunks written: ", level))
		s.WriteString(h.String())
		s.WriteRune('\n')
	}
	s.WriteString("total chunks written:   ")
	s.WriteString(was.chunks.String())
	s.WriteRune('\n')

	for level, h := range was.bytesByLevel {
		s.WriteString(fmt.Sprintf("level %d bytes  written: ", level))
		s.WriteString(h.String())
		s.WriteRune('\n')
	}
	s.WriteString("total bytes  written:   ")
	s.WriteString(was.bytes.String())
	s.WriteRune('\n')
	return s.String()
}

type Hist struct {
	vs     []int
	sorted bool
}

func (h *Hist) add(i int) {
	h.vs = append(h.vs, i)
	h.sorted = false
}

func (h *Hist) merge(hp Hist) {
	h.vs = append(h.vs, hp.vs...)
	h.sorted = false
}

func (h *Hist) perc(p float32) int {
	if !h.sorted {
		sort.Ints(h.vs)
		h.sorted = true
	}
	i := int(float32(len(h.vs)) * p)
	if i >= len(h.vs) {
		i = len(h.vs) - 1
	}
	return h.vs[i]
}

func (h *Hist) avg() float64 {
	// we can just use an int64 here because these are byte sizes of things
	// that exist on a disk...
	var sum int64
	for _, v := range h.vs {
		sum += int64(v)
	}
	return float64(sum) / float64(len(h.vs))
}

func (h *Hist) sum() (s int) {
	for _, v := range h.vs {
		s += v
	}
	return
}

func (h *Hist) count() int {
	return len(h.vs)
}

func (h *Hist) String() string {
	return fmt.Sprintf("avg: %10.2f, p10: %10d, p50: %10d, p90: %10d, p99: %10d, p99.9: %10d, p100: %10d",
		h.avg(), h.perc(.1), h.perc(.5), h.perc(.9), h.perc(.99), h.perc(.999), h.perc(1))
}
