// Copyright 2026 Dolthub, Inc.
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

package netstats

import (
	"sort"
	"sync"
)

// darkCache is a shadow of every region ever dispatched to a download
// worker, used to measure the realistic ceiling on savings from a real
// dark-range cache. For each new chunk range arriving from
// StreamChunkLocations, we can ask whether its byte span is already
// contained in a previously-dispatched region — if so, a real cache
// serving from the already-downloaded dark bytes would have let us skip
// re-fetching this chunk.
//
// Regions dispatched from a ranges.Tree for the same URL are non-
// overlapping by construction (the tree coalesces on insert), so the
// per-URL list of spans is a simple sorted set of disjoint intervals.
type darkCache struct {
	mu   sync.Mutex
	sets map[string]*darkURLSet
}

type darkURLSet struct {
	// Sorted by start; intervals are non-overlapping.
	spans []darkSpan
}

type darkSpan struct {
	start, end uint64
}

func newDarkCache() *darkCache {
	return &darkCache{sets: make(map[string]*darkURLSet)}
}

// insert records that [start, end) on url was dispatched. Spans may be
// absorbed into or extend an existing span if they touch.
func (c *darkCache) insert(url string, start, end uint64) {
	if start >= end {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	set := c.sets[url]
	if set == nil {
		set = &darkURLSet{}
		c.sets[url] = set
	}
	// Find the first span whose start is strictly greater than the new
	// span's start. Candidates for overlap/merge are at idx-1 and idx.
	idx := sort.Search(len(set.spans), func(i int) bool {
		return set.spans[i].start > start
	})
	newSpan := darkSpan{start: start, end: end}
	// Merge with previous if it abuts or overlaps.
	if idx > 0 && set.spans[idx-1].end >= start {
		idx--
		if set.spans[idx].end > newSpan.end {
			newSpan.end = set.spans[idx].end
		}
		newSpan.start = set.spans[idx].start
		set.spans = append(set.spans[:idx], set.spans[idx+1:]...)
	}
	// Merge with any following spans that the (possibly extended) new
	// span now overlaps.
	for idx < len(set.spans) && set.spans[idx].start <= newSpan.end {
		if set.spans[idx].end > newSpan.end {
			newSpan.end = set.spans[idx].end
		}
		set.spans = append(set.spans[:idx], set.spans[idx+1:]...)
	}
	set.spans = append(set.spans, darkSpan{})
	copy(set.spans[idx+1:], set.spans[idx:])
	set.spans[idx] = newSpan
}

// contains reports whether [offset, offset+length) on url is fully
// covered by a previously-inserted span.
func (c *darkCache) contains(url string, offset, length uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	set := c.sets[url]
	if set == nil || len(set.spans) == 0 {
		return false
	}
	// Find the span with the largest start <= offset.
	idx := sort.Search(len(set.spans), func(i int) bool {
		return set.spans[i].start > offset
	})
	if idx == 0 {
		return false
	}
	prev := set.spans[idx-1]
	return prev.start <= offset && prev.end >= offset+length
}

// stats returns the current number of (url, span count, total bytes).
func (c *darkCache) stats() (urls int, spans int, bytes uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	urls = len(c.sets)
	for _, set := range c.sets {
		spans += len(set.spans)
		for _, s := range set.spans {
			bytes += s.end - s.start
		}
	}
	return
}
