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

package ranges

import (
	"github.com/google/btree"
)

// MRQueue is a per-URL queue of GetRanges designed for multi-range
// HTTP dispatch. Unlike |Tree|, it does not coalesce ranges on insert;
// chunks are kept separate in a per-URL BTree (ordered by offset) and
// grouped at pop time into a single multi-range HTTP request.
//
// PopRequest is where the coalescing happens: adjacent or near-adjacent
// chunks (gap <= |slop|) are merged into a single "span group" emitted
// as one Range header entry; distant chunks become separate entries.
// The whole request is bounded by a total-bytes budget and a max
// span-group count (for header size).
//
// Intended for use when the remote supports multi-range byte range
// requests (e.g. CloudFront, which splits them into parallel upstream
// single-range requests). When the remote only supports single ranges,
// use |Tree| instead.
type MRQueue struct {
	byURL map[string]*btree.BTreeG[*GetRange]
	// urls tracks the order in which URLs first appeared. Round-robin
	// dispatch across URLs uses this as the rotation axis. Entries
	// are not removed eagerly when a URL drains; emptyClean sweeps
	// them on the next full rotation.
	urls  []string
	pos   int
	total int
}

func NewMRQueue() *MRQueue {
	return &MRQueue{byURL: make(map[string]*btree.BTreeG[*GetRange])}
}

func (q *MRQueue) Len() int {
	return q.total
}

// intern returns a canonical (shared) string equal to |s| when this
// MRQueue has already seen |s| as a URL, otherwise returns |s|
// unchanged. Deduping prevents multiple heap allocations of the same
// (often long, presigned) URL string across many inserts.
func (q *MRQueue) intern(s string) string {
	t, ok := q.byURL[s]
	if !ok || t.Len() == 0 {
		return s
	}
	var canonical string
	t.Ascend(func(gr *GetRange) bool {
		canonical = gr.Url
		return false
	})
	return canonical
}

func (q *MRQueue) Insert(url string, hash []byte, offset uint64, length uint32, dictOffset uint64, dictLength uint32) {
	url = q.intern(url)
	t, ok := q.byURL[url]
	if !ok {
		t = btree.NewG[*GetRange](64, GetRangeLess)
		q.byURL[url] = t
		q.urls = append(q.urls, url)
	}
	t.ReplaceOrInsert(&GetRange{
		Url:        url,
		Hash:       hash,
		Offset:     offset,
		Length:     length,
		DictOffset: dictOffset,
		DictLength: dictLength,
	})
	q.total++
}

// PopRequest assembles one multi-range HTTP request's worth of chunks
// from a single URL and removes them from the queue. Chunks are
// returned as groups: each group is a run of one or more chunks whose
// inter-chunk gaps are all <= |slop| and which will be sent as a
// single entry in the multi-range Range header (covering
// [group[0].Offset, last chunk end)).
//
// The request is bounded by |maxBytes| (sum of chunk lengths plus
// bridged slop bytes across all groups) and |maxRanges| (number of
// groups, i.e. entries in the Range header). At least one chunk is
// always emitted when the queue is non-empty, even if that chunk's
// length alone exceeds |maxBytes|.
//
// Returns:
//   - groups: [][]*GetRange, one slice per slop-bridged span.
//   - url:    the URL for this request.
//   - dark:   slop bytes bridged between chunks within groups. These
//             are bytes that will be downloaded by the server (within
//             a single byte range) but not delivered to any chunk
//             callback — the multi-range analogue of |Tree|'s dark
//             bytes from DeleteMaxRegion.
//
// When the queue is empty, returns (nil, "", 0).
func (q *MRQueue) PopRequest(maxBytes uint64, maxRanges int, slop uint64) ([][]*GetRange, string, uint64) {
	url := q.nextURL()
	if url == "" {
		return nil, "", 0
	}
	t := q.byURL[url]

	var groups [][]*GetRange
	var curGroup []*GetRange
	var curEnd uint64 // exclusive end offset of curGroup
	var totalBytes uint64
	var dark uint64
	var taken []*GetRange

	// Iterate ascending, deciding per-chunk whether to include it.
	// Stop early (return false) as soon as a chunk would exceed
	// budget; we can't mutate the BTree during Ascend, so deletions
	// are deferred to a second pass.
	t.Ascend(func(c *GetRange) bool {
		cLen := uint64(c.Length)
		if len(curGroup) == 0 {
			// First chunk in the request. Always take it,
			// even if alone it exceeds maxBytes — we must
			// make forward progress.
			curGroup = append(curGroup, c)
			curEnd = c.Offset + cLen
			totalBytes = cLen
			taken = append(taken, c)
			return true
		}
		gap := c.Offset - curEnd
		if gap <= slop {
			added := gap + cLen
			if totalBytes+added > maxBytes {
				return false
			}
			curGroup = append(curGroup, c)
			curEnd = c.Offset + cLen
			totalBytes += added
			dark += gap
			taken = append(taken, c)
			return true
		}
		// Starting a new group: +1 for the in-progress group we
		// haven't closed yet, +1 for the new one.
		if len(groups)+2 > maxRanges {
			return false
		}
		if totalBytes+cLen > maxBytes {
			return false
		}
		groups = append(groups, curGroup)
		curGroup = []*GetRange{c}
		curEnd = c.Offset + cLen
		totalBytes += cLen
		taken = append(taken, c)
		return true
	})
	if len(curGroup) > 0 {
		groups = append(groups, curGroup)
	}

	for _, c := range taken {
		t.Delete(c)
	}
	q.total -= len(taken)
	if t.Len() == 0 {
		delete(q.byURL, url)
	}

	return groups, url, dark
}

// nextURL returns the URL at (or after) the current round-robin
// position that has pending chunks. Empty URLs are cleaned up lazily
// as they are encountered. Advances the cursor past the returned URL
// so the next call rotates to the following URL. Returns "" when the
// queue is empty.
func (q *MRQueue) nextURL() string {
	if q.total == 0 {
		q.urls = nil
		q.pos = 0
		return ""
	}
	// Walk forward from q.pos, cleaning dead URLs, until we find one
	// that is still present in byURL. q.total > 0 guarantees at least
	// one survives.
	for {
		if q.pos >= len(q.urls) {
			q.pos = 0
		}
		u := q.urls[q.pos]
		if _, ok := q.byURL[u]; ok {
			q.pos++
			return u
		}
		// Dead URL; drop it from the ring.
		q.urls = append(q.urls[:q.pos], q.urls[q.pos+1:]...)
	}
}
