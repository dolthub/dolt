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

package tree

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/store/val"
)

// blobChunkDiffer is an implementation of val.ChunkDiffer for chunked blob values.
//
// This implementation may return identical chunks as diffs after the first differing chunk is found. It
// treats the entire remainder of the byte streams after the first diverging chunks to be part of the diff, since
// depending on the chunking algorithm used the byte offsets of those identical chunks may be different.
// We typically only care about the first diff chunk returned by these (the primary use case is ordering comparisons),
// but be advised.
type blobChunkDiffer struct {
	ns NodeStore
	l  blobDiffSide
	r  blobDiffSide
	// diverged becomes true once the aligned-prefix phase ends. From then on the differ only
	// drains leaves and never skips, because the two streams are no longer guaranteed aligned.
	diverged bool
}

var _ ChunkDiffer = (*blobChunkDiffer)(nil)

// blobDiffSide tracks one side of a chunk diff. A side is either an in-memory buffer (for inline
// or null adaptive values) or a stack of frames descending into a prolly tree (for out-of-band
// values). Only one of |inline| / |stack| is ever populated.
type blobDiffSide struct {
	stack    []blobDiffFrame
	inline   []byte
	consumed bool // true once an inline buffer has been emitted
}

// blobDiffFrame is a cursor into a single node: |node| with the next child/value to visit at
// |idx|.
type blobDiffFrame struct {
	node *Node
	idx  int
}

// trim pops any frames whose children are exhausted, so that afterwards the top frame (if any)
// is guaranteed to have a child remaining at its current index.
func (s *blobDiffSide) trim() {
	for len(s.stack) > 0 {
		top := &s.stack[len(s.stack)-1]
		if top.idx < top.node.Count() {
			return
		}
		s.stack = s.stack[:len(s.stack)-1]
	}
}

// exhausted reports whether this side has no more bytes to yield.
func (s *blobDiffSide) exhausted() bool {
	if len(s.stack) > 0 {
		return false
	}
	return s.inline == nil || s.consumed
}

// internalTop returns the current frame if this side is positioned at an internal node with a
// child still to visit. trim must have been called first. The returned pointer is only valid
// until the stack is next mutated.
func (s *blobDiffSide) internalTop() (*blobDiffFrame, bool) {
	if len(s.stack) == 0 {
		return nil, false
	}
	top := &s.stack[len(s.stack)-1]
	if top.node.IsLeaf() {
		return nil, false
	}
	return top, true
}

// descend reads the child at the current index of the top frame and pushes it onto the stack,
// advancing the top frame past that child so the walk resumes correctly when the child is
// exhausted.
func (s *blobDiffSide) descend(ctx context.Context, ns NodeStore) error {
	top := &s.stack[len(s.stack)-1]
	addr := top.node.getAddress(top.idx)
	top.idx++
	child, err := ns.Read(ctx, addr)
	if err != nil {
		return err
	}
	s.stack = append(s.stack, blobDiffFrame{node: child})
	return nil
}

// nextLeaf advances this side to its next leaf chunk and returns the bytes, descending through
// internal nodes as needed. It returns nil once the side is exhausted.
func (s *blobDiffSide) nextLeaf(ctx context.Context, ns NodeStore) ([]byte, error) {
	if len(s.stack) == 0 {
		// Inline side: yield the buffer exactly once.
		if s.consumed || s.inline == nil {
			return nil, nil
		}
		s.consumed = true
		return s.inline, nil
	}
	for len(s.stack) > 0 {
		top := &s.stack[len(s.stack)-1]
		if top.idx >= top.node.Count() {
			s.stack = s.stack[:len(s.stack)-1]
			continue
		}
		if top.node.IsLeaf() {
			data := top.node.GetValue(top.idx)
			top.idx++
			return data, nil
		}
		if err := s.descend(ctx, ns); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// Next implements val.ChunkDiffer.
// This method may return identical byte slices after the first one. See comment on blobChunkDiffer for details.
func (d *blobChunkDiffer) Next(ctx context.Context) ([]byte, []byte, error) {
	for {
		d.l.trim()
		d.r.trim()

		if d.l.exhausted() && d.r.exhausted() {
			return nil, nil, io.EOF
		}

		if !d.diverged {
			lTop, lInternal := d.l.internalTop()
			rTop, rInternal := d.r.internalTop()
			if lInternal && rInternal && lTop.node.Level() == rTop.node.Level() {
				if lTop.node.getAddress(lTop.idx) == rTop.node.getAddress(rTop.idx) {
					// Identical subtrees: skip both.
					lTop.idx++
					rTop.idx++
					continue
				}

				// Differing subtrees at the same level: descend into both and keep
				// looking for shared content one level down.
				if err := d.l.descend(ctx, d.ns); err != nil {
					return nil, nil, err
				}
				if err := d.r.descend(ctx, d.ns); err != nil {
					return nil, nil, err
				}
				continue
			}
			// The cursors can no longer be aligned (a leaf was reached, the levels differ,
			// or a side is inline). Everything from here on must be streamed.
			d.diverged = true
		}

		lChunk, err := d.l.nextLeaf(ctx, d.ns)
		if err != nil {
			return nil, nil, err
		}
		rChunk, err := d.r.nextLeaf(ctx, d.ns)
		if err != nil {
			return nil, nil, err
		}
		if lChunk == nil && rChunk == nil {
			return nil, nil, io.EOF
		}
		return lChunk, rChunk, nil
	}
}

// newBlobChunkDiffer constructs a differ over two adaptive values.
func newBlobChunkDiffer(ctx context.Context, ns NodeStore, l, r val.AdaptiveValue) (*blobChunkDiffer, error) {
	d := &blobChunkDiffer{ns: ns}
	if err := loadBlobDiffSide(ctx, ns, &d.l, l); err != nil {
		return nil, err
	}
	if err := loadBlobDiffSide(ctx, ns, &d.r, r); err != nil {
		return nil, err
	}
	// Fast path: identical out-of-band root addresses imply identical contents.
	if len(d.l.stack) == 1 && len(d.r.stack) == 1 {
		if d.l.stack[0].node.HashOf() == d.r.stack[0].node.HashOf() {
			d.l.stack = nil
			d.r.stack = nil
		}
	}
	return d, nil
}

func loadBlobDiffSide(ctx context.Context, ns NodeStore, side *blobDiffSide, v val.AdaptiveValue) error {
	if payload, ok := val.InlineValueBytes(v); ok {
		// Inline (or NULL — InlineValueBytes returns true with a nil payload for NULL).
		side.inline = payload
		return nil
	}
	addr, err := v.OutOfBandAddr()
	if err != nil {
		return err
	}
	root, err := ns.Read(ctx, addr)
	if err != nil {
		return err
	}
	if root != nil && !root.empty() {
		side.stack = []blobDiffFrame{{node: root}}
	}
	return nil
}
