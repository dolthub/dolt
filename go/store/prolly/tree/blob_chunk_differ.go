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

// blobChunkDiffer is an implementation of chunkDiffer for chunked blob values.
//
// Next() returns the first pair of non-matching leaf nodes in the tree on the first call (or EOF if the trees are
// identical). On subsequent calls, Next() returns all subsequent leaf nodes from each side of the diff, in order,
// regardless of whether they differ. This behavior is to support the primary use case of comparison, when only the
// first differing chunk typically matters for comparison purposes. An exception is when comparing collated strings,
// where the difference could occur mid-rune on a chunk boundary, so we need to load the next bytes, not the next diff.
type blobChunkDiffer struct {
	ns       NodeStore
	l        *blobDiffSide
	r        *blobDiffSide
	diverged bool
}

var _ chunkDiffer = (*blobChunkDiffer)(nil)

// blobDiffSide tracks one side of a chunk diff. A side is either an in-memory buffer (for inline
// or null adaptive values) or a stack of frames descending into a prolly tree (for out-of-band
// values). Only one of |inline| / |stack| is ever populated.
type blobDiffSide struct {
	stack    []blobDiffFrame
	inline   []byte
	consumed bool // true once an inline buffer has been emitted
}

func newBlobDiffSide(ctx context.Context, ns NodeStore, v val.AdaptiveValue) (*blobDiffSide, error) {
	side := &blobDiffSide{}

	if payload, ok := val.InlineValueBytes(v); ok {
		// Inline (or NULL — InlineValueBytes returns true with a nil payload for NULL).
		side.inline = payload
		return side, nil
	}
	addr, err := v.OutOfBandAddr()
	if err != nil {
		return nil, err
	}
	root, err := ns.Read(ctx, addr)
	if err != nil {
		return nil, err
	}
	if root != nil && !root.empty() {
		side.stack = []blobDiffFrame{{node: root}}
	}
	return side, nil
}

// blobDiffFrame is a cursor into a single node: |node| with the next child/value to visit at
// |idx|.
type blobDiffFrame struct {
	node *Node
	idx  int
}

// trim pops any frames whose children are exhausted, so that afterwards the top frame (if any)
// is guaranteed to have a child remaining at its current index.
func (side *blobDiffSide) trim() {
	for len(side.stack) > 0 {
		top := &side.stack[len(side.stack)-1]
		if top.idx < top.node.Count() {
			return
		}
		side.stack = side.stack[:len(side.stack)-1]
	}
}

// exhausted reports whether this side has no more bytes to yield.
func (side *blobDiffSide) exhausted() bool {
	if len(side.stack) > 0 {
		return false
	}
	return side.inline == nil || side.consumed
}

// internalTop returns the current frame if this side is positioned at an internal node with a
// child still to visit. trim must have been called first. The returned pointer is only valid
// until the stack is next mutated.
func (side *blobDiffSide) internalTop() (*blobDiffFrame, bool) {
	if len(side.stack) == 0 {
		return nil, false
	}
	top := &side.stack[len(side.stack)-1]
	if top.node.IsLeaf() {
		return nil, false
	}
	return top, true
}

// descend reads the child at the current index of the top frame and pushes it onto the stack,
// advancing the top frame past that child so the walk resumes correctly when the child is
// exhausted.
func (side *blobDiffSide) descend(ctx context.Context, ns NodeStore) error {
	top := &side.stack[len(side.stack)-1]
	addr := top.node.getAddress(top.idx)
	top.idx++
	child, err := ns.Read(ctx, addr)
	if err != nil {
		return err
	}
	side.stack = append(side.stack, blobDiffFrame{node: child})
	return nil
}

// nextLeaf advances this side to its next leaf chunk and returns the bytes, descending through
// internal nodes as needed. It returns nil once the side is exhausted.
func (side *blobDiffSide) nextLeaf(ctx context.Context, ns NodeStore) ([]byte, error) {
	if len(side.stack) == 0 {
		// Inline side: yield the buffer exactly once.
		if side.consumed || side.inline == nil {
			return nil, nil
		}
		side.consumed = true
		return side.inline, nil
	}
	for len(side.stack) > 0 {
		top := &side.stack[len(side.stack)-1]
		if top.idx >= top.node.Count() {
			side.stack = side.stack[:len(side.stack)-1]
			continue
		}
		if top.node.IsLeaf() {
			data := top.node.GetValue(top.idx)
			top.idx++
			return data, nil
		}
		if err := side.descend(ctx, ns); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// Next implements chunkDiffer.
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
	leftSide, err := newBlobDiffSide(ctx, ns, l)
	if err != nil {
		return nil, err
	}
	rightSide, err := newBlobDiffSide(ctx, ns, r)
	if err != nil {
		return nil, err
	}

	d := &blobChunkDiffer{
		ns: ns,
		l:  leftSide,
		r:  rightSide,
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
