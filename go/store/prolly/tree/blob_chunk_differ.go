package tree

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/store/val"
)

// blobChunkDiffer is an implementation of val.ChunkDiffer for chunked blob values
type blobChunkDiffer struct {
	ns   NodeStore
	l, r blobDiffSide
}

var _ val.ChunkDiffer = (*blobChunkDiffer)(nil)

// blobDiffSide tracks one side of a chunk diff. A side is either an in-memory buffer (for
// inline or null adaptive values) or a stack of frames into a prolly tree (for out-of-band
// values). Only one of |inline| / |stack| is non-empty at a time.
type blobDiffSide struct {
	stack    []blobDiffFrame
	inline   []byte
	consumed bool // true once an inline buffer has been emitted
}

type blobDiffFrame struct {
	node *Node
	idx  int
}

func (s *blobDiffSide) popExhausted() {
	for len(s.stack) > 0 {
		top := &s.stack[len(s.stack)-1]
		if top.idx < top.node.Count() {
			return
		}
		s.stack = s.stack[:len(s.stack)-1]
	}
}

func (s *blobDiffSide) exhausted() bool {
	if len(s.stack) > 0 {
		return false
	}
	return s.inline == nil || s.consumed
}

// nextLeafBytes advances this side to the next leaf chunk and returns its bytes. Returns
// nil when the side is exhausted; in that case the caller should also check exhausted().
func (s *blobDiffSide) nextLeafBytes(ctx context.Context, ns NodeStore) ([]byte, error) {
	if len(s.stack) == 0 {
		if s.consumed || s.inline == nil {
			return nil, nil
		}
		data := s.inline
		s.consumed = true
		return data, nil
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
		addr := top.node.getAddress(top.idx)
		top.idx++
		child, err := ns.Read(ctx, addr)
		if err != nil {
			return nil, err
		}
		s.stack = append(s.stack, blobDiffFrame{node: child})
	}
	return nil, nil
}

// trySkipMatchingSubtree skips both sides past the next pair of child subtrees if both sides
// are currently at internal nodes and the two child addresses match.
func (d *blobChunkDiffer) trySkipMatchingSubtree() bool {
	if len(d.l.stack) == 0 || len(d.r.stack) == 0 {
		return false
	}
	lTop := &d.l.stack[len(d.l.stack)-1]
	rTop := &d.r.stack[len(d.r.stack)-1]
	if lTop.node.IsLeaf() || rTop.node.IsLeaf() {
		return false
	}
	if lTop.idx >= lTop.node.Count() || rTop.idx >= rTop.node.Count() {
		return false
	}
	if lTop.node.getAddress(lTop.idx) != rTop.node.getAddress(rTop.idx) {
		return false
	}
	lTop.idx++
	rTop.idx++
	return true
}

// Next implements val.ChunkDiffer.
func (d *blobChunkDiffer) Next(ctx context.Context) ([]byte, []byte, error) {
	for {
		d.l.popExhausted()
		d.r.popExhausted()
		if d.l.exhausted() && d.r.exhausted() {
			return nil, nil, io.EOF
		}
		if d.trySkipMatchingSubtree() {
			continue
		}
		lChunk, err := d.l.nextLeafBytes(ctx, d.ns)
		if err != nil {
			return nil, nil, err
		}
		rChunk, err := d.r.nextLeafBytes(ctx, d.ns)
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
	// Fast path: identical out-of-band addresses imply identical contents.
	if len(d.l.stack) == 1 && len(d.r.stack) == 1 {
		lh := d.l.stack[0].node.HashOf()
		rh := d.r.stack[0].node.HashOf()
		if lh == rh {
			d.l.stack = nil
			d.r.stack = nil
		}
	}
	return d, nil
}

func loadBlobDiffSide(ctx context.Context, ns NodeStore, side *blobDiffSide, v val.AdaptiveValue) error {
	if payload, ok := val.InlineValueBytes(v); ok {
		// Inline (or NULL — InlineValueBytes returns true with nil payload for NULL).
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
