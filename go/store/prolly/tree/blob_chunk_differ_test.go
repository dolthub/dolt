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
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
)

// buildBlobTree writes |data| as a fixed-chunk blob tree and returns the root node. An empty
// blob yields a nil root (an exhausted side).
func buildBlobTree(t *testing.T, ctx context.Context, ns NodeStore, chunkSize int, data []byte) *Node {
	b, err := NewBlobBuilder(chunkSize)
	require.NoError(t, err)
	b.SetNodeStore(ns)
	b.Init(len(data))
	root, _, err := b.Chunk(ctx, bytes.NewReader(data))
	require.NoError(t, err)
	return root
}

// differFromRoots builds a blobChunkDiffer directly over two root nodes, bypassing the
// AdaptiveValue plumbing so the tree-walking logic can be exercised in isolation.
func differFromRoots(ns NodeStore, l, r *Node) *blobChunkDiffer {
	d := &blobChunkDiffer{
		ns:       ns,
		l:        &blobDiffSide{},
		r:        &blobDiffSide{},
		diverged: false,
	}
	if l != nil && !l.empty() {
		d.l.stack = []blobDiffFrame{{node: l}}
	}
	if r != nil && !r.empty() {
		d.r.stack = []blobDiffFrame{{node: r}}
	}
	return d
}

// drainDiffer runs the differ to EOF and returns the concatenated bytes emitted for each side.
func drainDiffer(t *testing.T, ctx context.Context, d *blobChunkDiffer) (left, right []byte) {
	for i := 0; ; i++ {
		require.Less(t, i, 1_000_000, "differ did not terminate")
		l, r, err := d.Next(ctx)
		if err == io.EOF {
			return left, right
		}
		require.NoError(t, err)
		// At least one side must produce bytes on a non-EOF result.
		require.False(t, l == nil && r == nil, "Next returned no bytes without EOF")
		left = append(left, l...)
		right = append(right, r...)
	}
}

// compareViaDiffer mirrors the byte-comparison logic of val.compareChunkDiffer (the production
// consumer of this differ) so the test exercises the same semantics: accumulate the emitted
// bytes for each side and compare them lexicographically, treating a prefix as "less".
func compareViaDiffer(t *testing.T, ctx context.Context, d *blobChunkDiffer) int {
	var lBuf, rBuf []byte
	done := false
	for {
		for !done && (len(lBuf) == 0 || len(rBuf) == 0) {
			l, r, err := d.Next(ctx)
			if err == io.EOF {
				done = true
				break
			}
			require.NoError(t, err)
			lBuf = append(lBuf, l...)
			rBuf = append(rBuf, r...)
		}
		switch {
		case len(lBuf) == 0 && len(rBuf) == 0:
			return 0
		case len(lBuf) == 0:
			return -1
		case len(rBuf) == 0:
			return 1
		}
		n := len(lBuf)
		if len(rBuf) < n {
			n = len(rBuf)
		}
		if c := bytes.Compare(lBuf[:n], rBuf[:n]); c != 0 {
			return c
		}
		lBuf = lBuf[n:]
		rBuf = rBuf[n:]
	}
}

// assertDifferCorrect drives the differ over the trees for |left| and |right| and verifies two
// properties:
//
//   - The bytes the differ emits are exactly the suffixes of |left| and |right| that remain
//     after stripping a single, byte-identical common prefix from both. (Nothing differing is
//     ever skipped, and nothing identical-and-aligned is ever needlessly emitted in a way that
//     changes the result.)
//   - Feeding those emissions through the production consumer's comparison logic yields the same
//     ordering as a direct byte comparison of the full values.
func assertDifferCorrect(t *testing.T, ctx context.Context, ns NodeStore, chunkSize int, left, right []byte) {
	t.Helper()
	lRoot := buildBlobTree(t, ctx, ns, chunkSize, left)
	rRoot := buildBlobTree(t, ctx, ns, chunkSize, right)

	el, er := drainDiffer(t, ctx, differFromRoots(ns, lRoot, rRoot))

	// The emitted bytes are the suffixes after an identical common prefix; the prefix stripped
	// from each side must be the same length and the same bytes. (bytes.Equal treats a nil and
	// an empty slice as equal, which is what the consumer does too.)
	k := len(left) - len(el)
	require.GreaterOrEqual(t, k, 0, "emitted more bytes than the left value contains")
	require.Equal(t, len(right)-len(er), k, "different-length prefix skipped on each side")
	require.True(t, bytes.Equal(left[:k], right[:k]), "skipped prefix differs between the two sides")
	require.True(t, bytes.Equal(left[k:], el), "emitted left bytes are not the left suffix")
	require.True(t, bytes.Equal(right[k:], er), "emitted right bytes are not the right suffix")

	// And the comparison the consumer derives matches a direct comparison of the full values.
	want := sign(bytes.Compare(left, right))
	got := sign(compareViaDiffer(t, ctx, differFromRoots(ns, lRoot, rRoot)))
	require.Equal(t, want, got, "differ comparison disagrees with bytes.Compare")
}

func sign(i int) int {
	switch {
	case i < 0:
		return -1
	case i > 0:
		return 1
	default:
		return 0
	}
}

// makeData returns deterministic but non-trivial bytes of length n.
func makeData(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i*7 + 13) % 251)
	}
	return b
}

func withByteChanged(data []byte, off int) []byte {
	out := append([]byte(nil), data...)
	out[off] ^= 0xff
	return out
}

// TestBlobChunkDifferMultiLevel checks that the differ produces correct results across a range
// of edits to a tree that is several levels deep. The original implementation found the first
// differing address in the root and then walked straight down to the first leaf of that subtree,
// which produces wrong results once a tree has more than one level; these cases exercise that.
func TestBlobChunkDifferMultiLevel(t *testing.T) {
	ctx := context.Background()
	ns := NewTestNodeStore()

	const chunkSize = 80 // fanout of 4 (chunkSize / hash.ByteLen)
	const size = 4000    // ~50 leaves => a level-3 tree

	base := makeData(size)

	// Sanity check: the tree really is multi-level, otherwise this test proves nothing.
	root := buildBlobTree(t, ctx, ns, chunkSize, base)
	require.GreaterOrEqual(t, root.Level(), 2, "test data must produce a multi-level tree")

	cases := []struct {
		name  string
		left  []byte
		right []byte
	}{
		{"identical", base, base},
		{"change first byte", base, withByteChanged(base, 0)},
		{"change byte in first leaf", base, withByteChanged(base, 10)},
		{"change byte in second leaf", base, withByteChanged(base, chunkSize+5)},
		{"change byte mid tree", base, withByteChanged(base, size/2+3)},
		{"change byte in last leaf", base, withByteChanged(base, size-1)},
		{"change byte in deep non-first subtree", base, withByteChanged(base, chunkSize*30+17)},
		{"truncated", base, base[:size-chunkSize*3-7]},
		{"appended", base, append(append([]byte(nil), base...), makeData(chunkSize*5+9)...)},
		{"prepended", base, append(append([]byte(nil), makeData(chunkSize*5+9)...), base...)},
		{"completely different", base, makeData(size)},
		{"left empty", nil, base},
		{"right empty", base, nil},
		{"both empty", nil, nil},
		{"single leaf vs multi level", makeData(chunkSize - 1), base},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Run both orderings to cover the symmetric paths through the differ.
			assertDifferCorrect(t, ctx, ns, chunkSize, c.left, c.right)
			assertDifferCorrect(t, ctx, ns, chunkSize, c.right, c.left)
		})
	}
}

// TestBlobChunkDifferLocatesDeepDiff is the focused regression test for the multi-level bug. A
// single byte deep inside the tree is changed — in a leaf that is neither the first leaf overall
// nor the first leaf of its enclosing subtree. The first non-EOF result from Next must be the
// pair of leaves that actually differ, not an identical earlier leaf reached by blindly walking
// to the first leaf of the first differing root subtree.
func TestBlobChunkDifferLocatesDeepDiff(t *testing.T) {
	ctx := context.Background()
	ns := NewTestNodeStore()

	const chunkSize = 80
	const size = 4000
	const off = chunkSize*30 + 17 // deep, non-first leaf, non-first subtree
	leafIdx := off / chunkSize

	base := makeData(size)
	root := buildBlobTree(t, ctx, ns, chunkSize, base)
	require.GreaterOrEqual(t, root.Level(), 2)
	require.Greater(t, leafIdx, 0, "diff must not be in the first leaf")

	modified := withByteChanged(base, off)

	lRoot := buildBlobTree(t, ctx, ns, chunkSize, base)
	rRoot := buildBlobTree(t, ctx, ns, chunkSize, modified)
	d := differFromRoots(ns, lRoot, rRoot)

	l, r, err := d.Next(ctx)
	require.NoError(t, err)

	wantLeft := leafAt(base, chunkSize, leafIdx)
	wantRight := leafAt(modified, chunkSize, leafIdx)
	require.Equal(t, wantLeft, []byte(l), "first emitted left chunk is not the differing leaf")
	require.Equal(t, wantRight, []byte(r), "first emitted right chunk is not the differing leaf")
	require.NotEqual(t, []byte(l), []byte(r), "first emitted pair must actually differ")
}

// TestBlobChunkDifferDescendsWithoutStreaming verifies that locating a single deep difference in
// a multi-level tree only reads the nodes along the path to the changed leaf, rather than
// streaming every leaf. This is the behavior that makes diffing large blobs cheap, and it
// confirms the differ navigates the tree structure instead of flattening it.
func TestBlobChunkDifferDescendsWithoutStreaming(t *testing.T) {
	ctx := context.Background()
	inner := NewTestNodeStore()

	const chunkSize = 80
	const size = 8000 // ~100 leaves
	const off = chunkSize*55 + 3

	base := makeData(size)
	modified := withByteChanged(base, off)

	lRoot := buildBlobTree(t, ctx, inner, chunkSize, base)
	rRoot := buildBlobTree(t, ctx, inner, chunkSize, modified)
	require.GreaterOrEqual(t, lRoot.Level(), 2)

	counting := &countingNodeStore{NodeStore: inner}
	d := differFromRoots(counting, lRoot, rRoot)

	// Pull just the first divergence, the way the production consumer does once it has found a
	// differing byte.
	_, _, err := d.Next(ctx)
	require.NoError(t, err)

	totalLeaves := size / chunkSize
	// Locating the diff reads at most a couple of nodes per level on each side; it must be far
	// fewer than reading every leaf of the tree.
	require.Less(t, counting.reads, totalLeaves/2,
		"locating a single deep diff read %d nodes (tree has %d leaves)", counting.reads, totalLeaves)
}

// TestBlobChunkDifferVariousShapes exercises the differ across a matrix of sizes and chunk
// sizes, comparing every value against a single-byte-edited copy at several offsets, and against
// length changes. Each pair is checked against a direct byte comparison.
func TestBlobChunkDifferVariousShapes(t *testing.T) {
	ctx := context.Background()
	ns := NewTestNodeStore()

	chunkSizes := []int{40, 80, 200}
	sizes := []int{0, 1, 39, 80, 81, 500, 2000, 9001}

	for _, chunkSize := range chunkSizes {
		for _, size := range sizes {
			base := makeData(size)
			offsets := []int{0, size / 2, size - 1}
			t.Run(fmt.Sprintf("chunk=%d/size=%d", chunkSize, size), func(t *testing.T) {
				// Identical.
				assertDifferCorrect(t, ctx, ns, chunkSize, base, base)
				// Single-byte edits.
				for _, off := range offsets {
					if off < 0 || off >= size {
						continue
					}
					assertDifferCorrect(t, ctx, ns, chunkSize, base, withByteChanged(base, off))
				}
				// Length changes.
				if size > 0 {
					assertDifferCorrect(t, ctx, ns, chunkSize, base, base[:size-1])
				}
				assertDifferCorrect(t, ctx, ns, chunkSize, base, append(append([]byte(nil), base...), 0x01, 0x02, 0x03))
			})
		}
	}
}

// writeBlobLeaf writes a single leaf node holding |content| and returns it with its address.
func writeBlobLeaf(t *testing.T, ctx context.Context, ns NodeStore, content []byte) (*Node, hash.Hash) {
	s := message.NewBlobSerializer(ns.Pool())
	msg := s.Serialize([][]byte{{0}}, [][]byte{content}, []uint64{1}, 0)
	nd, _, err := NodeFromBytes(msg)
	require.NoError(t, err)
	addr, err := ns.Write(ctx, nd)
	require.NoError(t, err)
	return nd, addr
}

// writeBlobInternal writes an internal node pointing at |children| (with per-child leaf |counts|)
// at the given |level|, and returns it with its address.
func writeBlobInternal(t *testing.T, ctx context.Context, ns NodeStore, children []hash.Hash, counts []uint64, level int) (*Node, hash.Hash) {
	keys := make([][]byte, len(children))
	vals := make([][]byte, len(children))
	for i := range children {
		keys[i] = []byte{0}
		vals[i] = children[i][:]
	}
	s := message.NewBlobSerializer(ns.Pool())
	msg := s.Serialize(keys, vals, counts, level)
	nd, _, err := NodeFromBytes(msg)
	require.NoError(t, err)
	addr, err := ns.Write(ctx, nd)
	require.NoError(t, err)
	return nd, addr
}

// TestBlobChunkDifferNonUniformLeaves demonstrates a concrete wrong-answer bug that the old
// implementation produces on a multi-level tree, and that this implementation gets right.
//
// The trees are hand-built so that a leaf of differing length sits in front of a shared subtree:
//
//	left  = leaf("a")  + leaf("z")  => "az"
//	right = leaf("ab") + leaf("z")  => "abz"
//
// The second leaf ("z") is byte-identical on both sides and so shares an address. The old code
// would emit the first leaves ("a" vs "ab"), then — because the second child addresses match by
// index — skip the shared "z" subtree on both sides even though the streams are no longer
// byte-aligned (the left/right first leaves had different lengths). The consumer then compares
// "a" against "ab" alone and concludes left < right, when in fact "az" > "abz". Skipping after a
// length-changing divergence is exactly the unsafe shortcut this implementation avoids.
func TestBlobChunkDifferNonUniformLeaves(t *testing.T) {
	ctx := context.Background()
	ns := NewTestNodeStore()

	_, addrA := writeBlobLeaf(t, ctx, ns, []byte("a"))
	_, addrAB := writeBlobLeaf(t, ctx, ns, []byte("ab"))
	_, addrZ := writeBlobLeaf(t, ctx, ns, []byte("z"))

	lRoot, _ := writeBlobInternal(t, ctx, ns, []hash.Hash{addrA, addrZ}, []uint64{1, 1}, 1)
	rRoot, _ := writeBlobInternal(t, ctx, ns, []hash.Hash{addrAB, addrZ}, []uint64{1, 1}, 1)

	left, right := []byte("az"), []byte("abz")
	require.Equal(t, 1, sign(bytes.Compare(left, right))) // "az" > "abz"

	got := sign(compareViaDiffer(t, ctx, differFromRoots(ns, lRoot, rRoot)))
	require.Equal(t, 1, got, "differ must report az > abz")

	// And the reverse ordering, for good measure.
	gotRev := sign(compareViaDiffer(t, ctx, differFromRoots(ns, rRoot, lRoot)))
	require.Equal(t, -1, gotRev, "differ must report abz < az")
}

func leafAt(data []byte, chunkSize, idx int) []byte {
	start := idx * chunkSize
	end := start + chunkSize
	if end > len(data) {
		end = len(data)
	}
	return data[start:end]
}
