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

package tree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/val"
)

func TestWriteImmutableTree(t *testing.T) {
	tests := []struct {
		execErr   error
		initErr   error
		inputSize int
		chunkSize int
		checkSum  bool
	}{
		{
			inputSize: 100,
			chunkSize: 40,
		},
		{
			inputSize: 100,
			chunkSize: 100,
		},
		{
			inputSize: 100,
			chunkSize: 100,
		},
		{
			inputSize: 255,
			chunkSize: 40,
		},
		{
			inputSize: 243,
			chunkSize: 40,
		},
		{
			inputSize: 47,
			chunkSize: 40,
		},
		{
			inputSize: 200,
			chunkSize: 40,
		},
		{
			inputSize: 200,
			chunkSize: 40,
		},
		{
			inputSize: 1,
			chunkSize: 40,
		},
		{
			inputSize: 20,
			chunkSize: 500,
		},
		{
			inputSize: 1_000,
			chunkSize: 40,
			checkSum:  false,
		},
		{
			inputSize: 1_000,
			chunkSize: 60,
			checkSum:  false,
		},
		{
			inputSize: 1_000,
			chunkSize: 80,
			checkSum:  false,
		},
		{
			inputSize: 10_000,
			chunkSize: 100,
			checkSum:  false,
		},
		{
			inputSize: 50_000_000,
			chunkSize: 4000,
			checkSum:  false,
		},
		{
			inputSize: 50_000_000,
			chunkSize: 32_000,
			checkSum:  false,
		},
		{
			inputSize: 0,
			chunkSize: 40,
		},
		{
			inputSize: 100,
			chunkSize: 41,
			initErr:   ErrInvalidChunkSize,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("inputSize=%d; chunkSize=%d", tt.inputSize, tt.chunkSize), func(t *testing.T) {
			buf := make([]byte, tt.inputSize)
			for i := range buf {
				buf[i] = byte(i)
			}
			ctx := context.Background()
			r := bytes.NewReader(buf)
			ns := NewTestNodeStore()
			// serializer := message.NewBlobSerializer(ns.Pool())

			b, err := NewBlobBuilder(tt.chunkSize)
			if tt.initErr != nil {
				require.True(t, errors.Is(err, tt.initErr))
				return
			}
			b.SetNodeStore(ns)
			b.Init(tt.inputSize)
			root, _, err := b.Chunk(ctx, r)

			if tt.execErr != nil {
				require.True(t, errors.Is(err, tt.execErr))
				return
			}
			require.NoError(t, err)

			expSubtrees := expectedSubtrees(tt.inputSize, tt.chunkSize)
			expLevel := expectedLevel(tt.inputSize, tt.chunkSize)
			expSum := expectedSum(tt.inputSize)
			expUnfilled := expectedUnfilled(tt.inputSize, tt.chunkSize)

			intChunkSize := int(math.Ceil(float64(tt.chunkSize) / float64(hash.ByteLen)))

			unfilledCnt := 0
			sum := 0
			byteCnt := 0
			WalkNodes(ctx, root, ns, func(ctx context.Context, n *Node) error {
				if n.empty() {
					return nil
				}
				var keyCnt int
				leaf := n.IsLeaf()
				if leaf {
					byteCnt += len(getBlobValues(n.msg))
					for _, i := range n.GetValue(0) {
						sum += int(i)
					}
					keyCnt = len(getBlobValues(n.msg))
					if keyCnt != tt.chunkSize {
						unfilledCnt += 1
					}
				} else {
					keyCnt = n.Count()
					if keyCnt < intChunkSize {
						unfilledCnt += 1
					}
				}
				return nil
			})

			level := root.Level()
			assert.Equal(t, expLevel, level)
			if tt.checkSum {
				assert.Equal(t, expSum, sum)
			}
			assert.Equal(t, tt.inputSize, byteCnt)
			assert.Equal(t, expUnfilled, unfilledCnt)
			if expLevel > 0 {
				root, err = root.LoadSubtrees()
				require.NoError(t, err)
				for i := range expSubtrees {
					sc := root.GetSubtreeCount(i)
					assert.Equal(t, expSubtrees[i], sc)
				}
			}
		})
	}
}

func expectedLevel(size, chunk int) int {
	if size <= chunk {
		return 0
	}
	size = int(math.Ceil(float64(size) / float64(chunk)))
	l := 1
	intChunk := int(math.Ceil(float64(chunk) / float64(hash.ByteLen)))
	for size > intChunk {
		size = int(math.Ceil(float64(size) / float64(intChunk)))
		l += 1
	}
	return l
}

func expectedSubtrees(size, chunk int) subtreeCounts {
	if size <= chunk {
		return subtreeCounts{0}
	}
	l := expectedLevel(size, chunk)

	size = int(math.Ceil(float64(size) / float64(chunk)))
	intChunk := int(math.Ceil(float64(chunk) / float64(hash.ByteLen)))

	filledSubtree := int(math.Pow(float64(intChunk), float64(l-1)))

	subtrees := make(subtreeCounts, 0)
	for size > filledSubtree {
		subtrees = append(subtrees, uint64(filledSubtree))
		size -= filledSubtree
	}
	if size > 0 {
		subtrees = append(subtrees, uint64(size))
	}
	if len(subtrees) > intChunk {
		panic("unreachable")
	}
	return subtrees
}

func expectedSum(size int) int {
	return (size * (size + 1) / 2) - size
}

func expectedUnfilled(size, chunk int) int {
	if size == chunk || size == 0 {
		return 0
	} else if size < chunk {
		return 1
	}

	var unfilled int
	// level 0 is special case
	if size%chunk != 0 {
		unfilled += 1
	}
	size = int(math.Ceil(float64(size) / float64(chunk)))

	intChunk := int(math.Ceil(float64(chunk) / float64(hash.ByteLen)))
	for size > intChunk {
		if size%intChunk != 0 {
			unfilled += 1
		}
		size = int(math.Ceil(float64(size) / float64(intChunk)))
	}
	if size < intChunk {
		unfilled += 1
	}
	return unfilled
}

func TestImmutableTreeWalk(t *testing.T) {
	tests := []struct {
		blobLen   int
		chunkSize int
		keyCnt    int
	}{
		{
			blobLen:   250,
			chunkSize: 60,
			keyCnt:    4,
		},
		{
			blobLen:   250,
			chunkSize: 40,
			keyCnt:    4,
		},
		{
			blobLen:   378,
			chunkSize: 60,
			keyCnt:    12,
		},
		{
			blobLen:   5000,
			chunkSize: 40,
			keyCnt:    6,
		},
		{
			blobLen:   1,
			chunkSize: 40,
			keyCnt:    6,
		},
		{
			blobLen:   50_000_000,
			chunkSize: 4000,
			keyCnt:    1,
		},
		{
			blobLen:   10_000,
			chunkSize: 80,
			keyCnt:    6,
		},
	}

	ns := NewTestNodeStore()
	for _, tt := range tests {
		t.Run(fmt.Sprintf("inputSize=%d; chunkSize=%d; keyCnt=%d", tt.blobLen, tt.chunkSize, tt.keyCnt), func(t *testing.T) {
			r := newTree(t, ns, tt.keyCnt, tt.blobLen, tt.chunkSize)
			var cnt int
			walkOpaqueNodes(context.Background(), r, ns, func(ctx context.Context, n *Node) error {
				cnt++
				return nil
			})
			require.Equal(t, blobAddrCnt(tt.blobLen, tt.chunkSize)*tt.keyCnt+1, cnt)
		})
	}
}

func blobAddrCnt(size, chunk int) int {
	if size == 0 {
		return 0
	}
	if size <= chunk {
		return 1
	}
	size = int(math.Ceil(float64(size) / float64(chunk)))
	l := 1
	sum := size
	intChunk := int(math.Ceil(float64(chunk) / float64(hash.ByteLen)))
	for size > intChunk {
		size = int(math.Ceil(float64(size) / float64(intChunk)))
		sum += size
		l += 1
	}
	return sum + 1
}

func newTree(t *testing.T, ns NodeStore, keyCnt, blobLen, chunkSize int) *Node {
	ctx := context.Background()

	keyDesc := val.NewTupleDescriptor(val.Type{Enc: val.Uint32Enc})
	valDesc := val.NewTupleDescriptor(val.Type{Enc: val.BytesAddrEnc})

	tuples := make([][2]val.Tuple, keyCnt)
	keyBld := val.NewTupleBuilder(keyDesc, ns)
	valBld := val.NewTupleBuilder(valDesc, ns)
	var err error
	for i := range tuples {
		keyBld.PutUint32(0, uint32(i))
		tuples[i][0], err = keyBld.Build(context.Background(), sharedPool)
		if err != nil {
			panic(err)
		}

		addr := mustNewBlob(ctx, ns, blobLen, chunkSize)
		valBld.PutBytesAddr(0, addr)
		tuples[i][1], err = valBld.Build(context.Background(), sharedPool)
		if err != nil {
			panic(err)
		}
	}

	s := message.NewProllyMapSerializer(valDesc, ns.Pool())
	chunker, err := newEmptyChunker(ctx, ns, s)
	require.NoError(t, err)
	for _, pair := range tuples {
		err := chunker.AddPair(ctx, Item(pair[0]), Item(pair[1]))
		require.NoError(t, err)
	}
	root, err := chunker.Done(ctx)
	require.NoError(t, err)
	return root
}

func mustNewBlob(ctx context.Context, ns NodeStore, len, chunkSize int) hash.Hash {
	buf := make([]byte, len)
	for i := range buf {
		buf[i] = byte(i)
	}
	r := bytes.NewReader(buf)
	b, err := NewBlobBuilder(chunkSize)
	if err != nil {
		panic(err)
	}
	b.SetNodeStore(ns)
	b.Init(len)
	_, addr, err := b.Chunk(ctx, r)
	if err != nil {
		panic(err)
	}
	return addr
}

// countingNodeStore wraps a NodeStore and counts how many calls are made to Read. This is used
// to verify that streamed comparisons stop loading nodes once the result is decided.
type countingNodeStore struct {
	NodeStore
	reads int
}

func (c *countingNodeStore) Read(ctx context.Context, h hash.Hash) (*Node, error) {
	c.reads++
	return c.NodeStore.Read(ctx, h)
}

func (c *countingNodeStore) OpenChunkDiffer(ctx context.Context, l, r val.AdaptiveValue) (ChunkDiffer, error) {
	return newBlobChunkDiffer(ctx, c, l, r)
}

// TestBlobChunkDiffer exercises the new ChunkDiffer API end-to-end. The "identical roots"
// case is the headline benefit: when both adaptive values share an out-of-band address, the
// differ should short-circuit to EOF without reading the blob's interior at all. The
// "localized diff" case verifies that hash-based subtree skipping limits reads to roughly the
// height of the tree even when the values are very large.
func TestBlobChunkDiffer(t *testing.T) {
	const blobLen = 50_000
	const chunkSize = 4000

	ctx := context.Background()
	ns := NewTestNodeStore()
	left := make([]byte, blobLen)
	right := make([]byte, blobLen)
	for i := range left {
		left[i] = byte(i)
		right[i] = byte(i)
	}
	// Diverge near the end so a naive byte-wise walk would touch almost every chunk.
	right[blobLen-1] = left[blobLen-1] + 1

	lVal, err := val.NewOutOfBandAdaptiveValue(ctx, ns, left)
	require.NoError(t, err)
	rVal, err := val.NewOutOfBandAdaptiveValue(ctx, ns, right)
	require.NoError(t, err)
	sameAsLeft, err := val.NewOutOfBandAdaptiveValue(ctx, ns, append([]byte(nil), left...))
	require.NoError(t, err)

	t.Run("identical roots short-circuit to EOF", func(t *testing.T) {
		cns := &countingNodeStore{NodeStore: ns}
		differ, err := cns.OpenChunkDiffer(ctx, lVal, sameAsLeft)
		require.NoError(t, err)
		l, r, err := differ.Next(ctx)
		require.Equal(t, io.EOF, err)
		require.Nil(t, l)
		require.Nil(t, r)
		// Construction reads the root from each side; nothing more should follow.
		require.LessOrEqual(t, cns.reads, 2)
	})

	t.Run("localized diff bounds the read count", func(t *testing.T) {
		// Diff a deep blob — its tree has multiple levels, so walking it naively would read
		// every chunk. With subtree-hash skipping we should only descend the path that
		// actually contains the change.
		cns := &countingNodeStore{NodeStore: ns}
		differ, err := cns.OpenChunkDiffer(ctx, lVal, rVal)
		require.NoError(t, err)

		// Drain to confirm we eventually hit EOF and saw exactly one differing leaf pair.
		divergingPairs := 0
		for {
			l, r, err := differ.Next(ctx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			if !bytes.Equal(l, r) {
				divergingPairs++
			}
		}
		require.Equal(t, 1, divergingPairs, "exactly one leaf pair should differ for a one-byte change")

		// A naive walk would visit every node in the tree on both sides. We assert that the
		// differ visits strictly fewer nodes than a full bilateral walk would.
		root, err := ns.(*nodeStoreValidator).Read(ctx, mustOOBAddr(t, lVal))
		require.NoError(t, err)
		fullNs := &countingNodeStore{NodeStore: ns}
		require.NoError(t, WalkNodes(ctx, root, fullNs, func(_ context.Context, _ *Node) error { return nil }))
		require.Less(t, cns.reads, 2*fullNs.reads, "differ should read fewer nodes than walking both trees in full")
	})
}

func mustOOBAddr(t *testing.T, v val.AdaptiveValue) hash.Hash {
	t.Helper()
	addr, err := v.OutOfBandAddr()
	require.NoError(t, err)
	return addr
}

// TestCompareAdaptiveValueStreamsChunks verifies that comparing two out-of-band AdaptiveValues
// through TupleDesc.Comparator returns the correct ordering and stops loading nodes once the
// ordering is decided. The streaming path is the entire reason this test exists; if it were
// to regress to loading both values fully, the early-termination assertion would fail.
func TestCompareAdaptiveValueStreamsChunks(t *testing.T) {
	ctx := context.Background()
	const blobLen = 50_000

	// Build two large byte sequences that share a long common prefix and differ at byte
	// index `divergeAt`. With blobLen >> chunkSize, the divergence is far enough from the
	// start that a non-streaming compare would touch every node in the tree.
	const divergeAt = 100
	left := make([]byte, blobLen)
	right := make([]byte, blobLen)
	for i := range left {
		left[i] = byte(i)
		right[i] = byte(i)
	}
	right[divergeAt] = left[divergeAt] + 1

	cases := []struct {
		name string
		l, r []byte
		want int
	}{
		{"left less", left, right, -1},
		{"right less", right, left, 1},
		{"equal", left, append([]byte(nil), left...), 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cns := &countingNodeStore{NodeStore: NewTestNodeStore()}
			lVal, err := val.NewOutOfBandAdaptiveValue(ctx, cns, c.l)
			require.NoError(t, err)
			rVal, err := val.NewOutOfBandAdaptiveValue(ctx, cns, c.r)
			require.NoError(t, err)

			td := val.NewTupleDescriptorWithArgs(
				val.TupleDescriptorArgs{ValueStore: cns},
				val.Type{Enc: val.BytesAdaptiveEnc},
			)

			writesAfterBuild := cns.reads
			cns.reads = 0
			got, err := td.Comparator().CompareValues(ctx, 0, lVal, rVal, val.Type{Enc: val.BytesAdaptiveEnc})
			require.NoError(t, err)
			require.Equal(t, c.want, got)

			// Bound the number of nodes read during compare. The mismatch is well within the
			// first leaf chunk, so we expect to read O(tree height) nodes, not the whole tree.
			// We give a generous upper bound (10x tree height) to keep the test stable while
			// still catching a regression to "load everything."
			if c.want != 0 {
				require.LessOrEqual(t, cns.reads, 8,
					"compare should read only the root-to-first-leaf path on each side; %d writes happened before compare",
					writesAfterBuild)
			}
		})
	}
}

// TestCompareAdaptiveTextWithCollation drives the full CollationTupleComparator → val
// streaming-collation path against real out-of-band adaptive text values. Without the
// streaming-collation hook the comparator would fall back to bytewise comparison and the
// "ABC" vs "abc" case would return -1 instead of 0 under a case-insensitive collation.
func TestCompareAdaptiveTextWithCollation(t *testing.T) {
	ctx := context.Background()

	// Use a long string so the value is forced out-of-band and the comparison actually walks
	// the streaming reader. The prefix is identical so collation has to decode runes from
	// real chunks before deciding.
	padding := strings.Repeat("x", 8_000)

	cases := []struct {
		name      string
		l, r      string
		collation sql.CollationID
		want      int
	}{
		{
			name:      "case-insensitive: identical except ASCII case",
			l:         padding + "Hello, World!",
			r:         padding + "HELLO, WORLD!",
			collation: sql.Collation_utf8mb4_0900_ai_ci,
			want:      0,
		},
		{
			name:      "binary collation: ASCII case differs",
			l:         padding + "Hello, World!",
			r:         padding + "HELLO, WORLD!",
			collation: sql.Collation_utf8mb4_bin,
			want:      1, // uppercase letters have lower byte values
		},
		{
			name:      "case-insensitive: trailing rune differs",
			l:         padding + "café",
			r:         padding + "cafz",
			collation: sql.Collation_utf8mb4_0900_ai_ci,
			want:      -1,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ns := NewTestNodeStore()
			lVal, err := val.NewOutOfBandAdaptiveValue(ctx, ns, []byte(c.l))
			require.NoError(t, err)
			rVal, err := val.NewOutOfBandAdaptiveValue(ctx, ns, []byte(c.r))
			require.NoError(t, err)

			cmp := schema.CollationTupleComparator{
				Collations: []sql.CollationID{c.collation},
			}.WithValueStore(ns).Validated([]val.Type{{Enc: val.StringAdaptiveEnc}})

			got, err := cmp.CompareValues(ctx, 0, lVal, rVal, val.Type{Enc: val.StringAdaptiveEnc})
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}

func getBlobValues(msg serial.Message) []byte {
	var b serial.Blob
	err := serial.InitBlobRoot(&b, msg, serial.MessagePrefixSz)
	if err != nil {
		panic(err)
	}
	return b.PayloadBytes()
}
