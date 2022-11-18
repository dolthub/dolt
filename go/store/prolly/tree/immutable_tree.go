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
	"encoding/json"
	"errors"
	"io"
	"math"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
)

const DefaultFixedChunkLength = 4000

var ErrInvalidChunkSize = errors.New("invalid chunkSize; value must be a multiple of 20")

var chunkBufPool = sync.Pool{
	New: func() any {
		return new([][]byte)
	},
}

func mustNewBlobBuilder(ns NodeStore, chunkSize int) *BlobBuilder {
	b, _ := NewBlobBuilder(ns, chunkSize)
	return b
}

// NewBlobBuilder writes the contents of |reader| as an append-only
// tree, returning the root node or an error if applicable. |chunkSize|
// fixes the split size of leaf and intermediate node chunks.
func NewBlobBuilder(ns NodeStore, chunkSize int) (*BlobBuilder, error) {
	if chunkSize%hash.ByteLen != 0 {
		return nil, ErrInvalidChunkSize
	}

	keys := make([][]byte, chunkSize/hash.ByteLen)
	for i := range keys {
		keys[i] = []byte{0}
	}
	return &BlobBuilder{
		ns:         ns,
		S:          message.NewBlobSerializer(ns.Pool()),
		lChunkSize: chunkSize,
		iChunkSize: chunkSize / hash.ByteLen,
		buf:        make([]byte, chunkSize),
		keys:       keys,
		na:         &nodeArena{},
		subtrees:   make([]uint64, chunkSize/hash.ByteLen),
	}, nil
}

type BlobBuilder struct {
	ns         NodeStore
	S          message.Serializer
	lChunkSize int
	iChunkSize int
	keys       [][]byte
	na         *nodeArena

	ctx      context.Context
	r        io.Reader
	dataSize int
	height   int
	chunkCnt int

	lastN    Node
	buf      []byte
	subtrees []uint64
	chunks   [][]byte

	level           int
	levelSize       int
	levelStart      int
	levelEnd        int
	levelSubtreeCnt uint64
	prevLevelEnd    int
	remainder       uint64
}

// Reset clears the BlobBuilder for re-use.
func (b *BlobBuilder) Reset() {
	b.r = nil
	b.dataSize = 0
	b.height = 0
	b.chunkCnt = 0
	b.ctx = nil
	for i := range b.subtrees {
		b.subtrees[i] = 1
	}
}

// Init calculates tree dimensions for a given blob.
func (b *BlobBuilder) Init(ctx context.Context, dataSize int, r io.Reader) {
	b.Reset()
	b.ctx = ctx
	b.dataSize = dataSize
	b.r = r

	b.height = b.blobHeight(b.dataSize, b.lChunkSize, b.iChunkSize)
	b.chunkCnt = b.chunkCount(b.dataSize, b.lChunkSize, b.iChunkSize)
	b.levelSize = int(math.Ceil(float64(b.dataSize) / float64(b.lChunkSize)))
	b.levelStart = b.chunkCnt - b.levelSize
	b.levelEnd = b.chunkCnt
	b.prevLevelEnd = b.chunkCnt
	b.remainder = 1
	b.levelSubtreeCnt = 1

	b.chunks = *chunkBufPool.Get().(*[][]byte)
	for len(b.chunks) < b.chunkCnt {
		b.chunks = append(b.chunks, []byte(nil))
	}
}

// Chunk builds the blob tree in reverse level-order. Level 0, consisting
// of leaf nodes containing the blob bytes, are built first but inserted
// at the end of the tree. The root node will be built last, and occupy
// |chunks[0]|.
//
// Tree building is divided into two phases so that we can use different data
// structures for 1) reading the blob bytes, and 2) building intermediate nodes
// of hash references.
//
// Every hash has a pre-calculated position in the tree. Chunk key tuple are
// nil for both leaf and intermediate chunks. Intermediate chunk value tuples
// include a range of lower level hashes of length `chunkSize/hashSize`.
//
// Subtree counts are calculated iteratively; the final node for any level
// will reference (from the lower level) zero or many full node references plus
// 2) one partial or full |remainder| node. The |remainder| accumulates iteratively
// for the final node of each level moving up the tree.
func (b *BlobBuilder) Chunk() (Node, hash.Hash, error) {
	if b.dataSize == 0 {
		return Node{}, hash.Hash{}, nil
	} else if b.chunkCnt == 1 {
		err := b.writeNextLeaf(0)
		if err != nil {
			return Node{}, hash.Hash{}, err
		}
		return b.lastN, hash.New(b.chunks[0]), nil
	}

	err := b.fillLeaves()
	if err != nil {
		return Node{}, hash.Hash{}, err
	}

	for !b.done() {
		b.incLevel()
		err = b.fillLevel()
		if err != nil {
			return Node{}, hash.Hash{}, err
		}
	}
	return b.lastN, hash.New(b.chunks[0]), nil
}

// done indicates the tree is complete.
func (b *BlobBuilder) done() bool {
	return b.levelStart <= 0
}

// incLevel updates the current/previous level tracking info.
func (b *BlobBuilder) incLevel() {
	b.level++
	b.prevLevelEnd = b.levelEnd
	b.levelEnd = b.levelStart
	b.levelSize = int(math.Ceil(float64(b.levelSize) / float64(b.iChunkSize)))
	b.levelStart = b.levelEnd - b.levelSize
	if b.level > 1 {
		// all full chunks will have the same subtree counts
		b.levelSubtreeCnt = b.levelSubtreeCnt * uint64(b.iChunkSize)
		for i := range b.subtrees {
			b.subtrees[i] = b.levelSubtreeCnt
		}
	}
}

// fillLevel writes chunks for an intermediate level, inserting the result
// addresses into the |chunks| tree. Intermediate level chunk values are
// |chunkSize/hashSize| slices of addresses from the child level of the tree.
// The final chunk for every level may be unfilled.
func (b *BlobBuilder) fillLevel() error {
	var start, end int
	for i := 0; i < b.levelSize; i++ {
		// |start| - |end| is a chunk-sized byte range from the previous level
		start = b.levelEnd + i*b.iChunkSize
		end = start + b.lChunkSize/hash.ByteLen

		if i == b.levelSize-1 {
			// the final node in a level is special
			if end > b.prevLevelEnd {
				// final node might not fill a whole chunk
				for i := end - 1; i >= b.prevLevelEnd; i-- {
					b.subtrees[i-start] = 0
				}
				end = b.prevLevelEnd
			}
			// and the final node in the lower level may not be full
			b.subtrees[end-start-1] = b.remainder
			b.remainder += b.levelSubtreeCnt * uint64(end-start-1)
		}

		err := b.writeChunkAtIdx(
			b.levelStart+i,
			b.keys,
			b.chunks[start:end],
			b.subtrees[:end-start],
			b.level,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// fillLeaves writes level 0 of the tree containing a slice of the blob
// byte array of length |chunkSize|.
func (b *BlobBuilder) fillLeaves() error {
	start := b.chunkCnt - int(math.Ceil(float64(b.dataSize)/float64(b.lChunkSize)))
	end := b.chunkCnt
	for i := start; i < end; i++ {
		err := b.writeNextLeaf(i)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *BlobBuilder) writeNextLeaf(i int) error {
	n, err := b.r.Read(b.buf)
	if err != nil {
		return err
	}

	return b.writeChunkAtIdx(i, [][]byte{{0}}, [][]byte{b.buf[:n]}, []uint64{1}, 0)
}

func (b *BlobBuilder) writeChunkAtIdx(
	i int, keys, vals [][]byte,
	subtrees []uint64,
	level int,
) error {
	msg := b.S.Serialize(keys, vals, subtrees, level)

	node, err := b.na.NodeFromBytes(msg)
	b.lastN = node
	h, err := b.ns.Write(b.ctx, node)
	if err != nil {
		return err
	}
	b.chunks[i] = h[:]
	return nil
}

// blobHeight calculates the number of tree levelse for a blob. We
// differentiate between the first level, which chunks |chunkSize| bytes
// from the blob, and intermediate levels that chunk |chunkSize/hashSize|
// addresses.
func (b *BlobBuilder) blobHeight(dataSize, lChunkSize, iChunkSize int) int {
	if dataSize == 0 {
		return 0
	}
	leaves := float64(dataSize) / float64(lChunkSize)
	return int(math.Ceil(math.Log2(leaves) / math.Log2(float64(iChunkSize))))
}

func (b *BlobBuilder) chunkCount(dataSize, lChunkSize, iChunkSize int) int {
	if dataSize == 0 {
		return 0
	}
	if dataSize <= lChunkSize {
		return 1
	}
	dataSize = int(math.Ceil(float64(dataSize) / float64(lChunkSize)))
	l := 1
	sum := dataSize
	for dataSize > iChunkSize {
		dataSize = int(math.Ceil(float64(dataSize) / float64(iChunkSize)))
		sum += dataSize
		l += 1
	}
	return sum + 1
}

const bytePeekLength = 128

type ByteArray struct {
	ImmutableTree
}

func NewByteArray(addr hash.Hash, ns NodeStore) *ByteArray {
	return &ByteArray{ImmutableTree{Addr: addr, ns: ns}}
}

func (b *ByteArray) ToBytes(ctx context.Context) ([]byte, error) {
	return b.bytes(ctx)
}

func (b *ByteArray) ToString(ctx context.Context) (string, error) {
	buf, err := b.bytes(ctx)
	if err != nil {
		return "", err
	}
	toShow := bytePeekLength
	if len(buf) < toShow {
		toShow = len(buf)
	}
	return string(buf[:toShow]), nil
}

type JSONDoc struct {
	ImmutableTree
}

func NewJSONDoc(addr hash.Hash, ns NodeStore) *JSONDoc {
	return &JSONDoc{ImmutableTree{Addr: addr, ns: ns}}
}

func (b *JSONDoc) ToJSONDocument(ctx context.Context) (sql.JSONDocument, error) {
	buf, err := b.bytes(ctx)
	if err != nil {
		return sql.JSONDocument{}, err
	}
	var doc sql.JSONDocument
	err = json.Unmarshal(buf, &doc.Val)
	if err != nil {
		return sql.JSONDocument{}, err
	}
	return doc, err
}

func (b *JSONDoc) ToString(ctx context.Context) (string, error) {
	buf, err := b.bytes(ctx)
	if err != nil {
		return "", err
	}
	toShow := bytePeekLength
	if len(buf) < toShow {
		toShow = len(buf)
	}
	return string(buf[:toShow]), nil
}

type TextStorage struct {
	ImmutableTree
}

func NewTextStorage(addr hash.Hash, ns NodeStore) *TextStorage {
	return &TextStorage{ImmutableTree{Addr: addr, ns: ns}}
}

func (b *TextStorage) ToBytes(ctx context.Context) ([]byte, error) {
	return b.bytes(ctx)
}

func (b *TextStorage) ToString(ctx context.Context) (string, error) {
	buf, err := b.bytes(ctx)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

type ImmutableTree struct {
	Addr hash.Hash
	buf  []byte
	ns   NodeStore
}

func (t *ImmutableTree) load(ctx context.Context) error {
	if t.Addr.IsEmpty() {
		t.buf = []byte{}
		return nil
	}
	n, err := t.ns.Read(ctx, t.Addr)
	if err != nil {
		return err
	}

	return WalkNodes(ctx, n, t.ns, func(ctx context.Context, n Node) error {
		if n.IsLeaf() {
			t.buf = append(t.buf, n.GetValue(0)...)
		}
		return nil
	})
}

func (t *ImmutableTree) bytes(ctx context.Context) ([]byte, error) {
	if t.buf == nil {
		err := t.load(ctx)
		if err != nil {
			return nil, err
		}
	}
	return t.buf[:], nil
}

func (t *ImmutableTree) next() (Node, error) {
	panic("not implemented")
}

func (t *ImmutableTree) close() error {
	panic("not implemented")
}

func (t *ImmutableTree) Read(buf bytes.Buffer) (int, error) {
	panic("not implemented")
}
