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
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/go-mysql-server/sql"
	"io"
	"math"
	"sync"
)

const DefaultFixedChunkLength = 4000

var ErrInvalidChunkSize = errors.New("invalid chunkSize; value must be a multiple of 20")
var ErrEmptyBlob = errors.New("invalid chunkSize; value must be a multiple of 20")

var chunkBufPool = sync.Pool{
	New: func() any {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
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
		ns:        ns,
		S:         message.NewBlobSerializer(ns.Pool()),
		chunkSize: chunkSize,
		buf:       make([]byte, chunkSize),
		keys:      keys,
		subtrees:  make([]uint64, chunkSize/hash.ByteLen),
	}, nil
}

type BlobBuilder struct {
	ns        NodeStore
	S         message.Serializer
	chunkSize int
	keys      [][]byte

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

func (b *BlobBuilder) Init(ctx context.Context, dataSize int, r io.Reader) {
	b.Reset()
	b.ctx = ctx
	b.dataSize = dataSize
	b.r = r
	b.height = b.blobHeight(b.dataSize, b.chunkSize)
	b.chunkCnt = b.chunkCount(b.dataSize, b.chunkSize)
	b.levelSize = int(math.Ceil(float64(b.dataSize) / float64(b.chunkSize)))
	b.levelStart = b.chunkCnt - b.levelSize
	b.levelEnd = b.chunkCnt
	b.prevLevelEnd = b.chunkCnt
	b.remainder = 1
	b.chunks = *chunkBufPool.Get().(*[][]byte)
	b.levelSubtreeCnt = 1
	for len(b.chunks) < b.chunkCnt {
		b.chunks = append(b.chunks, []byte(nil))
	}
}

func (b *BlobBuilder) Chunk() (Node, hash.Hash, error) {
	if b.dataSize == 0 {
		return Node{}, hash.Hash{}, ErrEmptyBlob
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

func (b *BlobBuilder) done() bool {
	return b.levelStart <= 0
}

func (b *BlobBuilder) incLevel() {
	b.level++
	b.prevLevelEnd = b.levelEnd
	b.levelEnd = b.levelStart
	b.levelSize = int(math.Ceil(float64(b.levelSize*hash.ByteLen) / float64(b.chunkSize)))
	b.levelStart = b.levelEnd - b.levelSize
	if b.level > 1 {
		// all full chunks will have the same subtree counts
		b.levelSubtreeCnt = b.levelSubtreeCnt * uint64(float64(b.chunkSize)/float64(hash.ByteLen))
		for i := range b.subtrees {
			b.subtrees[i] = b.levelSubtreeCnt
		}
	}
}

func (b *BlobBuilder) fillLevel() error {
	var start, end int
	for i := 0; i < b.levelSize; i++ {
		// |start| - |end| is a chunk-sized byte range from the previous level
		start = b.levelEnd + i*int(float64(b.chunkSize)/float64(hash.ByteLen))
		end = start + b.chunkSize/hash.ByteLen

		if i == b.levelSize-1 {
			if end > b.prevLevelEnd {
				for i := end - 1; i >= b.prevLevelEnd; i-- {
					b.subtrees[i-start] = 0
				}
				end = b.prevLevelEnd
			}
			b.subtrees[end-start-1] = b.remainder
			b.remainder += b.levelSubtreeCnt * uint64(end-start-1)
		}

		err := b.writeChunkAtPos(b.levelStart+i, b.keys, b.chunks[start:end], b.subtrees[:end-start], b.level)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *BlobBuilder) fillLeaves() error {
	start := b.chunkCnt - int(math.Ceil(float64(b.dataSize)/float64(b.chunkSize)))
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

	return b.writeChunkAtPos(i, [][]byte{{0}}, [][]byte{b.buf[:n]}, []uint64{1}, 0)
}

func (b *BlobBuilder) writeChunkAtPos(i int, keys, vals [][]byte, subtrees []uint64, level int) error {
	msg := b.S.Serialize(keys, vals, subtrees, level)

	node, err := NodeFromBytes(msg)
	b.lastN = node
	h, err := b.ns.Write(b.ctx, node)
	if err != nil {
		return err
	}
	b.chunks[i] = h[:]
	return nil
}

func (b *BlobBuilder) blobHeight(dataSize, chunkSize int) int {
	if dataSize == 0 {
		return 0
	}
	leaves := float64(dataSize) / float64(chunkSize)
	return int(math.Ceil(math.Log2(float64(leaves)) / math.Log2(float64(chunkSize/hash.ByteLen))))
}

func (b *BlobBuilder) chunkCount(dataSize, chunkSize int) int {
	if dataSize == 0 {
		return 0
	}
	if dataSize <= chunkSize {
		return 1
	}
	dataSize = int(math.Ceil(float64(dataSize) / float64(chunkSize)))
	l := 1
	sum := dataSize
	intChunk := int(math.Ceil(float64(chunkSize) / float64(hash.ByteLen)))
	for dataSize > intChunk {
		dataSize = int(math.Ceil(float64(dataSize) / float64(intChunk)))
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
