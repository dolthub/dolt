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

// buildImmutableTree writes the contents of |reader| as an append-only
// tree, returning the root node or an error if applicable. |chunkSize|
// fixes the split size of leaf and intermediate node chunks.
func buildImmutableTree(ctx context.Context, r io.Reader, ns NodeStore, S message.Serializer, dataSize, chunkSize int) (Node, error) {
	if chunkSize < hash.ByteLen*2 || chunkSize > int(message.MaxVectorOffset)/2 {
		// internal nodes must fit at least two 20-byte hashes
		return Node{}, ErrInvalidChunkSize
	}

	height := blobHeight(dataSize, chunkSize)
	chunkCnt := chunkCount(chunkSize, height)
	if chunkCnt == 1 {
		buf := make([]byte, dataSize)
		_, err := r.Read(buf)
		if err != nil {
			return Node{}, err
		}
		msg := S.Serialize([][]byte{{0}}, [][]byte{buf}, []uint64{1}, 0)
		node, err := NodeFromBytes(msg)
		_, err = ns.Write(ctx, node)
		if err != nil {
			return Node{}, err
		}
		return node, nil
	}

	var levels [][]novelNode
	var levelCnts []int
	var finalize bool

	// We use lookahead to check whether the reader has
	// more bytes. The reader will only EOF when reading
	// zero bytes into the lookahead buffer, but we want
	// to know at the beginning of a loop whether we are
	// finished.
	lookahead := make([]byte, chunkSize)
	lookaheadN, err := r.Read(lookahead)
	if err != nil {
		return Node{}, err
	}

	buf := make([]byte, chunkSize)
	for {
		copy(buf, lookahead)
		curN := lookaheadN
		lookaheadN, err = r.Read(lookahead)
		if err == io.EOF {
			finalize = true
		} else if err != nil {
			return Node{}, err
		}

		novel, err := _newLeaf(ctx, ns, S, buf[:curN])
		if err != nil {
			return Node{}, err
		}

		i := 0
		for {
			// Three cases for building tree
			// 1) reached new level => create new level
			// 2) add novel node to current level
			// 3) we didn't fill the current level => break
			// 4) we filled current level, chunk and recurse into parent
			//
			// Two cases for finalizing tree
			// 1) we haven't hit root, so we add the final chunk, finalize level, and continue upwards
			// 2) we overshot root finalizing chunks, and we return the single root in the lower level
			if i > len(levels)-1 {
				levels = append(levels, make([]novelNode, chunkSize))
				levelCnts = append(levelCnts, 0)
			}

			levels[i][levelCnts[i]] = novel
			levelCnts[i]++
			// note: the size of an internal node will be the key count times key length (hash)
			if levelCnts[i]*hash.ByteLen < chunkSize {
				// current level is not full
				if !finalize {
					// only continue and chunk this level if finalizing all in-progress nodes
					break
				}
			}

			nodes := levels[i][:levelCnts[i]]
			if len(nodes) == 1 && i == len(levels)-1 {
				// this is necessary and only possible if we're finalizing
				// note: this is the only non-error return
				return nodes[0].node, nil
			}

			// chunk the current level
			novel, err = _newInternal(ctx, ns, S, nodes, i+1, chunkSize)
			if err != nil {
				return Node{}, err
			}
			levelCnts[i] = 0
			i++
		}
	}
}

/*
func buildImmutableTree2(ctx context.Context, r io.Reader, dataSize int, ns NodeStore, S message.Serializer, chunkSize int) (Node, error) {
	if chunkSize < hash.ByteLen*2 || chunkSize > int(message.MaxVectorOffset)/2 {
		// internal nodes must fit at least two 20-byte hashes
		return Node{}, ErrInvalidChunkSize
	}

	height := blobHeight(dataSize, chunkSize)
	chunkCnt := chunkCount(chunkSize, height)
	if chunkCnt == 1 {
		buf := make([]byte, dataSize)
		_, err := r.Read(buf)
		if err != nil {
			return Node{}, err
		}
		msg := S.Serialize([][]byte{{0}}, [][]byte{buf}, []uint64{1}, 0)
		return NodeFromBytes(msg)
	}

	chunks := make([]hash.Hash, chunkCnt)

	buf := make([]byte, chunkSize)
	i := chunkCnt - 1
	levelSize := int(math.Ceil(float64(dataSize / chunkSize)))
	nextLevel := chunkCnt - levelSize
	for i >= nextLevel {
		_, err := r.Read(buf)
		if err != nil {
			return Node{}, err
		}
		msg := S.Serialize([][]byte{{0}}, [][]byte{buf}, []uint64{1}, 0)
		node, err := NodeFromBytes(msg)
		if err != nil {
			return Node{}, err
		}
		addr, err := ns.Write(ctx, node)
		if err != nil {
			return Node{}, err
		}
		chunks[i] = addr
		i--
	}

	// filled level 0
	level := 1
	subtrees := make([]uint64, chunkSize)
	fill := 0
	for i > 0 {
		if fill >= chunkSize {

		}
	}
	//todo: get children from index

	//todo: get height from index

	//todo: get subtrees from index

}
*/

var chunkBufPool = sync.Pool{
	New: func() any {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		return new([][]byte)
	},
}

func newBlobBuilder(ns NodeStore, chunkSize int) (*blobBuilder, error) {
	if chunkSize%hash.ByteLen != 0 {
		return nil, ErrInvalidChunkSize
	}

	keys := make([][]byte, chunkSize/hash.ByteLen)
	for i := range keys {
		keys[i] = []byte{0}
	}
	return &blobBuilder{
		ns:        ns,
		S:         message.NewBlobSerializer(ns.Pool()),
		chunkSize: chunkSize,
		buf:       make([]byte, chunkSize),
		keys:      keys,
		subtrees:  make([]uint64, chunkSize/hash.ByteLen),
	}, nil
}

type blobBuilder struct {
	ns        NodeStore
	S         message.Serializer
	chunkSize int
	keys      [][]byte

	ctx      context.Context
	r        io.Reader
	dataSize int
	height   int
	chunkCnt int

	// bytes.Buffer or sync.Pool?
	lastN           Node
	buf             []byte
	subtrees        []uint64
	chunks          [][]byte
	level           int
	levelSize       int
	levelStart      int
	levelEnd        int
	levelSubtreeCnt uint64
	prevLevelEnd    int
	remainder       uint64
}

func (b *blobBuilder) Reset() {
	b.r = nil
	b.dataSize = 0
	b.height = 0
	b.chunkCnt = 0
	b.ctx = nil
	for i := range b.subtrees {
		b.subtrees[i] = 1
	}
}

func (b *blobBuilder) init(ctx context.Context, dataSize int, r io.Reader) {
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

func (b *blobBuilder) chunk() (Node, hash.Hash, error) {
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

func (b *blobBuilder) done() bool {
	return b.levelStart <= 0
}

func (b *blobBuilder) incLevel() {
	b.level++
	b.prevLevelEnd = b.levelEnd
	b.levelEnd = b.levelStart
	b.levelSize = int(math.Ceil(float64(b.levelSize*hash.ByteLen) / float64(b.chunkSize)))
	b.levelStart = b.levelEnd - b.levelSize
	if b.level > 1 {
		b.levelSubtreeCnt = b.levelSubtreeCnt * uint64(float64(b.chunkSize)/float64(hash.ByteLen))
		// all full chunks will have the same subtree counts
		for i := range b.subtrees {
			b.subtrees[i] = b.levelSubtreeCnt
		}
	}
}

func (b *blobBuilder) fillLevel() error {
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

func (b *blobBuilder) fillLeaves() error {
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

func (b *blobBuilder) writeNextLeaf(i int) error {
	n, err := b.r.Read(b.buf)
	if err != nil {
		return err
	}

	return b.writeChunkAtPos(i, [][]byte{{0}}, [][]byte{b.buf[:n]}, []uint64{1}, 0)
}

func (b *blobBuilder) writeChunkAtPos(i int, keys, vals [][]byte, subtrees []uint64, level int) error {
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

func (b *blobBuilder) blobHeight(dataSize, chunkSize int) int {
	if dataSize == 0 {
		return 0
	}
	leaves := float64(dataSize) / float64(chunkSize)
	return int(math.Ceil(math.Log2(float64(leaves)) / math.Log2(float64(chunkSize/hash.ByteLen))))
}

func (b *blobBuilder) chunkCount(dataSize, chunkSize int) int {
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

func blobHeight(dataSize, chunkSize int) int {
	if dataSize == 0 {
		return 0
	}
	return int(math.Ceil(math.Log2(float64(dataSize))/math.Log2(float64(chunkSize)))) - 1
}

func chunkCount(chunkSize, height int) int {
	switch height {
	case 0:
		return 1
	case 1:
		return chunkSize + 1
	case 2:
		return chunkSize*chunkSize + 1
	case 3:
		return chunkSize*chunkSize*chunkSize + 1
	default:
		chunkCnt := 1
		for i := 1; i < height; i++ {
			// precalculate chunk count
			chunkCnt += int(math.Pow(float64(chunkSize), float64(i)))
		}
		return chunkCnt
	}
}

func iterChunkChildren(chunks []hash.Hash, r, chunkSize int, cb func(h hash.Hash)) {
	for i := 0; i < chunkSize; i++ {
		cb(chunks[r+i*chunkSize])
	}
}

func heightForIndex(i, height, chunkSize float64) int {
	l := 0.0
	h := height
	var m float64
	var mi float64
	for {
		m = l + (h-l)/2
		mi = math.Pow(chunkSize, m)
		switch {
		case mi == i || h == l:
			return int(m)
		case mi < i:
			l = m + 1
		case mi > i:
			h = m
		}
	}
}

func _newInternal(ctx context.Context, ns NodeStore, s message.Serializer, nodes []novelNode, level int, chunkSize int) (novelNode, error) {
	keys := make([][]byte, len(nodes))
	vals := make([][]byte, len(nodes))
	subtrees := make([]uint64, len(nodes))
	treeCnt := uint64(0)
	for i := range nodes {
		keys[i] = []byte{0}
		vals[i] = nodes[i].addr[:]
		subtrees[i] = nodes[i].treeCount
		treeCnt += nodes[i].treeCount
	}
	msg := s.Serialize(keys, vals, subtrees, level)
	node, err := NodeFromBytes(msg)
	if err != nil {
		return novelNode{}, err
	}
	addr, err := ns.Write(ctx, node)
	if err != nil {
		return novelNode{}, err
	}
	return novelNode{
		addr:      addr,
		node:      node,
		lastKey:   []byte{0},
		treeCount: treeCnt,
	}, nil
}

func _newLeaf(ctx context.Context, ns NodeStore, s message.Serializer, buf []byte) (novelNode, error) {
	msg := s.Serialize([][]byte{{0}}, [][]byte{buf}, []uint64{1}, 0)
	node, err := NodeFromBytes(msg)
	if err != nil {
		return novelNode{}, err
	}
	addr, err := ns.Write(ctx, node)
	if err != nil {
		return novelNode{}, err
	}
	return novelNode{
		addr:      addr,
		node:      node,
		lastKey:   []byte{0},
		treeCount: 1,
	}, nil
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

func NewImmutableTreeFromReader(ctx context.Context, r io.Reader, ns NodeStore, dataSize, chunkSize int) (*ImmutableTree, error) {
	s := message.NewBlobSerializer(ns.Pool())
	root, err := buildImmutableTree(ctx, r, ns, s, dataSize, chunkSize)
	if errors.Is(err, io.EOF) {
		return &ImmutableTree{Addr: hash.Hash{}}, nil
	} else if err != nil {
		return nil, err
	}
	return &ImmutableTree{Addr: root.HashOf()}, nil
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
