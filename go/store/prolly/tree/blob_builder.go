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
	"context"
	"errors"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	sqltypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/goccy/go-json"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

const DefaultFixedChunkLength = 4000

var ErrInvalidChunkSize = errors.New("invalid chunkSize; value must be a multiple of 20")

func mustNewBlobBuilder(chunkSize int) *BlobBuilder {
	b, _ := NewBlobBuilder(chunkSize)
	return b
}

// NewBlobBuilder writes the contents of |reader| as an append-only
// tree, returning the root node or an error if applicable. |chunkSize|
// fixes the split size of leaf and intermediate node chunks.
func NewBlobBuilder(chunkSize int) (*BlobBuilder, error) {
	if chunkSize%hash.ByteLen != 0 {
		return nil, ErrInvalidChunkSize
	}

	keys := make([][]byte, chunkSize/hash.ByteLen)
	for i := range keys {
		keys[i] = zeroKey
	}
	return &BlobBuilder{
		chunkSize: chunkSize,
		keys:      keys,
	}, nil
}

type blobNodeWriter interface {
	Write(ctx context.Context, r io.Reader) (hash.Hash, uint64, error)
}

type BlobBuilder struct {
	ns        NodeStore
	S         message.Serializer
	chunkSize int
	keys      [][]byte
	wr        blobNodeWriter
	lastN     Node
	topLevel  int

	levelCap int
	buf      []byte
	vals     [][]byte
	subtrees []uint64
}

func (b *BlobBuilder) SetNodeStore(ns NodeStore) {
	b.ns = ns
	b.S = message.NewBlobSerializer(ns.Pool())
}

// Reset clears the BlobBuilder for re-use.
func (b *BlobBuilder) Reset() {
	b.wr = nil
	b.topLevel = 0
	b.buf = nil
	b.vals = nil
	b.subtrees = nil
	b.lastN = Node{}
	b.levelCap = 0
}

// Init calculates tree dimensions for a given blob.
func (b *BlobBuilder) Init(dataSize int) {
	if dataSize == 0 {
		return
	}

	if dataSize <= b.chunkSize {
		b.wr = &blobLeafWriter{
			bb:  b,
			buf: make([]byte, dataSize),
		}
		return
	}

	b.wr = &blobLeafWriter{
		bb:  b,
		buf: make([]byte, b.chunkSize),
	}

	numAddrs := b.chunkSize / hash.ByteLen
	dataSize = dataSize / b.chunkSize
	for dataSize > 0 {
		dataSize = dataSize / numAddrs
		b.topLevel += 1
	}

	// Allocate everything we need in batch, slice them up down below.
	if b.levelCap < b.topLevel {
		b.expand(numAddrs)
		b.levelCap = b.topLevel
	}

	writers := make([]blobLevelWriter, b.topLevel)
	for i, addrs := 0, 0; i < b.topLevel; i, addrs = i+1, addrs+numAddrs {
		wr := &writers[i]
		wr.bb = b
		wr.child = b.wr
		wr.buf = b.buf[addrs*hash.ByteLen : (addrs+numAddrs)*hash.ByteLen]
		wr.vals = b.vals[addrs : addrs+numAddrs]
		wr.subtrees = b.subtrees[addrs : addrs+numAddrs]
		wr.level = i + 1
		wr.sz = numAddrs
		b.wr = wr
	}
}

func (b *BlobBuilder) expand(numAddrs int) {
	b.buf = make([]byte, b.topLevel*numAddrs*hash.ByteLen)
	b.vals = make([][]byte, numAddrs*b.topLevel)
	b.subtrees = make([]uint64, numAddrs*b.topLevel)
}

// Chunk builds the blob tree by passing the Reader to the chain of level
// writers, terminated in a leaf writer. The leaf writer reads chunks from the
// Reader and writes them, returning their hashes to its parent level writer.
// When the parent level writer fills up with addresses, it writes a chunk and
// returns that address to its parent. This continues until the Reader returns
// io.EOF, when every writer in the chain completes its chunk and we return the
// root node.
func (b *BlobBuilder) Chunk(ctx context.Context, r io.Reader) (Node, hash.Hash, error) {
	if b.wr == nil {
		return Node{}, hash.Hash{}, nil
	}
	h, _, err := b.wr.Write(ctx, r)
	if err != nil && err != io.EOF {
		return Node{}, hash.Hash{}, err
	}
	return b.lastN, h, nil
}

// blobLeafWriter writes leaf chunks of the blob, with max capacity len(buf),
// for every call to Write().
type blobLeafWriter struct {
	bb  *BlobBuilder
	buf []byte
}

var zeroKey = []byte{0}
var zeroKeys = [][]byte{zeroKey}
var leafSubtrees = []uint64{1}

func (lw *blobLeafWriter) Write(ctx context.Context, r io.Reader) (hash.Hash, uint64, error) {
	n, err := r.Read(lw.buf)
	if err != nil {
		return hash.Hash{}, 0, err
	}
	h, err := lw.bb.write(ctx, zeroKeys, [][]byte{lw.buf[:n]}, leafSubtrees, 0)
	return h, 1, err
}

// blobLevelWriters writes internal chunks of a blob, using its |child| to
// write the level below it. On a call to |Write|, it repeatedly calls
// |child.Write|, accumulating addresses to its children, until it fills up or
// the Reader is exhausted. In either case, it then writes its node and
// returns.
type blobLevelWriter struct {
	bb       *BlobBuilder
	child    blobNodeWriter
	buf      []byte
	vals     [][]byte
	subtrees []uint64
	sz       int
	level    int
}

func (lw *blobLevelWriter) Write(ctx context.Context, r io.Reader) (hash.Hash, uint64, error) {
	i, off, totalCount := 0, 0, uint64(0)
	for {
		// Sketchy hack to elide a copy here...
		//h := (*hash.Hash)(unsafe.Pointer(&lw.buf[off]))
		//var n uint64
		//var err error
		h, n, err := lw.child.Write(ctx, r)
		if err != nil && err != io.EOF {
			return hash.Hash{}, 0, err
		}
		if n != 0 {
			totalCount += n
			copy(lw.buf[off:], h[:])
			lw.subtrees[i] = n
			lw.vals[i] = lw.buf[off : off+hash.ByteLen]
			i += 1
			off += hash.ByteLen
		}
		if i >= lw.sz || err == io.EOF {
			h, nerr := lw.bb.write(ctx, lw.bb.keys[:i], lw.vals[:i], lw.subtrees[:i], lw.level)
			if nerr != nil {
				return hash.Hash{}, 0, nerr
			}
			return h, totalCount, err
		}
	}
}

// Write the blob node. Called by level and leaf writers. Will store lastN if
// the level corresponds to our root level.
func (b *BlobBuilder) write(ctx context.Context, keys, vals [][]byte, subtrees []uint64, level int) (hash.Hash, error) {
	msg := b.S.Serialize(keys, vals, subtrees, level)
	node, _, err := NodeFromBytes(msg)
	if err != nil {
		return hash.Hash{}, err
	}
	h, err := b.ns.Write(ctx, node)
	if err != nil {
		return hash.Hash{}, err
	}
	if level == b.topLevel {
		b.lastN = node
	}
	return h, nil
}

type JSONDoc struct {
	val.ImmutableValue
	ns NodeStore
}

func NewJSONDoc(addr hash.Hash, ns NodeStore) *JSONDoc {
	return &JSONDoc{ImmutableValue: val.NewImmutableValue(addr, ns), ns: ns}
}

func (b *JSONDoc) ToJSONDocument(ctx context.Context) (sqltypes.JSONDocument, error) {
	buf, err := b.GetBytes(ctx)
	if err != nil {
		return sqltypes.JSONDocument{}, err
	}
	var doc sqltypes.JSONDocument
	err = json.Unmarshal(buf, &doc.Val)
	if err != nil {
		return sqltypes.JSONDocument{}, err
	}
	return doc, err
}

func (b *JSONDoc) ToLazyJSONDocument(ctx context.Context) (sql.JSONWrapper, error) {
	buf, err := b.GetBytes(ctx)
	if err != nil {
		return sqltypes.JSONDocument{}, err
	}
	buf = types.UnescapeHTMLCodepoints(buf)
	return sqltypes.NewLazyJSONDocument(buf), nil
}

func (b *JSONDoc) ToIndexedJSONDocument(ctx context.Context) (sql.JSONWrapper, error) {
	root, err := b.ns.Read(ctx, b.Addr)
	if err != nil {
		return nil, err
	}
	if root.level > 0 && root.keys.IsEmpty() {
		// We're reading a non-indexed multi-chunk document written by an older version of Dolt.
		return b.ToLazyJSONDocument(ctx)
	}
	return NewIndexedJsonDocument(ctx, root, b.ns), nil
}

func (b *JSONDoc) ToString(ctx context.Context) (string, error) {
	buf, err := b.GetBytes(ctx)
	if err != nil {
		return "", err
	}
	toShow := val.BytePeekLength
	if len(buf) < toShow {
		toShow = len(buf)
	}
	return string(buf[:toShow]), nil
}
