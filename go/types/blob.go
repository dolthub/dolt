// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"errors"
	"io"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/util/orderedparallel"

	"github.com/kch42/buzhash"
)

const (
	blobPattern = uint32(1<<11 - 1) // Avg Chunk Size of 2k

	// The window size to use for computing the rolling hash.
	blobWindowSize = 64
)

var RefOfBlobType = MakeRefType(BlobType)

// Blob represents a list of Blobs.
type Blob struct {
	seq indexedSequence
	h   *hash.Hash
}

func newBlob(seq indexedSequence) Blob {
	return Blob{seq, &hash.Hash{}}
}

func NewEmptyBlob() Blob {
	return Blob{newBlobLeafSequence(nil, []byte{}), &hash.Hash{}}
}

// BUG 155 - Should provide Write... Maybe even have Blob implement ReadWriteSeeker
func (b Blob) Reader() io.ReadSeeker {
	cursor := newCursorAtIndex(b.seq, 0)
	return &BlobReader{b.seq, cursor, nil, 0}
}

// Collection interface
func (b Blob) Len() uint64 {
	return b.seq.numLeaves()
}

func (b Blob) Empty() bool {
	return b.Len() == 0
}

func (b Blob) sequence() sequence {
	return b.seq
}

func (b Blob) hashPointer() *hash.Hash {
	return b.h
}

// Value interface
func (b Blob) Equals(other Value) bool {
	return other != nil && b.Hash() == other.Hash()
}

func (b Blob) Less(other Value) bool {
	return valueLess(b, other)
}

func (b Blob) Hash() hash.Hash {
	if b.h.IsEmpty() {
		*b.h = getHash(b)
	}

	return *b.h
}

func (b Blob) ChildValues() []Value {
	return []Value{}
}

func (b Blob) Chunks() []Ref {
	return b.seq.Chunks()
}

func (b Blob) Type() *Type {
	return b.seq.Type()
}

type BlobReader struct {
	seq           indexedSequence
	cursor        *sequenceCursor
	currentReader io.ReadSeeker
	pos           uint64
}

func (cbr *BlobReader) Read(p []byte) (n int, err error) {
	if cbr.currentReader == nil {
		cbr.updateReader()
	}

	n, err = cbr.currentReader.Read(p)
	for i := 0; i < n; i++ {
		cbr.pos++
		cbr.cursor.advance()
	}
	if err == io.EOF && cbr.cursor.idx < cbr.cursor.seq.seqLen() {
		cbr.currentReader = nil
		err = nil
	}

	return
}

func (cbr *BlobReader) Seek(offset int64, whence int) (int64, error) {
	abs := int64(cbr.pos)

	switch whence {
	case 0:
		abs = offset
	case 1:
		abs += offset
	case 2:
		abs = int64(cbr.seq.numLeaves()) + offset
	default:
		return 0, errors.New("Blob.Reader.Seek: invalid whence")
	}

	if abs < 0 {
		return 0, errors.New("Blob.Reader.Seek: negative position")
	}

	cbr.pos = uint64(abs)
	cbr.cursor = newCursorAtIndex(cbr.seq, cbr.pos)
	cbr.currentReader = nil
	return abs, nil
}

func (cbr *BlobReader) updateReader() {
	cbr.currentReader = bytes.NewReader(cbr.cursor.seq.(blobLeafSequence).data)
	cbr.currentReader.Seek(int64(cbr.cursor.idx), 0)
}

func newBlobLeafBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(blobWindowSize, 1, blobPattern, func(item sequenceItem) []byte {
		return []byte{item.(byte)}
	})
}

func newBlobLeafChunkFn(vr ValueReader, sink ValueWriter) makeChunkFn {
	return func(items []sequenceItem) (metaTuple, sequence) {
		buff := make([]byte, len(items))

		for i, v := range items {
			buff[i] = v.(byte)
		}

		return chunkBlobLeaf(vr, sink, buff)
	}
}

func chunkBlobLeaf(vr ValueReader, sink ValueWriter, buff []byte) (metaTuple, sequence) {
	seq := newBlobLeafSequence(vr, buff)
	blob := newBlob(seq)

	var ref Ref
	var child Collection
	if sink != nil {
		// Eagerly write chunks
		ref = sink.WriteValue(blob)
		child = nil
	} else {
		ref = NewRef(blob)
		child = blob
	}

	return newMetaTuple(ref, orderedKeyFromInt(len(buff)), uint64(len(buff)), child), seq
}

func NewBlob(r io.Reader) Blob {
	return NewStreamingBlob(r, nil)
}

type tempBlob struct {
	seq sequence
	mt  metaTuple
}

func NewStreamingBlob(r io.Reader, vrw ValueReadWriter) Blob {
	sc := newEmptySequenceChunker(newBlobLeafChunkFn(nil, vrw), newIndexedMetaSequenceChunkFn(BlobKind, nil, vrw), newBlobLeafBoundaryChecker(), newIndexedMetaSequenceBoundaryChecker)

	// TODO: The code below is a temporary. It's basically a custom leaf-level chunker for blobs. There are substational perf gains by doing it this way as it avoids the cost of boxing every single byte which is chunked.
	chunkBuff := [8192]byte{}
	chunkBytes := chunkBuff[:]
	bh := buzhash.NewBuzHash(blobWindowSize)
	offset := 0
	addByte := func(b byte) bool {
		if offset >= len(chunkBytes) {
			tmp := make([]byte, len(chunkBytes)*2)
			copy(tmp, chunkBytes)
			chunkBytes = tmp
		}
		chunkBytes[offset] = b
		offset++
		bh.HashByte(b)
		return bh.Sum32()&blobPattern == blobPattern
	}

	input := make(chan interface{}, 16)
	output := orderedparallel.New(input, func(item interface{}) interface{} {
		cp := item.([]byte)
		mt, seq := chunkBlobLeaf(vrw, vrw, cp)
		return tempBlob{seq, mt}
	}, 16)

	makeChunk := func() {
		cp := make([]byte, offset)
		copy(cp, chunkBytes[0:offset])
		input <- cp
		offset = 0
	}

	go func() {
		readBuff := [8192]byte{}
		for {
			n, err := r.Read(readBuff[:])
			for i := 0; i < n; i++ {
				if addByte(readBuff[i]) {
					makeChunk()
				}
			}
			if err != nil {
				d.Chk.True(io.EOF == err)
				if offset > 0 {
					makeChunk()
				}
				close(input)
				break
			}
		}
	}()

	for b := range output {
		tb := b.(tempBlob)
		sc.lastSeq = tb.seq
		if sc.parent == nil {
			sc.createParent()
		}
		sc.parent.Append(tb.mt)
	}

	return newBlob(sc.Done().(indexedSequence))
}
