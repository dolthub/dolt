// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/util/orderedparallel"
)

// Blob represents a list of Blobs.
type Blob struct {
	seq sequence
	h   *hash.Hash
}

func newBlob(seq sequence) Blob {
	return Blob{seq, &hash.Hash{}}
}

func NewEmptyBlob() Blob {
	return Blob{newBlobLeafSequence(nil, []byte{}), &hash.Hash{}}
}

// BUG 155 - Should provide Write... Maybe even have Blob implement ReadWriteSeeker
func (b Blob) Reader() io.ReadSeeker {
	iter := newSequenceIterator(b.seq, 0)
	return &BlobReader{b.seq, iter, nil, 0}

}

func (b Blob) Splice(idx uint64, deleteCount uint64, data []byte) Blob {
	if deleteCount == 0 && len(data) == 0 {
		return b
	}

	d.PanicIfFalse(idx <= b.Len())
	d.PanicIfFalse(idx+deleteCount <= b.Len())

	ch := b.newChunker(newCursorAtIndex(b.seq, idx), b.seq.valueReader())
	for deleteCount > 0 {
		ch.Skip()
		deleteCount--
	}

	for _, v := range data {
		ch.Append(v)
	}
	return newBlob(ch.Done())
}

// Concat returns a new Blob comprised of this joined with other. It only needs
// to visit the rightmost prolly tree chunks of this Blob, and the leftmost
// prolly tree chunks of other, so it's efficient.
func (b Blob) Concat(other Blob) Blob {
	seq := concat(b.seq, other.seq, func(cur *sequenceCursor, vr ValueReader) *sequenceChunker {
		return b.newChunker(cur, vr)
	})
	return newBlob(seq)
}

func (b Blob) newChunker(cur *sequenceCursor, vr ValueReader) *sequenceChunker {
	return newSequenceChunker(cur, vr, nil, makeBlobLeafChunkFn(vr), newIndexedMetaSequenceChunkFn(BlobKind, vr), hashValueByte)
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
	return b.Hash() == other.Hash()
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

func (b Blob) WalkValues(cb ValueCallback) {
}

func (b Blob) WalkRefs(cb RefCallback) {
	b.seq.WalkRefs(cb)
}

func (b Blob) Type() *Type {
	return b.seq.Type()
}

type BlobReader struct {
	seq           sequence
	iter          *sequenceIterator
	currentReader io.ReadSeeker
	pos           uint64
}

func (cbr *BlobReader) Read(p []byte) (n int, err error) {
	if cbr.currentReader == nil {
		cbr.updateReader()
	}

	n, err = cbr.currentReader.Read(p)
	cbr.pos += uint64(n)
	hasMore := cbr.iter.advance(n)
	if err == io.EOF && hasMore {
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
	cbr.iter = newSequenceIterator(cbr.seq, cbr.pos)
	cbr.currentReader = nil
	return abs, nil
}

func (cbr *BlobReader) updateReader() {
	chunk, idx := cbr.iter.chunkAndIndex()
	data := chunk.(blobLeafSequence).data
	cbr.currentReader = bytes.NewReader(data)
	cbr.currentReader.Seek(int64(idx), 0)
}

func makeBlobLeafChunkFn(vr ValueReader) makeChunkFn {
	return func(items []sequenceItem) (Collection, orderedKey, uint64) {
		buff := make([]byte, len(items))

		for i, v := range items {
			buff[i] = v.(byte)
		}

		return chunkBlobLeaf(vr, buff)
	}
}

func chunkBlobLeaf(vr ValueReader, buff []byte) (Collection, orderedKey, uint64) {
	blob := newBlob(newBlobLeafSequence(vr, buff))
	return blob, orderedKeyFromInt(len(buff)), uint64(len(buff))
}

// NewBlob creates a Blob by reading from every Reader in rs and concatenating
// the result. NewBlob uses one goroutine per Reader. Chunks are kept in memory
// as they're created - to reduce memory pressure and write to disk instead,
// use NewStreamingBlob with a non-nil reader.
func NewBlob(rs ...io.Reader) Blob {
	return readBlobsP(nil, rs...)
}

// NewStreamingBlob creates a Blob by reading from every Reader in rs and
// concatenating the result. NewStreamingBlob uses one goroutine per Reader.
// If vrw is not nil, chunks are written to vrw instead of kept in memory.
func NewStreamingBlob(vrw ValueReadWriter, rs ...io.Reader) Blob {
	return readBlobsP(vrw, rs...)
}

func readBlobsP(vrw ValueReadWriter, rs ...io.Reader) Blob {
	switch len(rs) {
	case 0:
		return NewEmptyBlob()
	case 1:
		return readBlob(rs[0], vrw)
	}

	blobs := make([]Blob, len(rs))

	wg := &sync.WaitGroup{}
	wg.Add(len(rs))

	for i, r := range rs {
		i2, r2 := i, r
		go func() {
			blobs[i2] = readBlob(r2, vrw)
			wg.Done()
		}()
	}

	wg.Wait()

	b := blobs[0]
	for i := 1; i < len(blobs); i++ {
		b = b.Concat(blobs[i])
	}
	return b
}

func readBlob(r io.Reader, vrw ValueReadWriter) Blob {
	sc := newEmptySequenceChunker(vrw, vrw, makeBlobLeafChunkFn(nil), newIndexedMetaSequenceChunkFn(BlobKind, nil), func(item sequenceItem, rv *rollingValueHasher) {
		rv.HashByte(item.(byte))
	})

	// TODO: The code below is temporary. It's basically a custom leaf-level chunker for blobs. There are substational perf gains by doing it this way as it avoids the cost of boxing every single byte which is chunked.
	chunkBuff := [8192]byte{}
	chunkBytes := chunkBuff[:]
	rv := newRollingValueHasher()
	offset := 0
	addByte := func(b byte) bool {
		if offset >= len(chunkBytes) {
			tmp := make([]byte, len(chunkBytes)*2)
			copy(tmp, chunkBytes)
			chunkBytes = tmp
		}
		chunkBytes[offset] = b
		offset++
		rv.HashByte(b)
		return rv.crossedBoundary
	}

	input := make(chan interface{}, 16)
	output := orderedparallel.New(input, func(item interface{}) interface{} {
		cp := item.([]byte)
		col, key, numLeaves := chunkBlobLeaf(vrw, cp)
		var ref Ref
		if vrw != nil {
			ref = vrw.WriteValue(col)
			col = nil
		} else {
			ref = NewRef(col)
		}
		return newMetaTuple(ref, key, numLeaves, col)
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
					rv.ClearLastBoundary()
					makeChunk()
				}
			}
			if err != nil {
				d.PanicIfFalse(io.EOF == err)
				if offset > 0 {
					makeChunk()
				}
				close(input)
				break
			}
		}
	}()

	for b := range output {
		if sc.parent == nil {
			sc.createParent()
		}
		sc.parent.Append(b.(metaTuple))
	}

	return newBlob(sc.Done())
}
