// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"errors"

	"io"

	"sync"

	"github.com/attic-labs/noms/go/d"
)

type BlobEditor struct {
	b     Blob
	edits *blobEdit
	pos   int64
}

func NewBlobEditor(b Blob) *BlobEditor {
	return &BlobEditor{b, nil, 0}
}

func (be *BlobEditor) Kind() NomsKind {
	return BlobKind
}

func (be *BlobEditor) Value() Value {
	return be.Blob()
}

func (be *BlobEditor) Blob() Blob {
	if be.edits == nil {
		return be.b // no edits
	}

	seq := be.b.sequence
	vrw := seq.valueReadWriter()

	curs := make([]chan *sequenceCursor, 0)
	for edit := be.edits; edit != nil; edit = edit.next {
		edit := edit

		// TODO: Use ReadMany
		cc := make(chan *sequenceCursor, 1)
		curs = append(curs, cc)
		go func() {
			cc <- newCursorAtIndex(seq, edit.idx)
		}()
	}

	var ch *sequenceChunker
	idx := 0
	for edit := be.edits; edit != nil; edit = edit.next {
		cur := <-curs[idx]
		idx++

		if ch == nil {
			ch = newSequenceChunker(cur, 0, vrw, makeBlobLeafChunkFn(vrw), newIndexedMetaSequenceChunkFn(BlobKind, vrw), hashValueByte)
		} else {
			ch.advanceTo(cur)
		}

		dc := edit.removed
		for dc > 0 {
			ch.Skip()
			dc--
		}

		for _, v := range edit.inserted {
			ch.Append(v)
		}
	}

	return newBlob(ch.Done())
}

func collapseBlobEdit(newEdit, edit *blobEdit) bool {
	if newEdit.idx+newEdit.removed < edit.idx ||
		edit.idx+uint64(len(edit.inserted)) < newEdit.idx {
		return false
	}

	collapsed := &blobEdit{}

	if newEdit.idx <= edit.idx {
		collapsed.idx = newEdit.idx

		overlap := newEdit.removed - (edit.idx - newEdit.idx) // number of leading N values removed from edit.inserted
		if overlap < uint64(len(edit.inserted)) {
			// newEdit doesn't remove all of edit.inserted
			collapsed.inserted = append(newEdit.inserted, edit.inserted[overlap:]...)
			collapsed.removed = newEdit.removed + edit.removed - overlap
		} else {
			// newEdit removes all of edit.inserted
			collapsed.inserted = newEdit.inserted
			collapsed.removed = newEdit.removed + edit.removed - uint64(len(edit.inserted))
		}
	} else {
		// edit.idx < newEdit.idx

		collapsed.idx = edit.idx

		editInsertedLen := uint64(len(edit.inserted))
		beginEditRemovePoint := newEdit.idx - edit.idx

		if beginEditRemovePoint == editInsertedLen {
			// newEdit took place at the position immediately after the last element of edit.inserted
			collapsed.inserted = append(edit.inserted, newEdit.inserted...)
			collapsed.removed = edit.removed + newEdit.removed
		} else {
			// newEdit takes place within edit.inserted
			collapsed.inserted = append(collapsed.inserted, edit.inserted[:beginEditRemovePoint]...)
			collapsed.inserted = append(collapsed.inserted, newEdit.inserted...)

			endEditRemovePoint := beginEditRemovePoint + newEdit.removed
			if endEditRemovePoint < editInsertedLen {
				// elements of edit.inserted remain beyond newEdit.removed
				collapsed.removed = edit.removed
				collapsed.inserted = append(collapsed.inserted, edit.inserted[endEditRemovePoint:]...)
			} else {
				collapsed.removed = edit.removed + endEditRemovePoint - editInsertedLen
			}
		}
	}

	*newEdit = *collapsed
	return true
}

func (be *BlobEditor) Len() uint64 {
	delta := int64(0)
	for edit := be.edits; edit != nil; edit = edit.next {
		delta += -int64(edit.removed) + int64(len(edit.inserted))
	}

	return uint64(int64(be.b.Len()) + delta)
}

func (be *BlobEditor) Splice(idx uint64, deleteCount uint64, insert []byte) *BlobEditor {
	ne := &blobEdit{idx, deleteCount, insert, nil}

	var last *blobEdit
	edit := be.edits

	for edit != nil {
		if collapseBlobEdit(ne, edit) {
			if last == nil {
				be.edits = edit.next
			} else {
				last.next = edit.next
			}

			edit = edit.next
			continue
		}

		if edit.idx > ne.idx {
			break
		}

		ne.idx = adjustBlobIdx(ne.idx, edit)
		last = edit
		edit = edit.next
	}

	if ne.removed == 0 && len(ne.inserted) == 0 {
		return be // effectively removed 1 or more existing slices
	}

	if ne.idx > be.b.Len() {
		d.Panic("Index Out Of Bounds")
	}
	if ne.idx == be.b.Len() && ne.removed > 0 {
		d.Panic("Index Out Of Bounds")
	}

	if last == nil {
		// Insert |ne| in first position
		ne.next = be.edits
		be.edits = ne
	} else {
		ne.next = last.next
		last.next = ne
	}

	return be
}

func (be *BlobEditor) Seek(offset int64, whence int) (int64, error) {
	abs := int64(be.pos)

	switch whence {
	case 0:
		abs = offset
	case 1:
		abs += offset
	case 2:
		abs = int64(be.Len()) + offset
	default:
		return 0, errors.New("BlobEditor.Seek: invalid whence")
	}

	if abs < 0 {
		return 0, errors.New("BlobEditor.Seek: negative position")
	}

	if uint64(abs) > be.Len() {
		return 0, errors.New("BlobEditor.Seek: sparse blobs not supported")
	}

	be.pos = int64(abs)
	return abs, nil
}

func (be *BlobEditor) Read(p []byte) (n int, err error) {
	startIdx := uint64(be.pos)
	endIdx := startIdx + uint64(len(p))
	if endIdx > be.Len() {
		endIdx = be.Len()
	}
	n = int(endIdx - startIdx)
	if endIdx == be.Len() {
		err = io.EOF
	}

	wg := &sync.WaitGroup{}
	asyncReadAt := func(length uint64) {
		idx := int64(startIdx)
		to := p[:length]
		wg.Add(1)
		go func() {
			be.b.ReadAt(to, idx)
			wg.Done()
		}()

		startIdx += length
		p = p[length:]
	}

	edit := be.edits
	for edit != nil && startIdx < endIdx {
		if edit.idx > startIdx {
			// ReadAt the bytes before the current edit
			end := endIdx
			if endIdx > edit.idx {
				end = edit.idx
			}

			asyncReadAt(end - startIdx)
			continue
		}

		insertedLength := uint64(len(edit.inserted))
		if edit.idx <= startIdx && startIdx < (edit.idx+insertedLength) {
			// Copy bytes within the current edit
			start := startIdx - edit.idx
			end := endIdx - edit.idx
			if end > insertedLength {
				end = insertedLength
			}

			copy(p, edit.inserted[start:end])
			p = p[end-start:]
			startIdx += end - start
			continue
		}

		startIdx = adjustBlobIdx(startIdx, edit)
		endIdx = adjustBlobIdx(endIdx, edit)
		edit = edit.next
	}

	if endIdx > startIdx {
		// ReadAt any bytes beyond the final edit
		asyncReadAt(endIdx - startIdx)
	}

	wg.Wait()
	return
}

func (be *BlobEditor) Write(p []byte) (n int, err error) {
	removeCount := uint64(len(p))
	remaining := be.Len() - uint64(be.pos)
	if remaining < removeCount {
		removeCount = remaining
	}

	be.Splice(uint64(be.pos), removeCount, p)
	return len(p), nil
}

func adjustBlobIdx(idx uint64, e *blobEdit) uint64 {
	return idx + e.removed - uint64(len(e.inserted))
}

type blobEdit struct {
	idx      uint64
	removed  uint64
	inserted []byte
	next     *blobEdit
}
