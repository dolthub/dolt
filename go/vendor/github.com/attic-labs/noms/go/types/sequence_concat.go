// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/d"

type newSequenceChunkerFn func(cur *sequenceCursor, vrw ValueReadWriter) *sequenceChunker

func concat(fst, snd sequence, newSequenceChunker newSequenceChunkerFn) sequence {
	if fst.numLeaves() == 0 {
		return snd
	}
	if snd.numLeaves() == 0 {
		return fst
	}

	// concat works by tricking the sequenceChunker into resuming chunking at a
	// cursor to the end of fst, then finalizing chunking to the start of snd - by
	// swapping fst cursors for snd cursors in the middle of chunking.
	vrw := fst.valueReadWriter()
	if vrw != snd.valueReadWriter() {
		d.Panic("cannot concat sequences from different databases")
	}
	chunker := newSequenceChunker(newCursorAtIndex(fst, fst.numLeaves()), vrw)

	for cur, ch := newCursorAtIndex(snd, 0), chunker; ch != nil; ch = ch.parent {
		// Note that if snd is shallower than fst, then higher chunkers will have
		// their cursors set to nil. This has the effect of "dropping" the final
		// item in each of those sequences.
		ch.cur = cur
		if cur != nil {
			cur = cur.parent
			if cur != nil && ch.parent == nil {
				// If fst is shallower than snd, its cur will have a parent whereas the
				// chunker to snd won't. In that case, create a parent for fst.
				ch.createParent()
			}
		}
	}

	return chunker.Done()
}
