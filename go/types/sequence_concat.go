// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/d"

type newSequenceChunkerFn func(cur *sequenceCursor, vr ValueReader) *sequenceChunker

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
	vr := fst.valueReader()
	d.PanicIfTrue(vr != snd.valueReader(), "cannot concat sequences from different databases")
	chunker := newSequenceChunker(newCursorAtIndex(fst, fst.numLeaves()), vr)

	for cur, ch := newCursorAtIndex(snd, 0), chunker; cur != nil; cur = cur.parent {
		// If fst is shallower than snd, its cur will have a parent whereas the
		// chunker to snd won't. In that case, create a parent for fst.
		// Note that if the inverse is true - snd is shallower than fst - this just
		// means higher chunker levels will still have cursors from fst... which
		// point to the end, so finalisation won't do anything. This is correct.
		if ch.parent == nil {
			ch.createParent()
		}
		ch.cur = cur.clone()
		ch = ch.parent
	}

	return chunker.Done()
}
