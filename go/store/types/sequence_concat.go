// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"

	"github.com/dolthub/dolt/go/store/d"
)

type newSequenceChunkerFn func(cur *sequenceCursor, vrw ValueReadWriter) (*sequenceChunker, error)

func concat(ctx context.Context, fst, snd sequence, newSequenceChunker newSequenceChunkerFn) (sequence, error) {
	if fst.numLeaves() == 0 {
		return snd, nil
	}
	if snd.numLeaves() == 0 {
		return fst, nil
	}

	// concat works by tricking the sequenceChunker into resuming chunking at a
	// cursor to the end of fst, then finalizing chunking to the start of snd - by
	// swapping fst cursors for snd cursors in the middle of chunking.
	vrw := fst.valueReadWriter()
	if vrw != snd.valueReadWriter() {
		d.Panic("cannot concat sequences from different databases")
	}

	cur, err := newCursorAtIndex(ctx, fst, fst.numLeaves())

	if err != nil {
		return nil, err
	}

	chunker, err := newSequenceChunker(cur, vrw)

	if err != nil {
		return nil, err
	}

	ch := chunker
	cur, err = newCursorAtIndex(ctx, snd, 0)

	if err != nil {
		return nil, err
	}

	for ; ch != nil; ch = ch.parent {
		// Note that if snd is shallower than fst, then higher chunkers will have
		// their cursors set to nil. This has the effect of "dropping" the final
		// item in each of those sequences.
		ch.cur = cur
		if cur != nil {
			cur = cur.parent
			if cur != nil && ch.parent == nil {
				// If fst is shallower than snd, its cur will have a parent whereas the
				// sequenceSplitter to snd won't. In that case, create a parent for fst.
				err := ch.createParent(ctx)

				if err != nil {
					return nil, err
				}
			}
		}
	}

	return chunker.Done(ctx)
}
