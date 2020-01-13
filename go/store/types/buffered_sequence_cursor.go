// Copyright 2019 Liquidata, Inc.
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
	"errors"

	"github.com/liquidata-inc/dolt/go/store/d"
	"github.com/liquidata-inc/dolt/go/store/hash"
)


type bufSeqCurImpl struct {
	seqStream <-chan sequence
	curLeafSeq sequence
	curLeafIdx uint64
	errChan    chan error
}

func newBufferedSequenceCursor(ctx context.Context, sourceSeq sequence, bufSize int32) (*bufSeqCurImpl, error) {
	d.PanicIfTrue(sourceSeq == nil)
	errChan := make(chan error)

	leafBuffer := make(chan sequence, bufSize)
	go walkSequenceTree(ctx, leafBuffer, sourceSeq, errChan)

	select {
	case err := <-errChan:
		return nil, err
	case firstLeaf := <-leafBuffer:
		return &bufSeqCurImpl{leafBuffer, firstLeaf, 0, errChan}, nil
	}
}

func (bc *bufSeqCurImpl) current() (sequenceItem, error) {
	if !bc.valid() {
		// sequence_cursor panics here, but that's clown town
		return nil, errors.New("sequence is exhausted")
	}
	return bc.curLeafSeq.getItem(int(bc.curLeafIdx))
}

func (bc *bufSeqCurImpl) advance(ctx context.Context) (bool, error) {
	bc.curLeafIdx++
	if bc.curLeafIdx == bc.curLeafSeq.Len() {
		// grab the next chunk's leaf sequence
		select {
		case err := <-bc.errChan:
			return false, err
		case bc.curLeafSeq = <-bc.seqStream:
			bc.curLeafIdx = 0
		}
	}

	return true, nil
}

func (bc *bufSeqCurImpl) valid() bool {
	return bc.curLeafSeq != nil && bc.curLeafIdx < bc.curLeafSeq.Len()
}

func walkSequenceTree(ctx context.Context, leafBuffer chan sequence, sourceSeq sequence, ec chan error) {
	defer close(leafBuffer)

	vrw := sourceSeq.valueReadWriter()
	h, err := sourceSeq.Hash(sourceSeq.format())
	if err != nil {
		ec <- err
		return
	}

	v, err := vrw.ReadValue(ctx, h)
	if err != nil {
		ec <- err
		return
	}

	const concurrency = 6
	const readAtTreeLevel = 2
	lifo := ValueSlice{v}
	cc := make(chan chan sequence, concurrency)
	go func() {
		defer close(cc)
		// depth first search the tree
		for len(lifo) > 0 {
			// pop the stack
			val := lifo[0]
			lifo = lifo[1:]

			subTree := val.(sequence)
			if subTree.isLeaf() {
				// leaf in unbalanced tree
				leafBuffer <- subTree
				continue
			}

			if subTree.treeLevel() <= readAtTreeLevel {
				// spawn a reader go routine to buffer this subtree
				subTreeSink := make(chan sequence, bufSizeForLevel(readAtTreeLevel))
				cc <- subTreeSink
				go bufferSubTreeChunks(ctx, subTree, subTreeSink, ec)
			} else {
				// push child refs to LIFO queue
				valSlice, err := vrw.ReadManyValues(ctx, refHashes(subTree))
				if err != nil {
					ec <- err
				}
				lifo = append(valSlice, lifo...)
			}
		}
	}()

	// drain reader buffers into the leafBuffer
	// until the tree is exhausted
	for subTreeSink := range cc {
		for leafSeq := range subTreeSink {
			leafBuffer <- leafSeq
		}
	}
}

func bufferSubTreeChunks(ctx context.Context, subTree sequence, sink chan sequence, ec chan error) {
	defer close(sink)

	vrw := subTree.valueReadWriter()
	hs := refHashes(subTree)
	refStack, err := vrw.ReadManyValues(ctx, hs)
	refStack, _ = pruneMissingValues(refStack, hs)
	if err != nil {
		ec <- err
	}

	// alternate fetching subtrees and sending
	// chunks until the subtree is exhausted
	for len(refStack) > 0 {
		for _, v := range refStack {
			s := v.(sequence)
			if s.isLeaf() {
				sink <- s
				refStack = refStack[1:]
			}
		}

		var newHashes hash.HashSlice
		for _, v := range refStack {
			s := v.(sequence)
			if !s.isLeaf() {
				newHashes = append(newHashes, refHashes(s)...)
				refStack = refStack[1:]
			} else {
				newValues, err := vrw.ReadManyValues(ctx, newHashes)
				if err != nil {
					ec <- err
				}
				newValues, _ = pruneMissingValues(newValues, newHashes)
				refStack = append(newValues, refStack...)
			}
		}
	}
}

func refHashes(s sequence) hash.HashSlice {
	hs := make(hash.HashSlice, s.Len())
	i := 0
	_ = s.WalkRefs(s.format(), func(ref Ref) error {
		hs[i] = ref.TargetHash()
		i++
		return nil
	})
	return hs
}

func pruneMissingValues(vs ValueSlice, hs hash.HashSlice) (ValueSlice, hash.HashSlice) {
	d.PanicIfFalse(hs.Len() == len(vs))
	var pruned ValueSlice
	var missing hash.HashSlice
	for i, val := range vs {
		if val != nil {
			pruned = append(pruned, val)
		} else {
			missing = append(missing, hs[i])
		}
	}
	return pruned, missing
}

// Calculate an approximate upper bound for the
// number of chunks in this subtree given that
// PTree nodes contain 64 children on average.
func bufSizeForLevel(level int) int32 {
	const errMargin = 1.5
	size := 1
	for i := 0; i < level; i++ {
		size = size * 64
	}
	return int32(float64(size) * errMargin)
}
