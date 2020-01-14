// Copyright 2020 Liquidata, Inc.
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

const concurrency = 6
const readAtTreeLevel = 2

type bufSeqCurImpl struct {
	seqStream  <-chan sequence
	curLeafSeq sequence
	curLeafIdx uint64
	doneChan   chan interface{}
	errChan    chan error
}

func newBufferedSequenceCursor(ctx context.Context, sourceSeq sequence, requestedBufSize int32) (*bufSeqCurImpl, error) {
	d.PanicIfTrue(sourceSeq == nil)
	errChan := make(chan error)
	doneChan := make(chan interface{})

	leafBuffer := make(chan sequence, bufSize(requestedBufSize))
	go walkSequenceTree(ctx, leafBuffer, sourceSeq, errChan, doneChan)

	select {
	case err := <-errChan:
		return nil, err
	case firstLeaf := <-leafBuffer:
		return &bufSeqCurImpl{leafBuffer, firstLeaf, 0, doneChan, errChan}, nil
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

func (bc *bufSeqCurImpl) close() {
	close(bc.doneChan)
	bc.curLeafSeq = nil
}

func walkSequenceTree(ctx context.Context, leafBuffer chan sequence, sourceSeq sequence, ec chan error, done chan interface{}) {
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

	lifo := ValueSlice{v}
	cc := make(chan chan sequence, concurrency)
	go func() {
		defer close(cc)
		// depth first search the tree
		for len(lifo) > 0 {
			select {
			case <- done:
				return  // stop spawning readers
			default:
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
					subTreeSink := make(chan sequence, subTreeBufSize(readAtTreeLevel))
					cc <- subTreeSink
					go bufferSubTreeChunks(ctx, subTree, subTreeSink, ec, done)
				} else {
					// push child refs to LIFO queue
					valSlice, err := vrw.ReadManyValues(ctx, refHashes(subTree))
					if err != nil {
						ec <- err
					}
					lifo = append(valSlice, lifo...)
				}
			}
		}
	}()

	// drain reader buffers into the leafBuffer
	// until the tree is exhausted
	for subTreeSink := range cc {
		for leafSeq := range subTreeSink {
			select {
			case <- done:
				return
			default:
				leafBuffer <- leafSeq
			}
		}
	}
}

func bufferSubTreeChunks(ctx context.Context, subTree sequence, sink chan sequence, ec chan error, done chan interface{}) {
	defer close(sink)

	vrw := subTree.valueReadWriter()
	hs := refHashes(subTree)
	refStack, err := vrw.ReadManyValues(ctx, hs)
	if err != nil {
		ec <- err
	}


	for len(refStack) > 0 {
		select {
		case <- done:
			return
		default:
			// alternate fetching subtrees and sending
			// chunks until the subtree is exhausted
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
					refStack = append(newValues, refStack...)
				}
			}
		}
	}
}

func refHashes(s sequence) hash.HashSlice {
	hs := hash.NewHashSet()
	_ = s.WalkRefs(s.format(), func(ref Ref) error {
		hs.Insert(ref.TargetHash())
		return nil
	})
	return hs.HashSlice()
}

// leafBuffer + sum(readerBuffers) ~= requestedBufSize
func bufSize(requestedBufSize int32) int32 {
	s := requestedBufSize - (concurrency * subTreeBufSize(readAtTreeLevel))
	// ensure that leafBuffer can drain a readerBuffer
	if s < subTreeBufSize(readAtTreeLevel) {
		return subTreeBufSize(readAtTreeLevel)
	}
	return s
}

// calculate an approximate upper bound for the
// number of chunks in this subtree given that
// PTree nodes contain 64 children on average.
func subTreeBufSize(level int) int32 {
	const errMargin = 1.5
	size := 1
	for i := 0; i < level; i++ {
		size = size * 64
	}
	return int32(float64(size) * errMargin)
}
