// Copyright 2021 Dolthub, Inc.
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

package fish

import "sync"

const (
	// 10 buckets ranging from 64 bytes to 32K
	numBufPools = uint64(10)
	lastBufPool = uint64(9)

	minimumSize = uint64(1 << 6)             // 64 bytes
	maximumSize = minimumSize << lastBufPool // 32K bytes
)

func NewLadder() (l Ladder) {
	l = Ladder{levels: make([]*sync.Pool, numBufPools)}
	for i := range l.levels {
		bufSz := minimumSize << i

		l.levels[i] = &sync.Pool{
			New: func() interface{} {
				return make([]byte, bufSz)
			},
		}
	}
	return
}

type Ladder struct {
	levels []*sync.Pool
}

func (l Ladder) Get(requested uint64) []byte {
	if requested > maximumSize {
		return make([]byte, requested)
	}

	i := findLargerBucket(requested)
	return l.levels[i].Get().([]byte)
}

func (l Ladder) Put(buf []byte) {
	sz := uint64(len(buf))
	if sz < minimumSize {
		return // discard buf
	}

	i := findSmallerBucket(sz)
	l.levels[i].Put(buf)
}

func findLargerBucket(sz uint64) uint64 {
	idx := uint64(0)
	bucketSz := minimumSize
	for sz > bucketSz {
		bucketSz <<= 1
		idx++
	}
	if idx > lastBufPool {
		panic("cannot find larger bucket")
	}
	return idx
}

func findSmallerBucket(sz uint64) uint64 {
	if sz < minimumSize {
		panic("cannot find smaller bucket")
	}

	idx := lastBufPool
	bucketSz := minimumSize << lastBufPool
	for sz < bucketSz {
		bucketSz >>= 1
		idx--
	}
	return idx
}
