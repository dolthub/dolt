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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package prolly

import (
	"fmt"
	"math"

	"github.com/kch42/buzhash"
	"github.com/zeebo/xxh3"
)

const (
	// The window size to use for computing the rolling hash. This is way more than necessary assuming random data
	// (two bytes would be sufficient with a target chunk size of 4k). The benefit of a larger window is it allows
	// for better distribution on input with lower entropy. At a target chunk size of 4k, any given byte changing
	// has roughly a 1.5% chance of affecting an existing boundary, which seems like an acceptable trade-off. The
	// choice of a prime number provides better distribution for repeating input.
	chunkWindow = uint32(67)

	minChunkSize = 1 << 9
	maxChunkSize = 1 << 14
)

// nodeSplitter decides where nodeItem streams should be split into chunks.
type nodeSplitter interface {
	// Append provides more nodeItems to the splitter. Splitter's make chunk
	// boundary decisions based on the nodeItem contents. Upon return, callers
	// can use CrossedBoundary() to see if a chunk boundary has crossed.
	Append(items ...nodeItem) error

	// CrossedBoundary returns true if the provided nodeItems have caused a chunk
	// boundary to be crossed.
	CrossedBoundary() bool

	// Reset resets the state of the splitter.
	Reset()
}

//  splitterFactory makes a nodeSplitter.
type splitterFactory func(itemSize uint32, level uint8) nodeSplitter

var defaultSplitterFactory splitterFactory = newSmoothRollingHasher

// dynamicNodeSplitter is a nodeSplitter designed to constrain the chunk size
// distribution by reducing the likelihood of forming very large or very small chunks.
// As the size of the current chunk grows, dynamicNodeSplitter changes the target
// pattern to make it easier to match. The result is a chunk size distribution
// that is closer to a binomial distribution, rather than geometric.
type dynamicNodeSplitter struct {
	bz     *buzhash.BuzHash
	offset uint32
	window uint32
	salt   byte

	crossedBoundary bool
}

var _ nodeSplitter = &dynamicNodeSplitter{}

func newSmoothRollingHasher(_ uint32, salt uint8) nodeSplitter {
	return &dynamicNodeSplitter{
		bz:     buzhash.NewBuzHash(chunkWindow),
		window: chunkWindow,
		salt:   byte(salt),
	}
}

var _ splitterFactory = newSmoothRollingHasher

// Append implements NodeSplitter
func (sns *dynamicNodeSplitter) Append(items ...nodeItem) (err error) {
	for _, it := range items {
		for _, byt := range it {
			_ = sns.hashByte(byt)
		}
	}
	return nil
}

func (sns *dynamicNodeSplitter) hashByte(b byte) bool {
	sns.offset++

	if sns.crossedBoundary {
		return true
	}

	sns.bz.HashByte(b ^ sns.salt)

	if sns.offset < minChunkSize {
		return true
	}
	if sns.offset > maxChunkSize {
		sns.crossedBoundary = true
		return true
	}

	hash := sns.bz.Sum32()
	patt := patternFromOffset(sns.offset)
	sns.crossedBoundary = hash&patt == patt

	return sns.crossedBoundary
}

// CrossedBoundary implements NodeSplitter
func (sns *dynamicNodeSplitter) CrossedBoundary() bool {
	return sns.crossedBoundary
}

// Reset implements NodeSplitter
func (sns *dynamicNodeSplitter) Reset() {
	sns.crossedBoundary = false
	sns.offset = 0
	sns.bz = buzhash.NewBuzHash(sns.window)
}

func patternFromOffset(offset uint32) uint32 {
	shift := 15 - (offset >> 10)
	return 1<<shift - 1
}

type keySplitter struct {
	count           uint32
	crossedBoundary bool

	// the following are const
	min, max uint32
	hi, lo   uint32
	salt     uint32
}

func newKeySplitter(rowSize uint32, level uint8) nodeSplitter {
	// todo(andy): thread this param
	rowSize = 24
	return &keySplitter{
		// todo(andy) roundLog2 creates discontinuities
		min:  minChunkSize / rowSize,
		max:  maxChunkSize / rowSize,
		hi:   uint32(16 - roundLog2(rowSize)),
		lo:   uint32(10 - roundLog2(rowSize)),
		salt: xxHash32([]byte{level}, 0),
	}
}

var _ splitterFactory = newKeySplitter

func (ks *keySplitter) Append(items ...nodeItem) error {
	if len(items) != 2 {
		return fmt.Errorf("expected 2 nodeItems, %d were passed", len(items))
	}

	ks.count++
	if ks.count < ks.min {
		return nil
	}
	if ks.count > ks.max {
		ks.crossedBoundary = true
		return nil
	}

	p := ks.patternFromCount(ks.count)
	h := xxHash32(items[0], ks.salt)
	ks.crossedBoundary = h&p == p
	return nil
}

func (ks *keySplitter) CrossedBoundary() bool {
	return ks.crossedBoundary
}

func (ks *keySplitter) Reset() {
	ks.count = 0
	ks.crossedBoundary = false
}

func (ks *keySplitter) patternFromCount(count uint32) uint32 {
	shift := ks.hi - (count >> ks.lo)
	return 1<<shift - 1
}

func roundLog2(sz uint32) int {
	// invariant: |sz| > 1
	lg2 := math.Log2(float64(sz))
	return int(math.Round(lg2))
}

func xxHash32(b []byte, salt uint32) uint32 {
	return uint32(xxh3.Hash(b)) ^ salt
}
