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
	"math/bits"

	"github.com/kch42/buzhash"
	"github.com/zeebo/xxh3"
)

const (
	minChunkSize = 1 << 9
	maxChunkSize = 1 << 14
)

//  splitterFactory makes a nodeSplitter.
type splitterFactory func(level uint8) nodeSplitter

var defaultSplitterFactory splitterFactory = newRollingHashSplitter

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

// rollingHashSplitter is a nodeSplitter that makes chunk boundary decisions using
// a rolling value hasher that processes nodeItem pairs in a byte-wise fashion.
//
// rollingHashSplitter uses a dynamic hash pattern designed to constrain the chunk
// size distribution by reducing the likelihood of forming very large or very small
// chunks. As the size of the current chunk grows, rollingHashSplitter changes the
// target pattern to make it easier to match. The result is a chunk size distribution
// that is closer to a binomial distribution, rather than geometric.
type rollingHashSplitter struct {
	bz     *buzhash.BuzHash
	offset uint32
	window uint32
	salt   byte

	crossedBoundary bool
}

const (
	// The window size to use for computing the rolling hash. This is way more than necessary assuming random data
	// (two bytes would be sufficient with a target chunk size of 4k). The benefit of a larger window is it allows
	// for better distribution on input with lower entropy. At a target chunk size of 4k, any given byte changing
	// has roughly a 1.5% chance of affecting an existing boundary, which seems like an acceptable trade-off. The
	// choice of a prime number provides better distribution for repeating input.
	rollingHashWindow = uint32(67)
)

var _ nodeSplitter = &rollingHashSplitter{}

func newRollingHashSplitter(salt uint8) nodeSplitter {
	return &rollingHashSplitter{
		bz:     buzhash.NewBuzHash(rollingHashWindow),
		window: rollingHashWindow,
		salt:   byte(salt),
	}
}

var _ splitterFactory = newRollingHashSplitter

// Append implements NodeSplitter
func (sns *rollingHashSplitter) Append(items ...nodeItem) (err error) {
	for _, it := range items {
		for _, byt := range it {
			_ = sns.hashByte(byt)
		}
	}
	return nil
}

func (sns *rollingHashSplitter) hashByte(b byte) bool {
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
	patt := rollingHashPattern(sns.offset)
	sns.crossedBoundary = hash&patt == patt

	return sns.crossedBoundary
}

// CrossedBoundary implements NodeSplitter
func (sns *rollingHashSplitter) CrossedBoundary() bool {
	return sns.crossedBoundary
}

// Reset implements NodeSplitter
func (sns *rollingHashSplitter) Reset() {
	sns.crossedBoundary = false
	sns.offset = 0
	sns.bz = buzhash.NewBuzHash(sns.window)
}

func rollingHashPattern(offset uint32) uint32 {
	shift := 15 - (offset >> 10)
	return 1<<shift - 1
}

// keySplitter is a nodeSplitter that makes chunk boundary decisions on the hash of
// the key of a nodeItem pair. In contrast to the rollingHashSplitter, keySplitter
// tries to create chunks that have an average number of nodeItem pairs, rather than
// an average number of bytes. However, because the target number of nodeItem pairs
// is computed directly from the chunk size and count, the practical difference in
// the distribution of chunk sizes is minimal.
//
// keySplitter uses a dynamic hash pattern designed to constrain the chunk size
// distribution by reducing the likelihood of forming very large or very small chunks.
// As the size of the current chunk grows, keySplitter changes the target pattern to
// make it easier to match. The result is a chunk size distribution that is closer to
// a binomial distribution, rather than geometric.
type keySplitter struct {
	count, size     uint32
	crossedBoundary bool

	salt uint32
}

const (
	// log2MidPoint is 2^31.5
	log2MidPoint = 0b10110101000001001111001100110011
)

func newKeySplitter(level uint8) nodeSplitter {
	return &keySplitter{
		salt: xxHash32([]byte{level}, 0),
	}
}

var _ splitterFactory = newKeySplitter

func (ks *keySplitter) Append(items ...nodeItem) error {
	if len(items) != 2 {
		return fmt.Errorf("expected 2 nodeItems, %d were passed", len(items))
	}

	ks.size += uint32(len(items[0]) + len(items[1]))
	ks.count++

	if ks.size < minChunkSize {
		return nil
	}
	if ks.size > maxChunkSize {
		ks.crossedBoundary = true
		return nil
	}

	p := ks.getPattern()
	h := xxHash32(items[0], ks.salt)
	ks.crossedBoundary = h&p == p
	return nil
}

func (ks *keySplitter) CrossedBoundary() bool {
	return ks.crossedBoundary
}

func (ks *keySplitter) Reset() {
	ks.count, ks.size = 0, 0
	ks.crossedBoundary = false
}

// getPattern computes the target pattern for the keySplitter
// from its current state, taking into consideration the
// number of nodeItem pairs and their average size.
// The computed pattern becomes easier to match as the total
// size of the current node/chunk increases.
func (ks *keySplitter) getPattern() uint32 {
	avgSz := ks.size / ks.count
	hi := 16 - roundLog2(avgSz)
	lo := 10 - roundLog2(avgSz)
	shift := hi - (ks.count >> lo)
	return 1<<shift - 1
}

// roundLog2 is an optimized version of
// uint32(math.Round(math.Log2(sz)))
func roundLog2(sz uint32) (lg uint32) {
	// invariant: |sz| > 1
	lg = uint32(bits.Len32(sz) - 1)
	if sz > (log2MidPoint >> (31 - lg)) {
		lg++
	}
	return
}

func xxHash32(b []byte, salt uint32) uint32 {
	return uint32(xxh3.Hash(b)) ^ salt
}
