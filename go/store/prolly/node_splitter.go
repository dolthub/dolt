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
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/kch42/buzhash"
	"github.com/zeebo/xxh3"
)

const (
	minChunkSize = 1 << 9
	maxChunkSize = 1 << 14
)

var levelSalt = [...]uint64{
	saltFromLevel(1),
	saltFromLevel(2),
	saltFromLevel(3),
	saltFromLevel(4),
	saltFromLevel(5),
	saltFromLevel(6),
	saltFromLevel(7),
	saltFromLevel(8),
	saltFromLevel(9),
	saltFromLevel(10),
	saltFromLevel(11),
	saltFromLevel(12),
	saltFromLevel(13),
	saltFromLevel(14),
	saltFromLevel(15),
}

//  splitterFactory makes a nodeSplitter.
type splitterFactory func(level uint8) nodeSplitter

var defaultSplitterFactory splitterFactory = newKeySplitter

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
// keySplitter uses a dynamic threshold modeled on a weibull distribution
// (https://en.wikipedia.org/wiki/Weibull_distribution). As the size of the current
// trunk increases, it becomes easier to pass the threshold, reducing the likelihood
// of forming very large or very small chunks.
type keySplitter struct {
	count, size     uint32
	crossedBoundary bool

	salt uint64
}

const (
	targetSize float64 = 4096
	maxUint32  float64 = math.MaxUint32

	// weibull params
	K  = 5.0
	B  = 2.0 / (3.0 * targetSize)
	KB = K * B
)

func newKeySplitter(level uint8) nodeSplitter {
	return &keySplitter{
		salt: levelSalt[level],
	}
}

var _ splitterFactory = newKeySplitter

func (ks *keySplitter) Append(items ...nodeItem) error {
	if len(items) != 2 {
		return fmt.Errorf("expected 2 nodeItems, %d were passed", len(items))
	}

	// todo(andy): account for key/value offsets, vtable, etc.
	ks.size += uint32(len(items[0]) + len(items[1]))
	ks.count++

	if ks.size < minChunkSize {
		return nil
	}
	if ks.size > maxChunkSize {
		ks.crossedBoundary = true
		return nil
	}

	t := ks.getThreshold()
	h := xxHash32(items[0], ks.salt)
	ks.crossedBoundary = h < t
	return nil
}

func (ks *keySplitter) CrossedBoundary() bool {
	return ks.crossedBoundary
}

func (ks *keySplitter) Reset() {
	ks.count, ks.size = 0, 0
	ks.crossedBoundary = false
}

// getThreshold computes the current probability threshold using the weibull distribution.
// see: https://en.wikipedia.org/wiki/Weibull_distribution#Alternative_parameterizations
func (ks *keySplitter) getThreshold() uint32 {
	avgSz := float64(ks.size) / float64(ks.count)
	x := float64(ks.size) / targetSize

	x4 := x * x * x * x // x ^ (K-1)
	x5 := x * x4        // x ^  K
	p := KB * x4 * math.Exp(-B*x5)
	return uint32(p * maxUint32 * avgSz)
}

func xxHash32(b []byte, salt uint64) uint32 {
	return uint32(xxh3.HashSeed(b, salt))
}

func saltFromLevel(level uint8) (salt uint64) {
	full := sha512.Sum512([]byte{level})
	return binary.LittleEndian.Uint64(full[:8])
}
