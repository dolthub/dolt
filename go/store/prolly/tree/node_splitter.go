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

package tree

import (
	"crypto/sha512"
	"encoding/binary"
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

// splitterFactory makes a nodeSplitter.
type splitterFactory func(level uint8) nodeSplitter

var defaultSplitterFactory splitterFactory = newKeySplitter

// nodeSplitter decides where Item streams should be split into chunks.
type nodeSplitter interface {
	// Append provides more nodeItems to the splitter. Splitter's make chunk
	// boundary decisions based on the Item contents. Upon return, callers
	// can use CrossedBoundary() to see if a chunk boundary has crossed.
	Append(key, values Item) error

	// CrossedBoundary returns true if the provided nodeItems have caused a chunk
	// boundary to be crossed.
	CrossedBoundary() bool

	// Reset resets the state of the splitter.
	Reset()
}

// rollingHashSplitter is a nodeSplitter that makes chunk boundary decisions using
// a rolling value hasher that processes Item pairs in a byte-wise fashion.
//
// rollingHashSplitter uses a dynamic hash pattern designed to constrain the chunk
// Size distribution by reducing the likelihood of forming very large or very small
// chunks. As the Size of the current chunk grows, rollingHashSplitter changes the
// target pattern to make it easier to match. The result is a chunk Size distribution
// that is closer to a binomial distribution, rather than geometric.
type rollingHashSplitter struct {
	bz     *buzhash.BuzHash
	offset uint32
	window uint32
	salt   byte

	crossedBoundary bool
}

const (
	// The window Size to use for computing the rolling hash. This is way more than necessary assuming random data
	// (two bytes would be sufficient with a target chunk Size of 4k). The benefit of a larger window is it allows
	// for better distribution on input with lower entropy. At a target chunk Size of 4k, any given byte changing
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
func (sns *rollingHashSplitter) Append(key, value Item) (err error) {
	for _, byt := range key {
		_ = sns.hashByte(byt)
	}
	for _, byt := range value {
		_ = sns.hashByte(byt)
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
// the key of an Item pair.
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

func newKeySplitter(level uint8) nodeSplitter {
	return &keySplitter{
		salt: levelSalt[level],
	}
}

var _ splitterFactory = newKeySplitter

func (ks *keySplitter) Append(key, value Item) error {
	thisSize := uint32(len(key) + len(value))
	ks.size += thisSize

	if ks.size < minChunkSize {
		return nil
	}
	if ks.size > maxChunkSize {
		ks.crossedBoundary = true
		return nil
	}

	h := xxHash32(key, ks.salt)
	ks.crossedBoundary = weibullCheck(ks.size, thisSize, h)
	return nil
}

func (ks *keySplitter) CrossedBoundary() bool {
	return ks.crossedBoundary
}

func (ks *keySplitter) Reset() {
	ks.size = 0
	ks.crossedBoundary = false
}

const (
	targetSize float64 = 4096
	maxUint32  float64 = math.MaxUint32

	// weibull params
	K = 4.

	// TODO: seems like this should be targetSize / math.Gamma(1 + 1/K).
	L = targetSize
)

// weibullCheck returns true if we should split
// at |hash| for a given record inserted into a
// chunk of size |size|, where the record's size
// is |thisSize|. |size| is the size of the chunk
// after the record is inserted, so includes
// |thisSize| in it.
//
// weibullCheck attempts to form chunks whose
// sizes match the weibull distribution.
//
// The logic is as follows: given that we haven't
// split on any of the records up to |size - thisSize|,
// the probability that we should split on this record
// is (CDF(end) - CDF(start)) / (1 - CDF(start)), or,
// the percentage of the remaining portion of the CDF
// that this record actually covers. We split is |hash|,
// treated as a uniform random number between [0,1),
// is less than this percentage.
func weibullCheck(size, thisSize, hash uint32) bool {
	startx := float64(size - thisSize)
	start := -math.Expm1(-math.Pow(startx/L, K))

	endx := float64(size)
	end := -math.Expm1(-math.Pow(endx/L, K))

	p := float64(hash) / maxUint32
	d := 1 - start
	if d <= 0 {
		return true
	}
	target := (end - start) / d
	return p < target
}

func xxHash32(b []byte, salt uint64) uint32 {
	return uint32(xxh3.HashSeed(b, salt))
}

func saltFromLevel(level uint8) (salt uint64) {
	full := sha512.Sum512([]byte{level})
	return binary.LittleEndian.Uint64(full[:8])
}
