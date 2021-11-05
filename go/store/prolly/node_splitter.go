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

package prolly

import (
	"github.com/kch42/buzhash"
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

// nodeSplitter decides where sequences should be split into chunks.
type nodeSplitter interface {
	// Append provides more sequenceItems to the splitter. Callers pass a callback
	// function that uses |bw| to serialize sequenceItems. Splitter's make chunk
	// boundary decisions based on the contents of the byte buffer |bw.buff|. Upon
	// return, callers can use |CrossedBoundary| to see if a chunk boundary has crossed.
	Append(items ...nodeItem) error

	// CrossedBoundary returns true if the provided sequenceItems have caused a chunk
	// boundary to be crossed.
	CrossedBoundary() bool

	// Reset clears the currentPair byte buffer and resets the state of the splitter.
	Reset()
}

func newDefaultNodeSplitter(salt byte) nodeSplitter {
	return newSmoothRollingHasher(salt)
}

// dynamicNodeSplitter is a nodeSplitter designed to constrain the chunk size
// distribution by reducing the liklihood of forming very large or very small chunks.
// As the size of the current chunk grows, dynamicNodeSplitter changes the target
// pattern to make it easier to match. The result is a chunk size distribution
// that is closer to a binomial distribution, rather than geometric.
type dynamicNodeSplitter struct {
	bz     *buzhash.BuzHash
	offset uint32
	window uint32
	salt   byte

	patterns        []dynamicPattern
	crossedBoundary bool
}

var _ nodeSplitter = &dynamicNodeSplitter{}

type dynamicPattern [2]uint32

func (pr dynamicPattern) rangeEnd() uint32 {
	return pr[0]
}

func (pr dynamicPattern) pattern() uint32 {
	return pr[1]
}

var smoothPatterns = []dynamicPattern{
	// min size 512
	{1024, 1<<16 - 1},
	{2048, 1<<14 - 1},
	{4096, 1<<12 - 1},
	{8192, 1<<10 - 1},
	{maxChunkSize, 1<<8 - 1},
	// max size 16K
}

func newSmoothRollingHasher(salt byte) *dynamicNodeSplitter {
	return &dynamicNodeSplitter{
		bz:       buzhash.NewBuzHash(chunkWindow),
		window:   chunkWindow,
		salt:     salt,
		patterns: smoothPatterns,
	}
}

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

	if !sns.crossedBoundary {
		sns.bz.HashByte(b ^ sns.salt)

		if sns.offset < minChunkSize {
			return sns.crossedBoundary
		}
		if sns.offset > maxChunkSize {
			sns.crossedBoundary = true
			return sns.crossedBoundary
		}

		hash := sns.bz.Sum32()
		for _, rp := range sns.patterns {
			if sns.offset < rp.rangeEnd() {
				pat := rp.pattern()
				sns.crossedBoundary = hash&pat == pat
				return sns.crossedBoundary
			}
		}
	}

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
