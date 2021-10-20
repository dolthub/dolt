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
	smoothMinChunkSize = 1 << 9
	smoothMaxChunkSize = 1 << 14
)

// *smoothNodeSplitter is a nodeSplitter designed to constrain the output
// chunk size distribution. *smoothNodeSplitter matches against different patterns
// depending on the size of the current chunk being hashed. The larger the current
// chunk, the easier the pattern gets. The result is a chunk size distribution
// that is closer to a binomial distribution, rather than geometric.
type smoothNodeSplitter struct {
	bz     *buzhash.BuzHash
	offset uint32
	window uint32
	salt   byte

	patterns        []patternRange
	crossedBoundary bool
}

var _ nodeSplitter = &smoothNodeSplitter{}

type patternRange [2]uint32 // { rangeEnd, pattern }

const (
	rangeEnd = 0
	pattern  = 1
)

var smoothPatterns = []patternRange{
	// min = 512
	{1024, 1<<16 - 1},
	{2048, 1<<14 - 1},
	{4096, 1<<12 - 1},
	{8192, 1<<10 - 1},
	{16384, 1<<8 - 1},
	// max = 16384
}

func newSmoothRollingHasher(salt byte) *smoothNodeSplitter {
	return &smoothNodeSplitter{
		bz:       buzhash.NewBuzHash(chunkWindow),
		window:   chunkWindow,
		salt:     salt,
		patterns: smoothPatterns,
	}
}

func (sns *smoothNodeSplitter) Append(item nodeItem) (err error) {
	for _, byt := range item {
		_ = sns.hashByte(byt)
	}
	return nil
}

func (sns *smoothNodeSplitter) hashByte(b byte) bool {
	sns.offset++

	if !sns.crossedBoundary {
		sns.bz.HashByte(b ^ sns.salt)

		if sns.offset < smoothMinChunkSize {
			return sns.crossedBoundary
		}
		if sns.offset > smoothMaxChunkSize {
			sns.crossedBoundary = true
			return sns.crossedBoundary
		}

		hash := sns.bz.Sum32()
		for _, rp := range sns.patterns {
			if sns.offset < rp[rangeEnd] {
				pat := rp[pattern]
				sns.crossedBoundary = hash&pat == pat
				return sns.crossedBoundary
			}
		}
	}

	return sns.crossedBoundary
}

func (sns *smoothNodeSplitter) CrossedBoundary() bool {
	return sns.crossedBoundary
}

func (sns *smoothNodeSplitter) Reset() {
	sns.crossedBoundary = false
	sns.offset = 0
	sns.bz = buzhash.NewBuzHash(sns.window)
}
