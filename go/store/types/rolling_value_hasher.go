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
	"sync"

	"github.com/dolthub/dolt/go/store/sloppy"

	"github.com/kch42/buzhash"
)

const (
	defaultChunkPattern = uint32(1<<12 - 1) // Avg Chunk Size of 4k

	// The window size to use for computing the rolling hash. This is way more than necessary assuming random data (two bytes would be sufficient with a target chunk size of 4k). The benefit of a larger window is it allows for better distribution on input with lower entropy. At a target chunk size of 4k, any given byte changing has roughly a 1.5% chance of affecting an existing boundary, which seems like an acceptable trade-off. The choice of a prime number provides better distribution for repeating input.
	chunkWindow = uint32(67)

	oldMaxChunkSize = 1 << 24

	minChunkSize = 1 << 10
	maxChunkSize = 1 << 13

	smoothMinChunkSize = 1 << 9
	smoothMaxChunkSize = 1 << 14
)

type signatureRange [2]uint32 // { endOfRange, pattern }

var smoothRanges = []signatureRange{
	// min = 512
	{1024, 1<<16 - 1},
	{2048, 1<<14 - 1},
	{4096, 1<<12 - 1},
	{8192, 1<<10 - 1},
	{16384, 1<<8 - 1},
	// max = 16384
}

var TestRewrite bool = false
var TestSmooth bool = false

// Only set by tests
var (
	chunkPattern  = defaultChunkPattern
	chunkConfigMu = &sync.Mutex{}
)

func chunkingConfig() (pattern, window uint32) {
	chunkConfigMu.Lock()
	defer chunkConfigMu.Unlock()
	return chunkPattern, chunkWindow
}

func smallTestChunks() {
	chunkConfigMu.Lock()
	defer chunkConfigMu.Unlock()
	chunkPattern = uint32(1<<8 - 1) // Avg Chunk Size of 256 bytes
}

func normalProductionChunks() {
	chunkConfigMu.Lock()
	defer chunkConfigMu.Unlock()
	chunkPattern = defaultChunkPattern
}

// TestWithSmallChunks allows testing with small chunks outside of pkg types.
func TestWithSmallChunks(cb func()) {
	smallTestChunks()
	defer normalProductionChunks()
	cb()
}

type rollingValueHasher struct {
	bw              binaryNomsWriter
	bz              *buzhash.BuzHash
	crossedBoundary bool
	pattern, window uint32
	salt            byte
	sl              *sloppy.Sloppy
	nbf             *NomsBinFormat
}

func hashValueBytes(item sequenceItem, rv *rollingValueHasher) error {
	return rv.HashValue(item.(Value))
}

func hashValueByte(item sequenceItem, rv *rollingValueHasher) error {
	rv.HashByte(item.(byte))

	return nil
}

func newRollingValueHasher(nbf *NomsBinFormat, salt byte) *rollingValueHasher {
	pattern, window := chunkingConfig()
	w := newBinaryNomsWriter()

	rv := &rollingValueHasher{
		bw:      w,
		bz:      buzhash.NewBuzHash(window),
		pattern: pattern,
		window:  window,
		salt:    salt,
		nbf:     nbf,
	}

	rv.sl = sloppy.New(rv.HashByte)

	return rv
}

func (rv *rollingValueHasher) HashByte(b byte) bool {
	return rv.hashByte(b, rv.bw.offset)
}

func (rv *rollingValueHasher) hashByte(b byte, offset uint32) bool {
	if !rv.crossedBoundary {
		rv.bz.HashByte(b ^ rv.salt)
		if TestRewrite {

			// chunk with smoothed probabilities
			if TestSmooth {
				s32 := rv.bz.Sum32()
				if offset > smoothMinChunkSize {
					for _, r := range smoothRanges {
						rangeEnd, pattern := r[0], r[1]
						if offset < rangeEnd {
							rv.crossedBoundary = s32&pattern == pattern
							return rv.crossedBoundary
						}
					}
				}
				if offset > smoothMaxChunkSize {
					rv.crossedBoundary = true
				}

				// chunk with min/max
			} else {
				if offset > minChunkSize {
					rv.crossedBoundary = (rv.bz.Sum32()&rv.pattern == rv.pattern)
				}
				if offset > maxChunkSize {
					rv.crossedBoundary = true
				}
			}

			// chunk with constant probability, no min/max
		} else {
			rv.crossedBoundary = (rv.bz.Sum32()&rv.pattern == rv.pattern)
			if offset > oldMaxChunkSize {
				rv.crossedBoundary = true
			}
		}
	}
	return rv.crossedBoundary
}

func (rv *rollingValueHasher) Reset() {
	rv.crossedBoundary = false
	rv.bz = buzhash.NewBuzHash(rv.window)
	rv.bw.reset()
	rv.sl.Reset()
}

func (rv *rollingValueHasher) HashValue(v Value) error {
	err := v.writeTo(&rv.bw, rv.nbf)

	if err != nil {
		return err
	}

	rv.sl.Update(rv.bw.data())

	return nil
}

func (rv *rollingValueHasher) hashBytes(buff []byte) {
	rv.bw.writeRaw(buff)
	rv.sl.Update(rv.bw.data())
}
