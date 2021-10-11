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
	chunkWindow  = uint32(67)
	maxChunkSize = 1 << 24 // TODO: Remove when https://github.com/attic-labs/noms/issues/3743 is fixed.
)

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

var _ chunker = &rollingValueHasher{}

func (rv *rollingValueHasher) Write(cb func(w *binaryNomsWriter) error) (err error) {
	err = cb(&rv.bw)
	if err == nil {
		rv.sl.Update(rv.bw.data())
	}
	return
}

func (rv *rollingValueHasher) HashByte(b byte) bool {
	return rv.hashByte(b, rv.bw.offset)
}

func (rv *rollingValueHasher) hashByte(b byte, offset uint32) bool {
	if !rv.crossedBoundary {
		rv.bz.HashByte(b ^ rv.salt)
		rv.crossedBoundary = (rv.bz.Sum32()&rv.pattern == rv.pattern)
		if offset > maxChunkSize {
			rv.crossedBoundary = true
		}
	}
	return rv.crossedBoundary
}

func (rv *rollingValueHasher) Nbf() *NomsBinFormat {
	return rv.nbf
}

func (rv *rollingValueHasher) CrossedBoundary() bool {
	return rv.crossedBoundary
}

func (rv *rollingValueHasher) Reset() {
	rv.crossedBoundary = false
	rv.bz = buzhash.NewBuzHash(rv.window)
	rv.bw.reset()
	rv.sl.Reset()
}

// rollingByteHasher is a chunker for Blobs
type rollingByteHasher struct {
	bw              binaryNomsWriter
	idx             uint32
	bz              *buzhash.BuzHash
	crossedBoundary bool
	pattern, window uint32
	salt            byte
	nbf             *NomsBinFormat
}

func newRollingByteHasher(nbf *NomsBinFormat, salt byte) *rollingByteHasher {
	pattern, window := chunkingConfig()
	w := newBinaryNomsWriter()

	rb := &rollingByteHasher{
		bw:      w,
		bz:      buzhash.NewBuzHash(window),
		pattern: pattern,
		window:  window,
		salt:    salt,
		nbf:     nbf,
	}

	return rb
}

var _ chunker = &rollingByteHasher{}

func (bh *rollingByteHasher) Write(cb func(w *binaryNomsWriter) error) (err error) {
	err = cb(&bh.bw)
	if err != nil {
		return err
	}

	for ; bh.idx < bh.bw.offset; bh.idx++ {
		bh.hashByte(bh.bw.buff[bh.idx], bh.bw.offset)
	}

	return
}

func (bh *rollingByteHasher) hashByte(b byte, offset uint32) bool {
	if !bh.crossedBoundary {
		bh.bz.HashByte(b ^ bh.salt)
		bh.crossedBoundary = (bh.bz.Sum32()&bh.pattern == bh.pattern)
		if offset > maxChunkSize {
			bh.crossedBoundary = true
		}
	}
	return bh.crossedBoundary
}

func (bh *rollingByteHasher) Nbf() *NomsBinFormat {
	return bh.nbf
}

func (bh *rollingByteHasher) CrossedBoundary() bool {
	return bh.crossedBoundary
}

func (bh *rollingByteHasher) Reset() {
	bh.crossedBoundary = false
	bh.bz = buzhash.NewBuzHash(bh.window)

	bh.bw.reset()
	bh.idx = 0
}
