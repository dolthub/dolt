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
	"sync"

	"github.com/kch42/buzhash"
)

const (
	defaultChunkPattern = uint32(1<<12 - 1) // Avg Chunk Size of 4k

	// The window size to use for computing the rolling hash. This is way more than necessary assuming random data
	// (two bytes would be sufficient with a target chunk size of 4k). The benefit of a larger window is it allows
	// for better distribution on input with lower entropy. At a target chunk size of 4k, any given byte changing
	// has roughly a 1.5% chance of affecting an existing boundary, which seems like an acceptable trade-off. The
	// choice of a prime number provides better distribution for repeating input.
	chunkWindow  = uint32(67)
	maxChunkSize = 1 << 24
)

// nodeSplitter decides where sequences should be split into chunks.
type nodeSplitter interface {
	// Append provides more sequenceItems to the splitter. Callers pass a callback
	// function that uses |bw| to serialize sequenceItems. Splitter's make chunk
	// boundary decisions based on the contents of the byte buffer |bw.buff|. Upon
	// return, callers can use |CrossedBoundary| to see if a chunk boundary has crossed.
	Append(item nodeItem) error

	// CrossedBoundary returns true if the provided sequenceItems have caused a chunk
	// boundary to be crossed.
	CrossedBoundary() bool

	// Reset clears the current byte buffer and resets the state of the splitter.
	Reset()
}

type rollingHasher struct {
	bz              *buzhash.BuzHash
	offset          uint32
	salt            byte
	pattern         uint32
	window          uint32
	crossedBoundary bool
}

var _ nodeSplitter = &rollingHasher{}

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

func newDefaultNodeSplitter(salt byte) nodeSplitter {
	return newRollingHasher(salt)
	//return newSmoothRollingHasher(salt)
}

func newRollingHasher(salt byte) *rollingHasher {
	pattern, window := chunkingConfig()

	rv := &rollingHasher{
		bz:      buzhash.NewBuzHash(window),
		pattern: pattern,
		window:  window,
		salt:    salt,
	}

	return rv
}

func (rv *rollingHasher) Append(item nodeItem) (err error) {
	for _, byt := range item {
		_ = rv.hashByte(byt)
	}
	return nil
}

func (rv *rollingHasher) hashByte(b byte) bool {
	rv.offset++
	if !rv.crossedBoundary {
		rv.bz.HashByte(b ^ rv.salt)
		rv.crossedBoundary = (rv.bz.Sum32()&rv.pattern == rv.pattern)
		if rv.offset > maxChunkSize {
			rv.crossedBoundary = true
		}
	}
	return rv.crossedBoundary
}

func (rv *rollingHasher) CrossedBoundary() bool {
	return rv.crossedBoundary
}

func (rv *rollingHasher) Reset() {
	rv.crossedBoundary = false
	rv.offset = 0
	rv.bz = buzhash.NewBuzHash(rv.window)
}
