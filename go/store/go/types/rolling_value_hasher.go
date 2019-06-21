// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sync"

	"github.com/attic-labs/noms/go/sloppy"

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

type rollingValueHasher struct {
	bw              binaryNomsWriter
	bz              *buzhash.BuzHash
	crossedBoundary bool
	pattern, window uint32
	salt            byte
	sl              *sloppy.Sloppy
}

func hashValueBytes(item sequenceItem, rv *rollingValueHasher) {
	rv.HashValue(item.(Value))
}

func hashValueByte(item sequenceItem, rv *rollingValueHasher) {
	rv.HashByte(item.(byte))
}

func newRollingValueHasher(salt byte) *rollingValueHasher {
	pattern, window := chunkingConfig()
	w := newBinaryNomsWriter()

	rv := &rollingValueHasher{
		bw:      w,
		bz:      buzhash.NewBuzHash(window),
		pattern: pattern,
		window:  window,
		salt:    salt,
	}

	rv.sl = sloppy.New(rv.HashByte)

	return rv
}

func (rv *rollingValueHasher) HashByte(b byte) bool {
	if !rv.crossedBoundary {
		rv.bz.HashByte(b ^ rv.salt)
		rv.crossedBoundary = (rv.bz.Sum32()&rv.pattern == rv.pattern)
		if rv.bw.offset > maxChunkSize {
			rv.crossedBoundary = true
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

func (rv *rollingValueHasher) HashValue(v Value) {
	v.writeTo(&rv.bw)
	rv.sl.Update(rv.bw.data())
}

func (rv *rollingValueHasher) hashBytes(buff []byte) {
	rv.bw.writeRaw(buff)
	rv.sl.Update(rv.bw.data())
}
