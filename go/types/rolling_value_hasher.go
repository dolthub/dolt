// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"encoding/binary"
	"sync"

	"github.com/attic-labs/noms/go/hash"
	"github.com/kch42/buzhash"
)

const (
	defaultChunkPattern = uint32(1<<12 - 1) // Avg Chunk Size of 4k

	// The window size to use for computing the rolling hash. This is way more than necessary assuming random data (two bytes would be sufficient with a target chunk size of 4k). The benefit of a larger window is it allows for better distribution on input with lower entropy. At a target chunk size of 4k, any given byte changing has roughly a 1.5% chance of affecting an existing boundary, which seems like an acceptable trade-off.
	defaultChunkWindow = uint32(64)
)

// Only set by tests
var (
	chunkPattern  = defaultChunkPattern
	chunkWindow   = defaultChunkWindow
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
	chunkPattern = uint32(1<<7 - 1) // Avg Chunk Size of 128 bytes
	chunkWindow = uint32(32)
}

func normalProductionChunks() {
	chunkConfigMu.Lock()
	defer chunkConfigMu.Unlock()
	chunkPattern = defaultChunkPattern
	chunkWindow = defaultChunkWindow
}

type rollingValueHasher struct {
	bz                          *buzhash.BuzHash
	enc                         *valueEncoder
	bytesHashed                 uint32
	lengthOnly, crossedBoundary bool
	pattern, window             uint32
}

func hashValueBytes(item sequenceItem, rv *rollingValueHasher) {
	rv.HashValue(item.(Value))
}

func hashValueByte(item sequenceItem, rv *rollingValueHasher) {
	rv.HashByte(item.(byte))
}

func newRollingValueHasher() *rollingValueHasher {
	pattern, window := chunkingConfig()
	rv := &rollingValueHasher{
		bz:      buzhash.NewBuzHash(window),
		pattern: pattern,
		window:  window,
	}
	rv.enc = newValueEncoder(rv, nil)
	return rv
}

func (rv *rollingValueHasher) HashByte(b byte) {
	rv.bytesHashed++
	if rv.lengthOnly {
		return
	}

	rv.bz.HashByte(b)
	rv.crossedBoundary = rv.crossedBoundary || (rv.bz.Sum32()&chunkPattern == chunkPattern)
}

func (rv *rollingValueHasher) ClearLastBoundary() {
	rv.crossedBoundary = false
	rv.bytesHashed = 0
}

func (rv *rollingValueHasher) HashValue(v Value) {
	rv.enc.writeValue(v)
}

// nomsWriter interface. Note: It's unfortunate to have another implementation of nomsWriter and this one must be kept in sync with binaryNomsWriter, but hashing values is a red-hot code path and it's worth a lot to avoid the allocations for literally encoding values.
func (rv *rollingValueHasher) writeBytes(v []byte) {
	for _, b := range v {
		rv.HashByte(b)
	}
}

func (rv *rollingValueHasher) writeUint8(v uint8) {
	rv.HashByte(byte(v))
}

func (rv *rollingValueHasher) writeUint32(v uint32) {
	// Big-Endian
	rv.HashByte(byte(v >> 24))
	rv.HashByte(byte(v >> 16))
	rv.HashByte(byte(v >> 8))
	rv.HashByte(byte(v))
}

func (rv *rollingValueHasher) writeUint64(v uint64) {
	// Big-Endian
	rv.HashByte(byte(v >> 56))
	rv.HashByte(byte(v >> 48))
	rv.HashByte(byte(v >> 40))
	rv.HashByte(byte(v >> 32))
	rv.HashByte(byte(v >> 24))
	rv.HashByte(byte(v >> 16))
	rv.HashByte(byte(v >> 8))
	rv.HashByte(byte(v))
}

func (rv *rollingValueHasher) hashVarint(n int64) {
	buff := [binary.MaxVarintLen64]byte{}
	count := binary.PutVarint(buff[:], n)
	for i := 0; i < count; i++ {
		rv.HashByte(buff[i])
	}
}

func (rv *rollingValueHasher) writeNumber(v Number) {
	i, exp := float64ToIntExp(float64(v))
	rv.hashVarint(i)
	rv.hashVarint(int64(exp))
}

func (rv *rollingValueHasher) writeBool(v bool) {
	if v {
		rv.writeUint8(uint8(1))
	} else {
		rv.writeUint8(uint8(0))
	}
}

func (rv *rollingValueHasher) writeString(v string) {
	size := uint32(len(v))
	rv.writeUint32(size)

	for i := 0; i < len(v); i++ {
		rv.HashByte(v[i])
	}
}

func (rv *rollingValueHasher) writeHash(h hash.Hash) {
	for _, b := range h[:] {
		rv.HashByte(b)
	}
}

func (rv *rollingValueHasher) appendType(t *Type) {
	// Type bytes aren't included in the byte stream we chunk over
}
