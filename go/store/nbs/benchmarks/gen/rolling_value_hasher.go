// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package gen

import "github.com/kch42/buzhash"

const (
	chunkPattern = uint32(1<<12 - 1) // Avg Chunk Size of 4k

	// The window size to use for computing the rolling hash. This is way more than necessary assuming random data (two bytes would be sufficient with a target chunk size of 4k). The benefit of a larger window is it allows for better distribution on input with lower entropy. At a target chunk size of 4k, any given byte changing has roughly a 1.5% chance of affecting an existing boundary, which seems like an acceptable trade-off.
	chunkWindow = uint32(64)
)

type rollingValueHasher struct {
	bz *buzhash.BuzHash
}

func newRollingValueHasher() *rollingValueHasher {
	return &rollingValueHasher{buzhash.NewBuzHash(chunkWindow)}
}

func (rv *rollingValueHasher) HashByte(b byte) bool {
	rv.bz.HashByte(b)
	return rv.bz.Sum32()&chunkPattern == chunkPattern
}
