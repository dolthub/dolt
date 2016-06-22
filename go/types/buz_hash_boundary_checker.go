// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/kch42/buzhash"
)

type buzHashBoundaryChecker struct {
	h                     *buzhash.BuzHash
	windowSize, valueSize int
	pattern               uint32
	getBytes              getBytesFn
}

type getBytesFn func(item sequenceItem) []byte

func newBuzHashBoundaryChecker(windowSize, valueSize int, pattern uint32, getBytes getBytesFn) boundaryChecker {
	return &buzHashBoundaryChecker{buzhash.NewBuzHash(uint32(windowSize * valueSize)), windowSize, valueSize, pattern, getBytes}
}

func (b *buzHashBoundaryChecker) Write(item sequenceItem) bool {
	b.h.Write(b.getBytes(item))
	return b.h.Sum32()&b.pattern == b.pattern
}

func (b *buzHashBoundaryChecker) WindowSize() int {
	return b.windowSize
}
