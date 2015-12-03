package types

import (
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/attic-labs/buzhash"
	"github.com/attic-labs/noms/d"
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
	bytes := b.getBytes(item)
	d.Chk.Equal(b.valueSize, len(bytes))
	_, err := b.h.Write(bytes)
	d.Chk.NoError(err)
	return b.h.Sum32()&b.pattern == b.pattern
}

func (b *buzHashBoundaryChecker) WindowSize() int {
	return b.windowSize
}
