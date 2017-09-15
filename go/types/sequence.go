// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/hash"

type sequenceItem interface{}

type compareFn func(x int, y int) bool

type sequence interface {
	getItem(idx int) sequenceItem
	seqLen() int
	numLeaves() uint64
	valueReadWriter() ValueReadWriter
	WalkRefs(cb RefCallback)
	typeOf() *Type
	Kind() NomsKind
	getCompareFn(other sequence) compareFn
	getChildSequence(idx int) sequence
	treeLevel() uint64
	isLeaf() bool
	getCompositeChildSequence(start uint64, length uint64) sequence
	cumulativeNumberOfLeaves(idx int) uint64
	hash() hash.Hash
	writeTo(nomsWriter)
}

const (
	sequencePartKind   = 0
	sequencePartLevel  = 1
	sequencePartCount  = 2
	sequencePartValues = 3
)
