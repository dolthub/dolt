// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

type listLeafSequence struct {
	values []Value
	t      *Type
	vr     ValueReader
}

func newListLeafSequence(vr ValueReader, v ...Value) indexedSequence {
	ts := make([]*Type, len(v))
	for i, v := range v {
		ts[i] = v.Type()
	}
	t := MakeListType(MakeUnionType(ts...))
	return listLeafSequence{v, t, vr}
}

// indexedSequence interface
func (ll listLeafSequence) cumulativeNumberOfLeaves(idx int) uint64 {
	return uint64(idx) + 1
}

func (ll listLeafSequence) getCompareFn(other sequence) compareFn {
	oll := other.(listLeafSequence)
	return func(idx, otherIdx int) bool {
		return ll.values[idx].Equals(oll.values[otherIdx])
	}
}

// sequence interface
func (ll listLeafSequence) getItem(idx int) sequenceItem {
	return ll.values[idx]
}

func (ll listLeafSequence) seqLen() int {
	return len(ll.values)
}

func (ll listLeafSequence) numLeaves() uint64 {
	return uint64(len(ll.values))
}

func (ll listLeafSequence) valueReader() ValueReader {
	return ll.vr
}

func (ll listLeafSequence) Chunks() (chunks []Ref) {
	for _, v := range ll.values {
		chunks = append(chunks, v.Chunks()...)
	}
	return
}

func (ll listLeafSequence) Type() *Type {
	return ll.t
}
