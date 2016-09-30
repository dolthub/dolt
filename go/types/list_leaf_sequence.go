// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

type listLeafSequence struct {
	leafSequence
	values []Value
}

func newListLeafSequence(vr ValueReader, v ...Value) sequence {
	ts := make([]*Type, len(v))
	for i, v := range v {
		ts[i] = v.Type()
	}
	t := MakeListType(MakeUnionType(ts...))
	return listLeafSequence{leafSequence{vr, len(v), t}, v}
}

// sequence interface

func (ll listLeafSequence) getCompareFn(other sequence) compareFn {
	oll := other.(listLeafSequence)
	return func(idx, otherIdx int) bool {
		return ll.values[idx].Equals(oll.values[otherIdx])
	}
}

func (ll listLeafSequence) getItem(idx int) sequenceItem {
	return ll.values[idx]
}

func (ll listLeafSequence) WalkRefs(cb RefCallback) {
	for _, v := range ll.values {
		v.WalkRefs(cb)
	}
}
