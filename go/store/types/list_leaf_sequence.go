// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

type listLeafSequence struct {
	leafSequence
}

func newListLeafSequence(vrw ValueReadWriter, vs ...Value) sequence {
	return listLeafSequence{newLeafSequenceFromValues(ListKind, vrw, vs...)}
}

// sequence interface

func (ll listLeafSequence) getCompareFn(other sequence) compareFn {
	return ll.getCompareFnHelper(other.(listLeafSequence).leafSequence)
}
