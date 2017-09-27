// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "sort"

type setLeafSequence struct {
	leafSequence
}

func newSetLeafSequence(vrw ValueReadWriter, vs ...Value) orderedSequence {
	return setLeafSequence{newLeafSequenceFromValues(SetKind, vrw, vs...)}
}

func (sl setLeafSequence) getCompareFn(other sequence) compareFn {
	return sl.getCompareFnHelper(other.(setLeafSequence).leafSequence)
}

// orderedSequence interface

func (sl setLeafSequence) getKey(idx int) orderedKey {
	return newOrderedKey(sl.getItem(idx).(Value))
}

func (sl setLeafSequence) search(key orderedKey) int {
	return sort.Search(int(sl.Len()), func(i int) bool {
		return !sl.getKey(i).Less(key)
	})
}
