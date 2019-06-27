// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "sort"

type setLeafSequence struct {
	leafSequence
}

func newSetLeafSequence(vrw ValueReadWriter, vs ...Value) orderedSequence {
	// TODO(binformat)
	return setLeafSequence{newLeafSequenceFromValues(SetKind, vrw, Format_7_18, vs...)}
}

func (sl setLeafSequence) getCompareFn(f *format, other sequence) compareFn {
	return sl.getCompareFnHelper(f, other.(setLeafSequence).leafSequence)
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
