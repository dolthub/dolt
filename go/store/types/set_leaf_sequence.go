// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "sort"

type setLeafSequence struct {
	leafSequence
}

func newSetLeafSequence(vrw ValueReadWriter, vs ...Value) (orderedSequence, error) {
	seq, err := newLeafSequenceFromValues(SetKind, vrw, vs...)

	if err != nil {
		return nil, err
	}

	return setLeafSequence{seq}, nil
}

func (sl setLeafSequence) getCompareFn(other sequence) compareFn {
	return sl.getCompareFnHelper(other.(setLeafSequence).leafSequence)
}

// orderedSequence interface

func (sl setLeafSequence) getKey(idx int) (orderedKey, error) {
	item, err := sl.getItem(idx)

	if err != nil {
		return orderedKey{}, err
	}

	return newOrderedKey(item.(Value), sl.format())
}

func (sl setLeafSequence) search(key orderedKey) (int, error) {
	var err error
	n := sort.Search(int(sl.Len()), func(i int) bool {
		if err != nil {
			return false
		}

		var k orderedKey
		k, err = sl.getKey(i)

		return err == nil && !k.Less(sl.format(), key)
	})

	if err != nil {
		return 0, err
	}

	return n, nil
}
