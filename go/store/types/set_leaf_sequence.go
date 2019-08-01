// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

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
	n, err := SearchWithErroringLess(int(sl.Len()), func(i int) (b bool, e error) {
		k, err := sl.getKey(i)

		if err != nil {
			return false, err
		}

		isLess, err := k.Less(sl.format(), key)

		if err != nil {
			return false, err
		}

		return !isLess, nil
	})
	
	if err != nil {
		return 0, err
	}

	return n, nil
}
