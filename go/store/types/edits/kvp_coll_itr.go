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

package edits

import "github.com/liquidata-inc/dolt/go/store/types"

// KVPCollItr is a KVPIterator implementation for iterating over a KVPCollection
type KVPCollItr struct {
	coll       *KVPCollection
	done       bool
	slIdx      int
	idx        int
	currSl     types.KVPSlice
	currSlSize int
	currKey    types.LesserValuable
	nbf        *types.NomsBinFormat
}

// NewItr creates a new KVPCollItr from a KVPCollection
func NewItr(nbf *types.NomsBinFormat, coll *KVPCollection) *KVPCollItr {
	firstSl := coll.slices[0]
	firstKey := firstSl[0].Key
	slSize := len(firstSl)

	return &KVPCollItr{coll, false, 0, 0, firstSl, slSize, firstKey, nbf}
}

// Less returns whether the current key this iterator is less than the current key for another iterator
func (itr *KVPCollItr) Less(other *KVPCollItr) (bool, error) {
	if other.currKey == nil {
		return true, nil
	}

	if itr.currKey == nil {
		return false, nil
	}

	return itr.currKey.Less(itr.nbf, other.currKey)
}

// returns the next kvp, the slice it was read from when that slice is empty, and whether or not iteration is complete.
// when sliceIfExhausted returns a non-nil value it is assumed that the caller will take and use the buffer and that
// it's data is no longer valid.
func (itr *KVPCollItr) nextForDestructiveMerge() (nextKVP *types.KVP, sliceIfExhausted types.KVPSlice, itrDone bool) {
	if itr.done {
		return nil, nil, true
	}

	kvp := &itr.currSl[itr.idx]
	itr.idx++

	if itr.idx == itr.currSlSize {
		exhausted := itr.currSl

		itr.idx = 0
		itr.slIdx++

		if itr.slIdx < itr.coll.numSlices {
			itr.currSl = itr.coll.slices[itr.slIdx]
			itr.currSlSize = len(itr.currSl)
			itr.currKey = itr.currSl[itr.idx].Key
		} else {
			itr.done = true
			itr.currKey = nil
		}

		return kvp, exhausted, itr.done
	}

	itr.currKey = itr.currSl[itr.idx].Key
	return kvp, nil, false
}

// Next returns the next KVP
func (itr *KVPCollItr) Next() (*types.KVP, error) {
	kvp, _, _ := itr.nextForDestructiveMerge()
	return kvp, nil
}

// NumEdits returns the number of KVPs representing the edits that this will iterate over
func (itr *KVPCollItr) NumEdits() int64 {
	return itr.coll.Size()
}

// Peek returns the next KVP without advancing
func (itr *KVPCollItr) Peek() *types.KVP {
	if itr.done {
		return nil
	}

	return &itr.currSl[itr.idx]
}

// Reset sets the iterator back to the beginning of the collection so it can be iterated over again.
func (itr *KVPCollItr) Reset() {
	itr.done = false
	itr.slIdx = 0
	itr.idx = 0
	itr.currSl = itr.coll.slices[0]
	itr.currSlSize = len(itr.currSl)
	itr.currKey = itr.currSl[0].Key
}
