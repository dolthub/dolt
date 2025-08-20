// Copyright 2019 Dolthub, Inc.
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

import (
	"context"

	"github.com/dolthub/dolt/go/store/types"
)

// KVPCollection is a collection of sorted KVPs
type KVPCollection struct {
	vr        types.ValueReader
	slices    []types.KVPSlice
	buffSize  int
	numSlices int
	totalSize int64
}

// NewKVPCollection creates a new KVPCollection from a sorted KVPSlice
func NewKVPCollection(vr types.ValueReader, sl types.KVPSlice) *KVPCollection {
	return newKVPColl(vr, cap(sl), 1, int64(len(sl)), []types.KVPSlice{sl})
}

func newKVPColl(vr types.ValueReader, maxSize, numSlices int, totalSize int64, slices []types.KVPSlice) *KVPCollection {
	if slices == nil {
		panic("invalid params")
	}

	return &KVPCollection{vr, slices, maxSize, numSlices, totalSize}
}

// Size returns the total number of elements in the collection
func (coll *KVPCollection) Size() int64 {
	return coll.totalSize
}

// Iterator returns an iterator that will iterate over the KVPs in the collection in order.
func (coll *KVPCollection) Iterator() *KVPCollItr {
	return NewItr(coll.vr, coll)
}

// DestructiveMerge merges two KVPCollections into a new collection.  This KVPCollection and the
// collection it is being merged with will no longer be valid once this method is called.  A
// new KVPCollection will be returned which holds the merged collections.
func (left *KVPCollection) DestructiveMerge(ctx context.Context, right *KVPCollection) (*KVPCollection, error) {
	if left.buffSize != right.buffSize {
		panic("Cannot merge collections with varying buffer sizes.")
	}

	lItr := left.Iterator()
	rItr := right.Iterator()
	resBuilder := NewKVPCollBuilder(left.vr, left.buffSize)

	var done bool
	var kvp *types.KVP
	var exhaustedBuff types.KVPSlice
	var currItr *KVPCollItr
	var otherItr *KVPCollItr

	for !done {
		currItr, otherItr = lItr, rItr
		isLess, err := rItr.Less(ctx, lItr)

		if err != nil {
			return nil, err
		}

		if isLess {
			currItr, otherItr = rItr, lItr
		}

		kvp, exhaustedBuff, done = currItr.nextForDestructiveMerge()
		resBuilder.AddKVP(*kvp)

		if exhaustedBuff != nil {
			resBuilder.AddBuffer(exhaustedBuff)
		}
	}

	resBuilder.MoveRemaining(otherItr)
	return resBuilder.Build(), nil
}
