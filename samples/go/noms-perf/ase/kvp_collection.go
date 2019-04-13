package ase

import (
	"github.com/attic-labs/noms/go/types"
)

type KVPCollection struct {
	buffSize  int
	numSlices int
	totalSize int
	slices    []KVPSlice
}

func NewKVPCollection(sl KVPSlice) *KVPCollection {
	return newKVPColl(cap(sl), 1, len(sl), []KVPSlice{sl})
}

func newKVPColl(maxSize, numSlices, totalSize int, slices []KVPSlice) *KVPCollection {
	if slices == nil {
		panic("invalid params")
	}

	return &KVPCollection{maxSize, numSlices, totalSize, slices}
}

func (coll *KVPCollection) String() string {
	itr := coll.Iterator()
	val := itr.Next()

	keys := make([]types.Value, coll.totalSize)
	for i := 0; val != nil; i++ {
		keys[i] = val.Key
		val = itr.Next()
	}

	tpl := types.NewTuple(keys...)
	return types.EncodedValue(tpl)
}

func (coll *KVPCollection) Size() int {
	return coll.totalSize
}

func (coll *KVPCollection) Iterator() *KVPCollItr {
	return NewItr(coll)
}

func (left *KVPCollection) DestructiveMerge(right *KVPCollection) *KVPCollection {
	if left.buffSize != right.buffSize {
		panic("Cannot merge collections with varying buffer sizes.")
	}

	lItr := left.Iterator()
	rItr := right.Iterator()
	resBuilder := NewKVPCollBuilder(left.buffSize)

	var done bool
	var kvp *KVP
	var exhaustedBuff KVPSlice
	var currItr *KVPCollItr
	var otherItr *KVPCollItr

	for !done {
		currItr, otherItr = rItr, lItr
		if lItr.Less(rItr) {
			currItr, otherItr = lItr, rItr
		}

		kvp, exhaustedBuff, done = currItr.nextForDestructiveMerge()
		resBuilder.AddKVP(*kvp)

		if exhaustedBuff != nil {
			resBuilder.AddBuffer(exhaustedBuff)
		}
	}

	resBuilder.MoveRemaining(otherItr)
	return resBuilder.Build()
}
