package edits

import "github.com/attic-labs/noms/go/types"

// KVPCollection is a collection of sorted KVPs
type KVPCollection struct {
	buffSize  int
	numSlices int
	totalSize int64
	slices    []types.KVPSlice
}

// NewKVPCollection creates a new KVPCollection from a sorted KVPSlice
func NewKVPCollection(sl types.KVPSlice) *KVPCollection {
	return newKVPColl(cap(sl), 1, int64(len(sl)), []types.KVPSlice{sl})
}

func newKVPColl(maxSize, numSlices int, totalSize int64, slices []types.KVPSlice) *KVPCollection {
	if slices == nil {
		panic("invalid params")
	}

	return &KVPCollection{maxSize, numSlices, totalSize, slices}
}

// Size returns the total number of elements in the collection
func (coll *KVPCollection) Size() int64 {
	return coll.totalSize
}

// Iterator returns an iterator that will iterate over the KVPs in the collection in order.
func (coll *KVPCollection) Iterator() *KVPCollItr {
	return NewItr(coll)
}

// DestructiveMerge merges two KVPCollections into a new collection.  This KVPCollection and the
// collection it is being merged with will no longer be valid once this method is called.  A
// new KVPCollection will be returned which holds the merged collections.
func (left *KVPCollection) DestructiveMerge(right *KVPCollection) *KVPCollection {
	if left.buffSize != right.buffSize {
		panic("Cannot merge collections with varying buffer sizes.")
	}

	lItr := left.Iterator()
	rItr := right.Iterator()
	resBuilder := NewKVPCollBuilder(left.buffSize)

	var done bool
	var kvp *types.KVP
	var exhaustedBuff types.KVPSlice
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
