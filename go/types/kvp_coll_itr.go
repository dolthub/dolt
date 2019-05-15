package types

// KVPCollItr is a KVPIterator implementation for iterating over a KVPCollection
type KVPCollItr struct {
	coll       *KVPCollection
	done       bool
	slIdx      int
	idx        int
	currSl     KVPSlice
	currSlSize int
	currKey    LesserValuable
}

// NewItr creates a new KVPCollItr from a KVPCollection
func NewItr(coll *KVPCollection) *KVPCollItr {
	firstSl := coll.slices[0]
	firstKey := firstSl[0].Key
	slSize := len(firstSl)

	return &KVPCollItr{coll, false, 0, 0, firstSl, slSize, firstKey}
}

// Less returns whether the current key this iterator is less than the current key for another iterator
func (itr *KVPCollItr) Less(other *KVPCollItr) bool {
	return other.currKey == nil || itr.currKey != nil && itr.currKey.Less(other.currKey)
}

// returns the next kvp, the slice it was read from when that slice is empty, and whether or not iteration is complete.
// when sliceIfExhausted returns a non-nil value it is assumed that the caller will take and use the buffer and that
// it's data is no longer valid.
func (itr *KVPCollItr) nextForDestructiveMerge() (nextKVP *KVP, sliceIfExhausted KVPSlice, itrDone bool) {
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
func (itr *KVPCollItr) Next() *KVP {
	kvp, _, _ := itr.nextForDestructiveMerge()
	return kvp
}

// Peek returns the next KVP without advancing
func (itr *KVPCollItr) Peek() *KVP {
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
