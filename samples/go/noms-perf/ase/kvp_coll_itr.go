package ase

import "github.com/attic-labs/noms/go/types"

type KVPCollItr struct {
	coll       *KVPCollection
	done       bool
	slIdx      int
	idx        int
	currSl     KVPSlice
	currSlSize int
	currKey    types.Value
}

func NewItr(coll *KVPCollection) *KVPCollItr {
	firstSl := coll.slices[0]
	firstKey := firstSl[0].Key
	slSize := len(firstSl)

	return &KVPCollItr{coll, false, 0, 0, firstSl, slSize, firstKey}
}

func (itr *KVPCollItr) Less(other *KVPCollItr) bool {
	return other.currKey == nil || itr.currKey != nil && itr.currKey.Less(other.currKey)
}

func (itr *KVPCollItr) nextForDestructiveMerge() (*KVP, KVPSlice, bool) {
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

func (itr *KVPCollItr) Next() *KVP {
	kvp, _, _ := itr.nextForDestructiveMerge()
	return kvp
}

func (itr *KVPCollItr) Reset() {
	itr.done = false
	itr.slIdx = 0
	itr.idx = 0
	itr.currSl = itr.coll.slices[0]
	itr.currSlSize = len(itr.currSl)
	itr.currKey = itr.currSl[0].Key
}
