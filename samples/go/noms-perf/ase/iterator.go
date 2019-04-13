package ase

import "github.com/attic-labs/noms/go/types"

type SortedEditItr struct {
	left  KVPSlice
	right KVPSlice
	lIdx  int
	rIdx  int
	lKey  types.Value
	rKey  types.Value
}

func NewSortedEditItr(left, right KVPSlice) *SortedEditItr {
	var lKey types.Value
	var rKey types.Value

	if left != nil {
		lKey = left[0].Key
	}

	if right != nil {
		rKey = right[0].Key
	}

	return &SortedEditItr{left, right, 0, 0, lKey, rKey}
}

func (itr *SortedEditItr) Next() *KVP {
	if itr.lKey == nil && itr.rKey == nil {
		return nil
	}

	if itr.rKey == nil || itr.lKey != nil && itr.lKey.Less(itr.rKey) {
		idx := itr.lIdx

		itr.lIdx++

		if itr.lIdx < len(itr.left) {
			itr.lKey = itr.left[itr.lIdx].Key
		} else {
			itr.lKey = nil
		}

		return &itr.left[idx]
	} else {
		idx := itr.rIdx

		itr.rIdx++

		if itr.rIdx < len(itr.right) {
			itr.rKey = itr.right[itr.rIdx].Key
		} else {
			itr.rKey = nil
		}

		return &itr.right[idx]
	}
}
