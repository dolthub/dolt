package ase

type SortedEditItr struct {
	leftItr  *KVPCollItr
	rightItr *KVPCollItr
	done     bool
}

func NewSortedEditItr(left, right *KVPCollection) *SortedEditItr {
	leftItr := NewItr(left)
	rightItr := NewItr(right)

	return &SortedEditItr{leftItr, rightItr, false}
}

func (itr *SortedEditItr) Next() *KVP {
	if itr.done {
		return nil
	}

	lesser := itr.rightItr
	if itr.leftItr.Less(itr.rightItr) {
		lesser = itr.leftItr
	}

	kvp := lesser.Next()

	itr.done = kvp == nil
	return kvp
}

func (itr *SortedEditItr) Size() int {
	return itr.leftItr.coll.totalSize + itr.rightItr.coll.totalSize
}
