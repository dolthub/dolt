package types

// listIterator allows iterating over a List from a given index.
type listIterator interface {
	next() (f Future, done bool)
}

func newListIterator(l List) listIterator {
	switch l := l.(type) {
	case listLeaf:
		return &listLeafIterator{l, 0}
	}
	panic("Unreachable")
}

func newListIteratorAt(l List, idx uint64) listIterator {
	switch l := l.(type) {
	case listLeaf:
		return &listLeafIterator{l, idx}
	}
	panic("Unreachable")
}

// listLeafIterator implements listIterator
type listLeafIterator struct {
	list listLeaf
	i    uint64
}

func (it *listLeafIterator) next() (f Future, done bool) {
	if it.i >= it.list.Len() {
		done = true
		return
	}
	f = it.list.getFuture(it.i)
	done = false
	it.i++
	return
}
