package types

func newListIterator(l List) *listIterator {
	return &listIterator{l, 0}
}

func newListIteratorAt(l List, idx uint64) *listIterator {
	return &listIterator{l, idx}
}

type listIterator struct {
	list List
	i    uint64
}

func (it *listIterator) next() (v Value, done bool) {
	if it.i >= it.list.Len() {
		done = true
		return
	}
	v = it.list.values[it.i]
	done = false
	it.i++
	return
}
