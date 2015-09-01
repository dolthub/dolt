package types

// listIterator allows iterating over a List from a given index.
type listIterator interface {
	next() (f Future, done bool)
}

func newListIterator(l List) listIterator {
	switch l := l.(type) {
	case listLeaf:
		return &listLeafIterator{l, 0}
	case compoundList:
		return &compoundListIterator{l, newListIterator(l.futures[0].Deref(l.cs).(List)), 0}
	}
	panic("Unreachable")
}

func newListIteratorAt(l List, idx uint64) listIterator {
	switch l := l.(type) {
	case listLeaf:
		return &listLeafIterator{l, idx}
	case compoundList:
		si := findSubIndex(idx, l.offsets)
		if si > 0 {
			idx -= l.offsets[si-1]
		}
		return &compoundListIterator{l, newListIteratorAt(l.futures[si].Deref(l.cs).(List), idx), uint64(si)}
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

// compoundListIterator implements listIterator
type compoundListIterator struct {
	list compoundList
	it   listIterator
	si   uint64
}

func (it *compoundListIterator) next() (f Future, done bool) {
	f, done = it.it.next()
	if done && it.si < uint64(len(it.list.futures))-1 {
		it.si++
		it.it = newListIterator(it.list.futures[it.si].Deref(it.list.cs).(List))
		f, done = it.it.next()
	}
	return
}
