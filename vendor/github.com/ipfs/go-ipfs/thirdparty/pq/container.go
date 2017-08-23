package pq

import "container/heap"

// PQ is a basic priority queue.
type PQ interface {
	// Push adds the ele
	Push(Elem)
	// Pop returns the highest priority Elem in PQ.
	Pop() Elem
	// Len returns the number of elements in the PQ.
	Len() int
	// Update `fixes` the PQ.
	Update(index int)

	// TODO  explain why this interface should not be extended
	// It does not support Remove. This is because...
}

// Elem describes elements that can be added to the PQ. Clients must implement
// this interface.
type Elem interface {
	// SetIndex stores the int index.
	SetIndex(int)
	// Index returns the last given by SetIndex(int).
	Index() int
}

// ElemComparator returns true if pri(a) > pri(b)
type ElemComparator func(a, b Elem) bool

// New creates a PQ with a client-supplied comparator.
func New(cmp ElemComparator) PQ {
	q := &wrapper{heapinterface{
		elems: make([]Elem, 0),
		cmp:   cmp,
	}}
	heap.Init(&q.heapinterface)
	return q
}

// wrapper exists because we cannot re-define Push. We want to expose
// Push(Elem) but heap.Interface requires Push(interface{})
type wrapper struct {
	heapinterface
}

var _ PQ = &wrapper{}

func (w *wrapper) Push(e Elem) {
	heap.Push(&w.heapinterface, e)
}

func (w *wrapper) Pop() Elem {
	return heap.Pop(&w.heapinterface).(Elem)
}

func (w *wrapper) Update(index int) {
	heap.Fix(&w.heapinterface, index)
}

// heapinterface handles dirty low-level details of managing the priority queue.
type heapinterface struct {
	elems []Elem
	cmp   ElemComparator
}

var _ heap.Interface = &heapinterface{}

// public interface

func (q *heapinterface) Len() int {
	return len(q.elems)
}

// Less delegates the decision to the comparator
func (q *heapinterface) Less(i, j int) bool {
	return q.cmp(q.elems[i], q.elems[j])
}

// Swap swaps the elements with indexes i and j.
func (q *heapinterface) Swap(i, j int) {
	q.elems[i], q.elems[j] = q.elems[j], q.elems[i]
	q.elems[i].SetIndex(i)
	q.elems[j].SetIndex(j)
}

// Note that Push and Pop in this interface are for package heap's
// implementation to call. To add and remove things from the heap, wrap with
// the pq struct to call heap.Push and heap.Pop.

func (q *heapinterface) Push(x interface{}) { // where to put the elem?
	t := x.(Elem)
	t.SetIndex(len(q.elems))
	q.elems = append(q.elems, t)
}

func (q *heapinterface) Pop() interface{} {
	old := q.elems
	n := len(old)
	elem := old[n-1]       // remove the last
	elem.SetIndex(-1)      // for safety // FIXME why?
	q.elems = old[0 : n-1] // shrink
	return elem
}
