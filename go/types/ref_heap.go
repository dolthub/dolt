// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

// RefHeap implements heap.Interface (which includes sort.Interface) as a height based priority queue.
type RefHeap []Ref

func (h RefHeap) Len() int {
	return len(h)
}

func (h RefHeap) Less(i, j int) bool {
	return HeapOrder(h[i], h[j])
}

func (h RefHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *RefHeap) Push(r interface{}) {
	*h = append(*h, r.(Ref))
}

func (h *RefHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h RefHeap) Empty() bool {
	return len(h) == 0
}

func (h RefHeap) Peek() (head Ref) {
	return h.PeekAt(0)
}

func (h RefHeap) PeekAt(idx int) (peek Ref) {
	if idx < len(h) {
		peek = h[idx]
	}
	return
}

// HeapOrder returns true if a is 'higher than' b, generally if its ref-height is greater. If the two are of the same height, fall back to sorting by TargetHash.
func HeapOrder(a, b Ref) bool {
	if a.Height() == b.Height() {
		return a.TargetHash().Less(b.TargetHash())
	}
	// > because we want the larger heights to be at the start of the queue.
	return a.Height() > b.Height()

}

// RefSlice implements sort.Interface to order by target ref.
type RefSlice []Ref

func (s RefSlice) Len() int {
	return len(s)
}

func (s RefSlice) Less(i, j int) bool {
	return s[i].TargetHash().Less(s[j].TargetHash())
}

func (s RefSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
