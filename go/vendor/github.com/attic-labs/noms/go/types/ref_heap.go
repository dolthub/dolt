// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sort"

	"github.com/attic-labs/noms/go/hash"
)

// RefByHeight implements sort.Interface to order by increasing HeightOrder(). It uses increasing order because this causes repeated pushes and pops of the 'tallest' Refs to re-use memory, avoiding reallocations.
// We might consider making this a firmer abstraction boundary as a part of BUG 2182
type RefByHeight []Ref

func (h RefByHeight) Len() int {
	return len(h)
}

func (h RefByHeight) Less(i, j int) bool {
	return !HeightOrder(h[i], h[j])
}

func (h RefByHeight) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *RefByHeight) PushBack(r Ref) {
	*h = append(*h, r)
}

func (h *RefByHeight) PopBack() Ref {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// DropIndices takes a slice of integer indices into h and splices out the Refs at those indices.
func (h *RefByHeight) DropIndices(indices []int) {
	sort.Ints(indices)
	old := *h
	numIdx := len(indices)
	for i, j := 0, 0; i < old.Len(); i++ {
		if len(indices) > 0 && i == indices[0] {
			indices = indices[1:]
			continue
		}
		if i != j {
			old[j] = old[i]
		}
		j++
	}
	*h = old[:old.Len()-numIdx]
}

func (h *RefByHeight) Unique() {
	seen := hash.HashSet{}
	result := make(RefByHeight, 0, cap(*h))
	for _, r := range *h {
		target := r.TargetHash()
		if !seen.Has(target) {
			result = append(result, r)
		}
		seen.Insert(target)
	}
	*h = result
}

// PopRefsOfHeight pops off and returns all refs r in h for which r.Height() == height.
func (h *RefByHeight) PopRefsOfHeight(height uint64) (refs RefSlice) {
	for h.MaxHeight() == height {
		r := h.PopBack()
		refs = append(refs, r)
	}
	return
}

// MaxHeight returns the height of the 'tallest' Ref in h.
func (h RefByHeight) MaxHeight() uint64 {
	if h.Empty() {
		return 0
	}
	return h.PeekEnd().Height()
}

func (h RefByHeight) Empty() bool {
	return h.Len() == 0
}

// PeekEnd returns, but does not Pop the tallest Ref in h.
func (h RefByHeight) PeekEnd() (head Ref) {
	return h.PeekAt(h.Len() - 1)
}

// PeekAt returns, but does not remove, the Ref at h[idx]. If the index is out of range, returns the empty Ref.
func (h RefByHeight) PeekAt(idx int) (peek Ref) {
	if idx >= 0 && idx < h.Len() {
		peek = h[idx]
	}
	return
}

// HeightOrder returns true if a is 'higher than' b, generally if its ref-height is greater. If the two are of the same height, fall back to sorting by TargetHash.
func HeightOrder(a, b Ref) bool {
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
