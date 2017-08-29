// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package merge

import (
	"fmt"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
)

func threeWayListMerge(a, b, parent types.List) (merged types.List, err error) {
	aSpliceChan, bSpliceChan := make(chan types.Splice), make(chan types.Splice)
	aStopChan, bStopChan := make(chan struct{}, 1), make(chan struct{}, 1)

	go func() {
		a.Diff(parent, aSpliceChan, aStopChan)
		close(aSpliceChan)
	}()
	go func() {
		b.Diff(parent, bSpliceChan, bStopChan)
		close(bSpliceChan)
	}()

	stopAndDrain := func(stop chan<- struct{}, drain <-chan types.Splice) {
		close(stop)
		for range drain {
		}
	}

	defer stopAndDrain(aStopChan, aSpliceChan)
	defer stopAndDrain(bStopChan, bSpliceChan)

	// The algorithm below relies on determining whether one splice "comes before" another, and whether the splices coming from the two diffs remove/add precisely the same elements. Unfortunately, the Golang zero-value for types.Splice (which is what gets read out of a/bSpliceChan when they're closed) is actaually a valid splice, albeit a meaningless one that indicates a no-op. It "comes before" any other splice, so having it in play really gums up the logic below. Rather than specifically checking for it all over the place, swap the zero-splice out for one full of SPLICE_UNASSIGNED, which is really the proper invalid splice value. That splice doesn't come before ANY valid splice, so the logic below can flow more clearly.
	zeroSplice := types.Splice{}
	zeroToInvalid := func(sp types.Splice) types.Splice {
		if sp == zeroSplice {
			return types.Splice{types.SPLICE_UNASSIGNED, types.SPLICE_UNASSIGNED, types.SPLICE_UNASSIGNED, types.SPLICE_UNASSIGNED}
		}
		return sp
	}
	invalidSplice := zeroToInvalid(types.Splice{})

	merged = parent
	offset := uint64(0)
	aSplice, bSplice := invalidSplice, invalidSplice
	for {
		// Get the next splice from both a and b. If either diff(a, parent) or diff(b, parent) is complete, aSplice or bSplice will get an invalid types.Splice. Generally, though, this allows us to proceed through both diffs in (index) order, considering the "current" splice from both diffs at the same time.
		if aSplice == invalidSplice {
			aSplice = zeroToInvalid(<-aSpliceChan)
		}
		if bSplice == invalidSplice {
			bSplice = zeroToInvalid(<-bSpliceChan)
		}
		// Both channels are producing zero values, so we're done.
		if aSplice == invalidSplice && bSplice == invalidSplice {
			break
		}
		if overlap(aSplice, bSplice) {
			if canMerge(a, b, aSplice, bSplice) {
				splice := merge(aSplice, bSplice)
				merged = apply(a, merged, offset, splice)
				offset += splice.SpAdded - splice.SpRemoved
				aSplice, bSplice = invalidSplice, invalidSplice
				continue
			}
			return parent, newMergeConflict("Overlapping splices: %s vs %s", describeSplice(aSplice), describeSplice(bSplice))
		}
		if aSplice.SpAt < bSplice.SpAt {
			merged = apply(a, merged, offset, aSplice)
			offset += aSplice.SpAdded - aSplice.SpRemoved
			aSplice = invalidSplice
			continue
		}
		merged = apply(b, merged, offset, bSplice)
		offset += bSplice.SpAdded - bSplice.SpRemoved
		bSplice = invalidSplice
	}

	return merged, nil
}

func overlap(s1, s2 types.Splice) bool {
	earlier, later := s1, s2
	if s2.SpAt < s1.SpAt {
		earlier, later = s2, s1
	}
	return s1.SpAt == s2.SpAt || earlier.SpAt+earlier.SpRemoved > later.SpAt
}

// canMerge returns whether aSplice and bSplice can be merged into a single splice that can be applied to parent. Currently, we're only willing to do this if the two splices do _precisely_ the same thing -- that is, remove the same number of elements from the same starting index and insert the exact same list of new elements.
func canMerge(a, b types.List, aSplice, bSplice types.Splice) bool {
	if aSplice != bSplice {
		return false
	}
	aIter, bIter := a.IteratorAt(aSplice.SpFrom), b.IteratorAt(bSplice.SpFrom)
	for count := uint64(0); count < aSplice.SpAdded; count++ {
		aVal, bVal := aIter.Next(), bIter.Next()
		if aVal == nil || bVal == nil || !aVal.Equals(bVal) {
			return false
		}
	}
	return true
}

// Since merge() is only called when canMerge() is true, we know s1 and s2 are exactly equal.
func merge(s1, s2 types.Splice) types.Splice {
	return s1
}

func apply(source, target types.List, offset uint64, s types.Splice) types.List {
	toAdd := make([]types.Valuable, s.SpAdded)
	iter := source.IteratorAt(s.SpFrom)
	for i := 0; uint64(i) < s.SpAdded; i++ {
		v := iter.Next()
		if v == nil {
			d.Panic("List diff returned a splice that inserts a nonexistent element.")
		}
		toAdd[i] = v
	}
	return target.Edit().Splice(s.SpAt+offset, s.SpRemoved, toAdd...).List()
}

func describeSplice(s types.Splice) string {
	return fmt.Sprintf("%d elements removed at %d; adding %d elements", s.SpRemoved, s.SpAt, s.SpAdded)
}
