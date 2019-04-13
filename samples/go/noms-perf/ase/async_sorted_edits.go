package ase

import (
	"github.com/attic-labs/noms/go/types"
	"sort"
)

func sorter(in, out chan KVPSlice) {
	for kvps := range in {
		sort.Stable(kvps)
		out <- kvps
	}
}

func merger(in chan [2]*KVPCollection, out chan *KVPCollection) {
	for {
		colls, ok := <-in

		if !ok {
			return
		}

		var res *KVPCollection
		if colls[1] == nil {
			res = colls[0]
		} else {
			res = colls[0].DestructiveMerge(colls[1])
		}

		out <- res
	}
}

type AsyncSortedEdits struct {
	sliceSize        int
	sortConcurrency  int
	asyncConcurrency int

	sortChan   chan KVPSlice
	resultChan chan KVPSlice
	doneChan   chan bool

	accumulating []KVP
	sortedColls  []*KVPCollection
}

func NewAsyncSortedEdits(sliceSize, asyncConCurrency, sortConcurrency int) *AsyncSortedEdits {
	sortChan := make(chan KVPSlice, asyncConCurrency*8)
	resChan := make(chan KVPSlice, asyncConCurrency*8)
	doneChan := make(chan bool, asyncConCurrency)

	for i := 0; i < asyncConCurrency; i++ {
		go func() {
			defer func() {
				doneChan <- true
			}()

			sorter(sortChan, resChan)
		}()
	}

	return &AsyncSortedEdits{
		sliceSize:        sliceSize,
		asyncConcurrency: asyncConCurrency,
		sortConcurrency:  sortConcurrency,
		sortChan:         sortChan,
		resultChan:       resChan,
		doneChan:         doneChan,
		accumulating:     make([]KVP, 0, sliceSize),
		sortedColls:      nil}
}

func (ase *AsyncSortedEdits) Set(k, v types.Value) {
	ase.accumulating = append(ase.accumulating, KVP{k, v})

	if len(ase.accumulating) == ase.sliceSize {
		ase.asyncSortAcc()
	}
}

func (ase *AsyncSortedEdits) asyncSortAcc() {
	ase.sortChan <- ase.accumulating
	ase.accumulating = make([]KVP, 0, ase.sliceSize)
	ase.pollSortedSlices()
}

func (ase *AsyncSortedEdits) pollSortedSlices() {
	for {
		select {
		case val := <-ase.resultChan:
			coll := NewKVPCollection(val)
			ase.sortedColls = append(ase.sortedColls, coll)

		default:
			return
		}
	}
}

func (ase *AsyncSortedEdits) FinishedEditing() {
	close(ase.sortChan)

	if len(ase.accumulating) > 0 {
		sl := KVPSlice(ase.accumulating)
		sort.Stable(sl)

		ase.resultChan <- sl
	}

	ase.wait()
}

func (ase *AsyncSortedEdits) wait() {
	running := ase.asyncConcurrency

	for {
		select {
		case val := <-ase.resultChan:
			coll := NewKVPCollection(val)
			ase.sortedColls = append(ase.sortedColls, coll)

		case <-ase.doneChan:
			running--

			if running == 0 {
				close(ase.resultChan)
				return
			}
		}
	}
}

func (ase *AsyncSortedEdits) Sort() {
	for len(ase.sortedColls) > 2 {
		pairs := pairCollections(ase.sortedColls)
		ase.sortedColls = nil

		numPairs := len(pairs)

		numGoRs := ase.sortConcurrency
		if numGoRs > numPairs {
			numGoRs = numPairs
		}

		sortChan := make(chan [2]*KVPCollection, numPairs)
		resChan := make(chan *KVPCollection, numPairs)
		for i := 0; i < numGoRs; i++ {
			go func() {
				defer func() {
					ase.doneChan <- true
				}()

				merger(sortChan, resChan)
			}()
		}

		for _, pair := range pairs {
			sortChan <- pair
		}

		close(sortChan)

		done := false
		for !done {
			select {
			case val := <-resChan:
				ase.sortedColls = append(ase.sortedColls, val)

			case <-ase.doneChan:
				numGoRs--

				if numGoRs == 0 {
					close(resChan)
					done = true
				}
			}
		}
	}
}

func pairCollections(colls []*KVPCollection) [][2]*KVPCollection {
	numColls := len(colls)
	pairs := make([][2]*KVPCollection, 0, numColls/2+1)
	sort.Slice(colls, func(i, j int) bool {
		return colls[i].Size() < colls[j].Size()
	})

	if numColls%2 == 1 {
		pairs = append(pairs, [2]*KVPCollection{colls[numColls-1], nil})

		colls = colls[:numColls-1]
		numColls -= 1
	}

	for i, j := 0, numColls-1; i < numColls/2; i, j = i+1, j-1 {
		pairs = append(pairs, [2]*KVPCollection{colls[i], colls[j]})
	}

	return pairs
}

func (ase *AsyncSortedEdits) Iterator() KVPIterator {
	switch len(ase.sortedColls) {
	case 1:
		return NewItr(ase.sortedColls[0])
	case 2:
		return NewSortedEditItr(ase.sortedColls[0], ase.sortedColls[1])
	}

	panic("Sort needs to be called prior to getting an Iterator.")
}
