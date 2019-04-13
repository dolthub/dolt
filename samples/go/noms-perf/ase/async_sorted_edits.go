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

func merger(in chan [2]KVPSlice, out chan KVPSlice) {
	for {
		slices, ok := <-in

		if !ok {
			return
		}

		res := slices[0].Merge(slices[1])
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
	sortedSlices [][]KVP
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
		sortedSlices:     nil}
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
			ase.sortedSlices = append(ase.sortedSlices, val)
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
			ase.sortedSlices = append(ase.sortedSlices, val)

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
	sortDir := true
	for len(ase.sortedSlices) > 2 {
		pairs := pairSlices(ase.sortedSlices)
		numPairs := len(pairs)

		numGoRs := ase.sortConcurrency
		if numGoRs > numPairs {
			numGoRs = numPairs
		}

		sortChan := make(chan [2]KVPSlice, numPairs)
		resChan := make(chan KVPSlice, numPairs)
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

		ase.sortedSlices = nil
		close(sortChan)

		done := false
		for !done {
			select {
			case val := <-resChan:
				ase.sortedSlices = append(ase.sortedSlices, val)

			case <-ase.doneChan:
				numGoRs--

				if numGoRs == 0 {
					close(resChan)
					done = true
				}
			}
		}

		sortDir = !sortDir
	}
}

func pairSlices(slices [][]KVP) [][2]KVPSlice {
	numSlices := len(slices)
	pairs := make([][2]KVPSlice, 0, numSlices/2+1)
	sort.Slice(slices, func(i, j int) bool {
		return len(slices[i]) < len(slices[j])
	})

	if numSlices%2 == 1 {
		x := slices[numSlices-1]
		y := slices[numSlices-2]
		z := slices[numSlices-3]

		middle := len(y) / 2

		pairs = append(pairs, [2]KVPSlice{x, y[:middle]})
		pairs = append(pairs, [2]KVPSlice{y[middle:], z})

		slices = slices[:numSlices-3]
		numSlices -= 3
	}

	for i, j := 0, numSlices-1; i < numSlices/2; i, j = i+1, j-1 {
		pairs = append(pairs, [2]KVPSlice{slices[i], slices[j]})
	}

	return pairs
}

func (ase *AsyncSortedEdits) Iterator() *SortedEditItr {
	var left KVPSlice
	var right KVPSlice

	if len(ase.sortedSlices) > 0 {
		left = ase.sortedSlices[0]
	}

	if len(ase.sortedSlices) > 1 {
		right = ase.sortedSlices[1]
	}

	if len(ase.sortedSlices) > 2 {
		panic("wtf")
	}

	return NewSortedEditItr(left, right)
}

func (ase *AsyncSortedEdits) PanicIfNotInOrder() {
	itr := ase.Iterator()

	prev := itr.Next()
	for {
		curr := itr.Next()

		if !prev.Key.Less(curr.Key) {
			panic("Not in order")
		}

		prev = curr
	}
}
