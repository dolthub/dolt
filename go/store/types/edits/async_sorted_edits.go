// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package edits

import (
	"sort"

	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

func sorter(nbf *types.NomsBinFormat, in, out chan types.KVPSlice) {
	for kvps := range in {
		sort.Stable(types.KVPSort{kvps, nbf})
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

// AsyncSortedEdits is a data structure that can have edits added to it, and as they are added it will
// send them in batches to be sorted.  Once all edits have been added the batches of edits can then
// be merge sorted together.
type AsyncSortedEdits struct {
	sliceSize        int
	sortConcurrency  int
	asyncConcurrency int

	sortChan   chan types.KVPSlice
	resultChan chan types.KVPSlice
	doneChan   chan bool

	accumulating []types.KVP
	sortedColls  []*KVPCollection

	nbf *types.NomsBinFormat
}

// NewAsyncSortedEdits creates an AsyncSortedEdits object that creates batches of size 'sliceSize' and kicks off
// 'asyncConcurrency' go routines for background sorting of batches.  The final Sort call is processed with
// 'sortConcurrency' go routines
func NewAsyncSortedEdits(nbf *types.NomsBinFormat, sliceSize, asyncConcurrency, sortConcurrency int) *AsyncSortedEdits {
	sortChan := make(chan types.KVPSlice, asyncConcurrency*8)
	resChan := make(chan types.KVPSlice, asyncConcurrency*8)
	doneChan := make(chan bool, asyncConcurrency)

	for i := 0; i < asyncConcurrency; i++ {
		go func() {
			defer func() {
				doneChan <- true
			}()

			sorter(nbf, sortChan, resChan)
		}()
	}

	return &AsyncSortedEdits{
		sliceSize:        sliceSize,
		asyncConcurrency: asyncConcurrency,
		sortConcurrency:  sortConcurrency,
		sortChan:         sortChan,
		resultChan:       resChan,
		doneChan:         doneChan,
		accumulating:     make([]types.KVP, 0, sliceSize),
		sortedColls:      nil,
		nbf:              nbf,
	}
}

// AddEdit adds an edit
func (ase *AsyncSortedEdits) AddEdit(k types.LesserValuable, v types.Valuable) {
	ase.accumulating = append(ase.accumulating, types.KVP{k, v})

	if len(ase.accumulating) == ase.sliceSize {
		ase.asyncSortAcc()
	}
}

func (ase *AsyncSortedEdits) asyncSortAcc() {
	ase.sortChan <- ase.accumulating
	ase.accumulating = make([]types.KVP, 0, ase.sliceSize)
	ase.pollSortedSlices()
}

func (ase *AsyncSortedEdits) pollSortedSlices() {
	for {
		select {
		case val := <-ase.resultChan:
			coll := NewKVPCollection(ase.nbf, val)
			ase.sortedColls = append(ase.sortedColls, coll)

		default:
			return
		}
	}
}

// FinishedEditing should be called once all edits have been added.  Once FinishedEditing is called adding more edits
// will have undefined behavior.
func (ase *AsyncSortedEdits) FinishedEditing() types.EditProvider {
	close(ase.sortChan)

	if len(ase.accumulating) > 0 {
		sl := types.KVPSlice(ase.accumulating)
		sort.Stable(types.KVPSort{sl, ase.nbf})

		ase.resultChan <- sl
	}

	ase.wait()

	ase.Sort()

	return ase.Iterator()
}

func (ase *AsyncSortedEdits) wait() {
	running := ase.asyncConcurrency

	for running > 0 {
		select {
		case val := <-ase.resultChan:
			coll := NewKVPCollection(ase.nbf, val)
			ase.sortedColls = append(ase.sortedColls, coll)

		case <-ase.doneChan:
			running--
		}
	}

	for {
		select {
		case val := <-ase.resultChan:
			coll := NewKVPCollection(ase.nbf, val)
			ase.sortedColls = append(ase.sortedColls, coll)
		default:
			close(ase.resultChan)
			return
		}
	}
}

// Sort performs a concurrent merge sort.  Once this completes use the Iterator method for getting a KVPIterator
// which can be used to iterate over all the KVPs in order.
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

		for numGoRs > 0 {
			select {
			case val := <-resChan:
				ase.sortedColls = append(ase.sortedColls, val)

			case <-ase.doneChan:
				numGoRs--
			}
		}

		done := false
		for !done {
			select {
			case val := <-resChan:
				ase.sortedColls = append(ase.sortedColls, val)

			default:
				close(resChan)
				done = true
			}
		}
	}
}

// we pair collections so that as you perform many merges you end up with collections of edits that are similarly sized
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

// Iterator returns a KVPIterator instance that can iterate over all the KVPs in order.
func (ase *AsyncSortedEdits) Iterator() types.EditProvider {
	switch len(ase.sortedColls) {
	case 0:
		return types.EmptyEditProvider{}
	case 1:
		return NewItr(ase.nbf, ase.sortedColls[0])
	case 2:
		return NewSortedEditItr(ase.nbf, ase.sortedColls[0], ase.sortedColls[1])
	}

	panic("Sort needs to be called prior to getting an Iterator.")
}

// Size returns the number of edits
func (ase *AsyncSortedEdits) Size() int64 {
	size := int64(0)
	for _, coll := range ase.sortedColls {
		size += coll.Size()
	}

	return size
}
