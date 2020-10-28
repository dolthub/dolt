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
	"context"
	"sort"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/dolthub/dolt/go/store/types"
)

// AsyncSortedEdits is a data structure that can have edits added to it, and as they are added it will
// send them in batches to be sorted.  Once all edits have been added the batches of edits can then
// be merge sorted together.
type AsyncSortedEdits struct {
	sliceSize       int
	sortConcurrency int
	closed          bool

	accumulating []types.KVP
	sortedColls  []*KVPCollection

	nbf *types.NomsBinFormat

	sortGroup *errgroup.Group
	sortCtx   context.Context
	sema      *semaphore.Weighted
}

// NewAsyncSortedEdits creates an AsyncSortedEdits object that creates batches of size 'sliceSize' and kicks off
// 'asyncConcurrency' go routines for background sorting of batches.  The final Sort call is processed with
// 'sortConcurrency' go routines
func NewAsyncSortedEdits(nbf *types.NomsBinFormat, sliceSize, asyncConcurrency, sortConcurrency int) *AsyncSortedEdits {
	group, groupCtx := errgroup.WithContext(context.TODO())
	return &AsyncSortedEdits{
		sliceSize:       sliceSize,
		sortConcurrency: sortConcurrency,
		accumulating:    make([]types.KVP, 0, sliceSize),
		sortedColls:     nil,
		nbf:             nbf,
		sortGroup:       group,
		sortCtx:         groupCtx,
		sema:            semaphore.NewWeighted(int64(asyncConcurrency)),
	}
}

// AddEdit adds an edit
func (ase *AsyncSortedEdits) AddEdit(k types.LesserValuable, v types.Valuable) {
	ase.accumulating = append(ase.accumulating, types.KVP{Key: k, Val: v})
	if len(ase.accumulating) == ase.sliceSize {
		coll := NewKVPCollection(ase.nbf, ase.accumulating)
		ase.sortedColls = append(ase.sortedColls, coll)
		toSort := ase.accumulating
		ase.accumulating = make([]types.KVP, 0, ase.sliceSize)
		if err := ase.sema.Acquire(ase.sortCtx, 1); err != nil {
			return
		}
		ase.sortGroup.Go(func() error {
			defer ase.sema.Release(1)
			return types.SortWithErroringLess(types.KVPSort{Values: toSort, NBF: ase.nbf})
		})
	}
}

// FinishedEditing should be called once all edits have been added. Once FinishedEditing is called adding more edits
// will have undefined behavior.
func (ase *AsyncSortedEdits) FinishedEditing() (types.EditProvider, error) {
	ase.closed = true

	if len(ase.accumulating) > 0 {
		err := types.SortWithErroringLess(types.KVPSort{Values: ase.accumulating, NBF: ase.nbf})
		if err != nil {
			return nil, err
		}

		coll := NewKVPCollection(ase.nbf, ase.accumulating)
		ase.sortedColls = append(ase.sortedColls, coll)
		ase.accumulating = nil
	}

	if err := ase.sortGroup.Wait(); err != nil {
		return nil, err
	}

	if err := ase.mergeCollections(); err != nil {
		return nil, err
	}

	return ase.iterator(), nil
}

// Close ensures that the accumulator is closed. Repeat calls are allowed. This and FinishedEditing are not thread safe,
// and thus external synchronization is required.
func (ase *AsyncSortedEdits) Close() {
	if !ase.closed {
		_, _ = ase.FinishedEditing()
	}
}

// mergeCollections performs a concurrent sorted-merge of |sortedColls|. Must be called after |sortGroup| is complete.
// Once this completes use the |iterator| method for getting a KVPIterator which can be used to iterate over all the
// KVPs in order.
func (ase *AsyncSortedEdits) mergeCollections() error {
	sema := semaphore.NewWeighted(int64(ase.sortConcurrency))
	for len(ase.sortedColls) > 2 {
		pairs := pairCollections(ase.sortedColls)
		ase.sortedColls = make([]*KVPCollection, len(pairs))
		mergeGroup, ctx := errgroup.WithContext(context.TODO())

		for i := range pairs {
			colls := pairs[i]
			if colls[1] == nil {
				ase.sortedColls[i] = colls[0]
			} else {
				if err := sema.Acquire(ctx, 1); err != nil {
					if werr := mergeGroup.Wait(); werr != nil {
						return werr
					}
					return err
				}
				capi := i
				mergeGroup.Go(func() error {
					defer sema.Release(1)
					var err error
					ase.sortedColls[capi], err = colls[0].DestructiveMerge(colls[1])
					return err
				})
			}
		}

		if err := mergeGroup.Wait(); err != nil {
			return err
		}
	}
	return nil
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

// iterator returns a KVPIterator instance that can iterate over all the KVPs in order.
func (ase *AsyncSortedEdits) iterator() types.EditProvider {
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
