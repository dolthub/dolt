// Copyright 2019 Dolthub, Inc.
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

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/dolthub/dolt/go/store/types"
)

// AsyncSortedEdits is a data structure that can have edits added to it, and as they are added it will
// send them in batches to be sorted.  Once all edits have been added the batches of edits can then
// be merge sorted together.
type AsyncSortedEdits struct {
	vr              types.ValueReader
	sortCtx         context.Context
	sortWork        chan types.KVPSort
	sortGroup       *errgroup.Group
	sema            *semaphore.Weighted
	accumulating    []types.KVP
	sortedColls     []*KVPCollection
	editsAdded      int
	sliceSize       int
	sortConcurrency int
	closed          bool
}

// NewAsyncSortedEditsWithDefaults creates a new AsyncSortedEdit instance with default concurrency and buffer size values
func NewAsyncSortedEditsWithDefaults(vr types.ValueReader) types.EditAccumulator {
	return NewAsyncSortedEdits(vr, 16*1024, 4, 2)
}

// NewAsyncSortedEdits creates an AsyncSortedEdits object that creates batches of size 'sliceSize' and kicks off
// 'asyncConcurrency' go routines for background sorting of batches.  The final Sort call is processed with
// 'sortConcurrency' go routines
func NewAsyncSortedEdits(vr types.ValueReader, sliceSize, asyncConcurrency, sortConcurrency int) *AsyncSortedEdits {
	group, groupCtx := errgroup.WithContext(context.TODO())
	sortCh := make(chan types.KVPSort, asyncConcurrency*4)
	return &AsyncSortedEdits{
		editsAdded:      0,
		sliceSize:       sliceSize,
		sortConcurrency: sortConcurrency,
		accumulating:    nil, // lazy alloc
		sortedColls:     nil,
		vr:              vr,
		sortWork:        sortCh,
		sortGroup:       group,
		sortCtx:         groupCtx,
		sema:            semaphore.NewWeighted(int64(asyncConcurrency)),
	}
}

// EditsAdded returns the number of edits that have been added to this EditAccumulator
func (ase *AsyncSortedEdits) EditsAdded() int {
	return ase.editsAdded
}

// AddEdit adds an edit. Not thread safe
func (ase *AsyncSortedEdits) AddEdit(k types.LesserValuable, v types.Valuable) {
	ase.editsAdded++
	if ase.accumulating == nil {
		ase.accumulating = make([]types.KVP, 0, ase.sliceSize)
	}

	ase.accumulating = append(ase.accumulating, types.KVP{Key: k, Val: v})
	if len(ase.accumulating) == ase.sliceSize {
		coll := NewKVPCollection(ase.vr, ase.accumulating)
		// ase.accumulating is getting sorted asynchronously and
		// in-place down below. We add it to |sortedColls| here.  By
		// the time |sortedColls| is used, it will be sorted.
		ase.sortedColls = append(ase.sortedColls, coll)
		toSort := types.KVPSort{Values: ase.accumulating}
		select {
		case ase.sortWork <- toSort:
			break
		default:
			if err := types.SortWithErroringLess(ase.sortCtx, ase.vr.Format(), toSort); err != nil {
				ase.sortGroup.Go(func() error {
					return err
				})
			}
		}
		if ase.sema.TryAcquire(1) {
			ase.sortGroup.Go(ase.sortWorker)
		}
		ase.accumulating = make([]types.KVP, 0, ase.sliceSize)
	}
}

// sortWorker is the async method that makes progress on |sortWork| until it
// is exhausted and then exits. Releases |1| from |ase.sema| when it exits.
func (ase *AsyncSortedEdits) sortWorker() error {
	defer ase.sema.Release(1)
	for {
		select {
		case toSort, ok := <-ase.sortWork:
			if !ok {
				return nil
			}
			if err := types.SortWithErroringLess(ase.sortCtx, ase.vr.Format(), toSort); err != nil {
				return err
			}
		case <-ase.sortCtx.Done():
			return ase.sortCtx.Err()
		default:
			return nil
		}
	}
}

// FinishedEditing should be called once all edits have been added. Once FinishedEditing is called adding more edits
// will have undefined behavior.
func (ase *AsyncSortedEdits) FinishedEditing(ctx context.Context) (types.EditProvider, error) {
	ase.closed = true

	if len(ase.accumulating) > 0 {
		toSort := types.KVPSort{Values: ase.accumulating}
		select {
		case ase.sortWork <- toSort:
			break
		default:
			if err := types.SortWithErroringLess(ase.sortCtx, ase.vr.Format(), toSort); err != nil {
				return nil, err
			}
		}
		coll := NewKVPCollection(ase.vr, ase.accumulating)
		ase.sortedColls = append(ase.sortedColls, coll)
		ase.accumulating = nil
	}

	close(ase.sortWork)

	// Calling thread helps work through remaining |sortWork| until it's sorted.
	for toSort := range ase.sortWork {
		if err := types.SortWithErroringLess(ctx, ase.vr.Format(), toSort); err != nil {
			return nil, err
		}
	}

	if err := ase.sortGroup.Wait(); err != nil {
		return nil, err
	}

	if err := ase.mergeCollections(ctx); err != nil {
		return nil, err
	}

	return ase.iterator(), nil
}

// Close ensures that the accumulator is closed. Repeat calls are allowed. This and FinishedEditing are not thread safe,
// and thus external synchronization is required.
func (ase *AsyncSortedEdits) Close(ctx context.Context) error {
	if !ase.closed {
		itr, err := ase.FinishedEditing(ctx)
		itrCloseErr := itr.Close(ctx)

		if err != nil {
			return err
		}

		return itrCloseErr
	}

	return nil
}

// mergeCollections performs a concurrent sorted-merge of |sortedColls|. Must be called after |sortGroup| is complete.
// Once this completes use the |iterator| method for getting a KVPIterator which can be used to iterate over all the
// KVPs in order.
func (ase *AsyncSortedEdits) mergeCollections(ctx context.Context) error {
	sema := semaphore.NewWeighted(int64(ase.sortConcurrency))
	for len(ase.sortedColls) > 2 {
		pairs := pairCollections(ase.sortedColls)
		ase.sortedColls = make([]*KVPCollection, len(pairs))
		mergeGroup, ctx := errgroup.WithContext(ctx)

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
					ase.sortedColls[capi], err = colls[0].DestructiveMerge(ctx, colls[1])
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

func pairCollections(colls []*KVPCollection) [][2]*KVPCollection {
	numColls := len(colls)
	pairs := make([][2]*KVPCollection, 0, numColls/2+1)

	// These pairs need to come back in order because our sort is stable.
	// If there is an odd number of collections, put the first element as a
	// single non-merge collection pair at the front of the list.
	if numColls%2 == 1 {
		pairs = append(pairs, [2]*KVPCollection{colls[0], nil})
		colls = colls[1:]
		numColls -= 1
	}

	for i := 0; i < numColls; i += 2 {
		pairs = append(pairs, [2]*KVPCollection{colls[i], colls[i+1]})
	}

	return pairs
}

// iterator returns a KVPIterator instance that can iterate over all the KVPs in order.
func (ase *AsyncSortedEdits) iterator() types.EditProvider {
	switch len(ase.sortedColls) {
	case 0:
		return types.EmptyEditProvider{}
	case 1:
		return NewItr(ase.vr, ase.sortedColls[0])
	case 2:
		return NewSortedEditItr(ase.vr, ase.sortedColls[0], ase.sortedColls[1])
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
