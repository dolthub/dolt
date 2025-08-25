// Copyright 2021 Dolthub, Inc.
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
	"io"
	"sort"

	"github.com/dolthub/dolt/go/store/types"
)

type entry struct {
	key       types.Value
	val       types.Valuable
	readerIdx int
}

var _ types.EditProvider = (*EPMerger)(nil)

type EPMerger struct {
	ctx         context.Context
	vr          types.ValueReader
	eps         []types.EditProvider
	nextKVPS    []entry
	editsRead   int64
	numEPs      int
	epsWithData int
	reachedEOF  bool
}

// NewEPMerger takes a slice of TupleReaders, whose contents should be key sorted key value tuple
// pairs, and return a *EPMerger
func NewEPMerger(ctx context.Context, vr types.ValueReader, eps []types.EditProvider) (*EPMerger, error) {
	fep := &EPMerger{
		ctx:      ctx,
		vr:       vr,
		numEPs:   len(eps),
		eps:      eps,
		nextKVPS: make([]entry, 0, len(eps)),
	}

	// read in the initial values from each stream and put them into the nextKVPS slice in sorted order.
	for i := range eps {
		kvp, err := fep.eps[i].Next(ctx)
		if err == io.EOF {
			continue
		} else if err != nil {
			return nil, err
		}

		key, val, err := keyAndValForKVP(ctx, kvp)
		if err != nil {
			return nil, err
		}

		// store the kvp along with the index of the reader it was read from.
		newEntry := entry{key: key, val: val, readerIdx: i}

		// binary search for where this entry should be inserted within the slice
		insIdx, err := search(ctx, vr, i, key, fep.nextKVPS)
		if err != nil {
			return nil, err
		}

		// grow the slice of entries
		fep.nextKVPS = fep.nextKVPS[:len(fep.nextKVPS)+1]

		// if necessary move existing entries to make room for new entry to be inserted in the correct place
		if insIdx < len(fep.nextKVPS)-1 {
			copy(fep.nextKVPS[insIdx+1:], fep.nextKVPS[insIdx:len(fep.nextKVPS)-1])
		}

		fep.nextKVPS[insIdx] = newEntry
	}

	fep.epsWithData = len(fep.nextKVPS)

	return fep, nil
}

func keyAndValForKVP(ctx context.Context, kvp *types.KVP) (key types.Value, val types.Valuable, err error) {
	key, err = kvp.Key.Value(ctx)
	if err != nil {
		return nil, nil, err
	}

	return key, kvp.Val, nil
}

// Next returns the next KVP representing the next edit to be applied.  Next will always return KVPs
// in key sorted order.  Once all KVPs have been read io.EOF will be returned.
func (fep *EPMerger) Next(ctx context.Context) (*types.KVP, error) {
	if fep.epsWithData == 0 {
		return nil, io.EOF
	}

	// nextKVP taken from index 0 as fep.nextKVPS are sorted low to high so element 0 is the next item
	nextKVP := fep.nextKVPS[0]

	// read the next tuple from the TupleStream that next kvp was read from
	kvp, err := fep.eps[nextKVP.readerIdx].Next(ctx)
	if err == io.EOF {
		// shrink the slice to only hold valid ordered data
		fep.nextKVPS = fep.nextKVPS[1:]
		fep.epsWithData--
		fep.reachedEOF = fep.epsWithData == 0

		// close the reader and execute close callback
		fep.eps[nextKVP.readerIdx].Close(fep.ctx)
	} else if err != nil {
		return nil, err
	} else {
		key, val, err := keyAndValForKVP(fep.ctx, kvp)
		if err != nil {
			return nil, err
		}

		// search for the location where the item should be placed
		insPos, err := search(ctx, fep.vr, nextKVP.readerIdx, key, fep.nextKVPS[1:])
		if err != nil {
			return nil, err
		}

		// if we are not inserting at the front move the items before the insertion index up
		if insPos > 0 {
			copy(fep.nextKVPS, fep.nextKVPS[1:insPos+1])
		}

		// insert the new entry
		fep.nextKVPS[insPos] = entry{key: key, val: val, readerIdx: nextKVP.readerIdx}
	}

	fep.editsRead++
	return &types.KVP{
		Key: nextKVP.key,
		Val: nextKVP.val,
	}, nil
}

// ReachedEOF returns true once all data is exhausted.  If ReachedEOF returns false that does not mean that there
// is more data, only that io.EOF has not been returned previously.  If ReachedEOF returns true then all edits have
// been read
func (fep *EPMerger) ReachedEOF() bool {
	return fep.reachedEOF
}

type comparableValue interface {
	Compare(ctx context.Context, nbf *types.NomsBinFormat, other types.LesserValuable) (int, error)
}

// search does a binary search or a sorted []entry and returns an integer representing the insertion index where the
// item should be placed in order to keep the vals sorted
func search(ctx context.Context, vr types.ValueReader, readerIdx int, key types.Value, vals []entry) (int, error) {
	var err error
	var n int
	if comparable, ok := key.(comparableValue); ok {
		n = sort.Search(len(vals), func(i int) bool {
			if err != nil {
				return false
			}

			var res int
			res, err = comparable.Compare(ctx, vr.Format(), vals[i].key)
			if err != nil {
				return false
			} else if res < 0 {
				return true
			} else if res > 0 {
				return false
			}

			return readerIdx < vals[i].readerIdx
		})
	} else {
		n = sort.Search(len(vals), func(i int) bool {
			if err != nil {
				return false
			}

			var isLess bool
			isLess, err = key.Less(ctx, vr.Format(), vals[i].key)
			if err != nil {
				return false
			} else if isLess {
				return true
			}

			if key.Equals(vals[i].key) {
				return readerIdx < vals[i].readerIdx
			}

			return false
		})
	}

	if err != nil {
		return 0, err
	}

	return n, nil
}

func (fep *EPMerger) Close(ctx context.Context) error {
	var firstErr error
	for _, ep := range fep.eps {
		err := ep.Close(ctx)

		if firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}
