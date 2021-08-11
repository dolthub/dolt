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
	"errors"
	"github.com/dolthub/dolt/go/store/types"
	"io"
	"os"
)

type entry struct {
	kvp *types.KVP
	readerIdx int
}

var _ types.EditProvider = (*TupleStreamMerger)(nil)

type TupleStreamMerger struct {
	ctx context.Context
	nbf *types.NomsBinFormat
	numEdits int64
	editsRead int64

	numReaders int
	readersWithData int
	readers []types.TupleReadCloser
	nextKVPS []entry

	readerClosed func(i int)
}

// EditProviderForFiles takes a list of filenames whose contents are alternating key value tuples written out using a
// TupleWriter and returns a TupleStreamMerger.  The tuples within each file must be sorted by the key
// tuples in order for valid.  The streams of tuples are merge sorted as they are read.
func EditProviderForFiles(ctx context.Context, nbf *types.NomsBinFormat, vrw types.ValueReadWriter, filenames []string, numEdits int64, deleteOnExit bool) (*TupleStreamMerger, error) {
	// Open each file and create a TupleReader from it.
	readers := make([]types.TupleReadCloser, len(filenames))
	for i, name := range filenames {
		f, err := os.Open(name)
		if err != nil {
			return nil, err
		}

		readers[i] = types.NewTupleReader(nbf, vrw, f)
	}

	deleteFile := func(i int) {
		if deleteOnExit {
			path := filenames[i]
			_ = os.Remove(path)
		}
	}

	return NewTupleStreamMerger(ctx, nbf, readers, numEdits, deleteFile)
}

// NewTupleStreamMerger takes a slice of TupleReaders, whose contents should be key sorted key value tuple
// pairs, and return a *TupleStreamMerger
func NewTupleStreamMerger(ctx context.Context, nbf *types.NomsBinFormat, readers []types.TupleReadCloser, numEdits int64, readerClosed func(i int)) (*TupleStreamMerger, error) {
	fep := &TupleStreamMerger{
		ctx:             ctx,
		nbf:             nbf,
		numEdits:        numEdits,
		numReaders:      len(readers),
		readers:         readers,
		nextKVPS:        make([]entry, 0, len(readers)),
		readerClosed:    readerClosed,
	}

	// read in the initial values from each stream and put them into the nextKVPS slice in sorted order.
	for i := range readers {
		 kvp, err := fep.readKVP(i)
		 if err == io.EOF {
		 	continue
		 } else if err != nil {
		 	return nil, err
		 }

		 // store the kvp along with the index of the reader it was read from.
		 newEntry := entry{ kvp: kvp, readerIdx: i }

		 // binary search for where this entry should be inserted within the slice
		 insIdx, err := search(nbf, kvp.Key, fep.nextKVPS)
		 if err != nil {
		 	return nil, err
		 }

		 // grow the slice of entries
		 fep.nextKVPS = fep.nextKVPS[:len(fep.nextKVPS) + 1]

		 // if necessary move existing entries to make room for new entry to be inserted in the correct place
		 if insIdx < len(fep.nextKVPS) - 1 {
		 	copy(fep.nextKVPS[insIdx+1:], fep.nextKVPS[insIdx:len(fep.nextKVPS) - 1])
		 }

		 fep.nextKVPS[insIdx] = newEntry
	}

	fep.readersWithData = len(fep.nextKVPS)

	return fep, nil
}

// Next returns the next KVP representing the next edit to be applied.  Next will always return KVPs
// in key sorted order
func (fep *TupleStreamMerger) Next() (*types.KVP, error) {
	if fep.readersWithData == 0 {
		return nil, io.EOF
	}

	// nextKVP taken from index 0 as fep.nextKVPS are sorted low to high so element 0 is the next item
	nextKVP := fep.nextKVPS[0]

	// read the next tuple from the TupleStream that next kvp was read from
	kvp, err := fep.readKVP(nextKVP.readerIdx)
	if err == io.EOF {
		// shrink the slice to only hold valid ordered data
		fep.nextKVPS = fep.nextKVPS[1:]
		fep.readersWithData--

		// close the reader and execute close callback
		fep.readers[nextKVP.readerIdx].Close(fep.ctx)

		if fep.readerClosed != nil {
			fep.readerClosed(nextKVP.readerIdx)
		}
	} else if err != nil {
		return nil, err
	} else {
		// search for the location where the item should be placed
		insPos, err := search(fep.nbf, kvp.Key, fep.nextKVPS[1:])

		if err != nil {
			return nil, err
		}

		// if we are not inserting at the front move the items before the insertion index up
		if insPos > 0 {
			copy(fep.nextKVPS, fep.nextKVPS[1:insPos+1])
		}

		// insert the new entry
		fep.nextKVPS[insPos] = entry{kvp: kvp, readerIdx: nextKVP.readerIdx}
	}

	fep.editsRead++
	return nextKVP.kvp, nil
}

// NumEdits returns the number of KVPs representing the edits that will be provided when calling next
func (fep *TupleStreamMerger) NumEdits() int64 {
	return fep.numEdits
}

// readKVP reads the next KVP off the TupleReader with the given index
func (fep *TupleStreamMerger) readKVP(readerIdx int) (*types.KVP, error) {
	rd := fep.readers[readerIdx]
	k, err := rd.Read()
	if err != nil {
		return nil, err
	}

	v, err := rd.Read()
	if err == io.EOF {
		 return nil, errors.New("corrupt tuple stream has a key without a value")
	} else if err != nil {
		 return nil, err
	}

	return &types.KVP{Key: k, Val: v}, nil
}

// search does a binary search or a sorted []entry and returns an integer representing the insertion index where the
// item should be placed in order to keep the vals sorted
func search(nbf *types.NomsBinFormat, key types.LesserValuable, vals []entry) (int, error) {
	const linearScanSize = 3

	if len(vals) == 0 {
		return 0, nil
	}

	start := 0
	end := len(vals) - 1

	for {
		entries := end - start

		if entries <= linearScanSize {
			// when the number of items left to check is below some threshold do a linear scan to find the insertion index
			for i := start; i <= end; i++ {
				isLess, err := key.Less(nbf, vals[i].kvp.Key)
				if err != nil {
					return 0, err
				} else if isLess {
					return i, nil
				}
			}

			return end + 1, nil
		}

		// choose the middle element and see i we need to cut off the top or bottom elements
		pos := start + (entries/2)
		isLess, err := key.Less(nbf, vals[pos].kvp.Key)

		if err != nil {
			return 0, err
		}

		if isLess {
			end = pos -1
		} else {
			start = pos
		}
	}
}