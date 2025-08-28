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

	"github.com/dolthub/dolt/go/store/types"
)

var _ types.EditAccumulator = (*DiskBackedEditAcc)(nil)

// DiskBackedEditAcc is an EditAccumulator implementation that flushes the edits to disk at regular intervals
type DiskBackedEditAcc struct {
	ctx           context.Context
	vrw           types.ValueReadWriter
	backing       types.EditAccumulator
	flusher       *DiskEditFlusher
	newEditAcc    func() types.EditAccumulator
	files         chan string
	accumulated   int64
	flushInterval int64
	flushCount    int
}

// NewDiskBackedEditAcc returns a new DiskBackedEditAccumulator instance
func NewDiskBackedEditAcc(ctx context.Context, vrw types.ValueReadWriter, flushInterval int64, directory string, newEditAcc func() types.EditAccumulator) *DiskBackedEditAcc {
	return &DiskBackedEditAcc{
		ctx:           ctx,
		vrw:           vrw,
		flusher:       NewDiskEditFlusher(ctx, directory, vrw),
		newEditAcc:    newEditAcc,
		backing:       newEditAcc(),
		flushInterval: flushInterval,
	}
}

// EditsAdded returns the number of edits that have been added to this EditAccumulator
func (dbea *DiskBackedEditAcc) EditsAdded() int {
	return int(dbea.accumulated)
}

// AddEdit adds an edit. Not thread safe
func (dbea *DiskBackedEditAcc) AddEdit(key types.LesserValuable, val types.Valuable) {
	dbea.backing.AddEdit(key, val)
	dbea.accumulated++

	if dbea.accumulated%dbea.flushInterval == 0 {
		// flush interval reached.  kick off a background routine to process everything
		dbea.flusher.Flush(dbea.backing, uint64(dbea.flushCount))
		dbea.flushCount++
		dbea.backing = dbea.newEditAcc()
	}
}

// FinishedEditing should be called when all edits have been added to get an EditProvider which provides the
// edits in sorted order. Adding more edits after calling FinishedEditing is an error.
func (dbea *DiskBackedEditAcc) FinishedEditing(ctx context.Context) (types.EditProvider, error) {
	// If we never flushed to disk then there is no need.  Just return the data from the backing edit accumulator
	if dbea.flushCount == 0 {
		return dbea.backing.FinishedEditing(ctx)
	}

	// flush any data we haven't flushed yet before processing
	sinceLastFlush := dbea.accumulated % dbea.flushInterval
	if sinceLastFlush > 0 {
		dbea.flusher.Flush(dbea.backing, (uint64(dbea.flushCount)))
		dbea.flushCount++
		dbea.backing = nil
	}

	results, err := dbea.flusher.Wait(ctx)
	if err != nil {
		return nil, err
	}

	eps := make([]types.EditProvider, len(results))
	for i := 0; i < len(results); i++ {
		eps[i] = results[i].Edits
	}

	return NewEPMerger(ctx, dbea.vrw, eps)
}

// Close ensures that the accumulator is closed. Repeat calls are allowed. Not guaranteed to be thread-safe, thus
// requires external synchronization.
func (dbea *DiskBackedEditAcc) Close(ctx context.Context) error {
	if dbea.backing != nil {
		err := dbea.backing.Close(ctx)
		dbea.backing = nil

		return err
	}

	return nil
}
