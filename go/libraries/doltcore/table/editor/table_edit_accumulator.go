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

package editor

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/types/edits"
)

const (
	invalidEaId = 0xFFFFFFFF
)

type doltKVP struct {
	k types.Tuple
	v types.Tuple
}

type TableEditAccumulator interface {
	// Delete adds a row to be deleted when these edits are eventually applied. Updates are modeled as a delete and an insert
	Delete(keyHash hash.Hash, key types.Tuple) error

	// Insert adds a row to be inserted when these edits are eventually applied. Updates are modeled as a delete and an insert.
	Insert(keyHash hash.Hash, key types.Tuple, val types.Tuple) error

	// Get returns a *doltKVP if the current TableEditAccumulator contains the given key, or it exists in the row data.
	// This assumes that the given hash is for the given key.
	Get(ctx context.Context, keyHash hash.Hash, key types.Tuple) (*doltKVP, bool, error)

	// HasPartial returns true if the current TableEditAccumulator contains the given partialKey
	HasPartial(ctx context.Context, idxSch schema.Schema, partialKeyHash hash.Hash, partialKey types.Tuple) ([]hashedTuple, error)

	// Commit applies the in memory edits to the list of committed in memory edits
	Commit(ctx context.Context, nbf *types.NomsBinFormat) error

	// Rollback rolls back in memory edits until it reaches the state represented by the savedTea
	Rollback(ctx context.Context) error

	// MaterializeEdits commits and applies the in memory edits to the row data
	MaterializeEdits(ctx context.Context, nbf *types.NomsBinFormat) (m types.Map, err error)
}

// var for testing
var flushThreshold int64 = 256 * 1024

// inMemModifications represent row adds and deletes that have not been written to the underlying storage and only exist
// in memory
type inMemModifications struct {
	ops     int64
	adds    map[hash.Hash]*doltKVP
	deletes map[hash.Hash]types.Tuple
}

// newInMemModifications returns a pointer to a newly created inMemModifications object
func newInMemModifications() *inMemModifications {
	return &inMemModifications{
		adds:    make(map[hash.Hash]*doltKVP),
		deletes: make(map[hash.Hash]types.Tuple),
	}
}

// MergeIn merges changes from another inMemModifications object into this instance
func (mods *inMemModifications) MergeIn(other *inMemModifications) {
	for keyHash, key := range other.deletes {
		delete(mods.adds, keyHash)
		mods.deletes[keyHash] = key
	}

	for keyHash, kvp := range other.adds {
		delete(mods.deletes, keyHash)
		mods.adds[keyHash] = kvp
	}

	mods.ops += other.ops
}

// Get returns whether a key hash has been added as an insert, or a delete in this inMemModifications object. If it is
// an insert the associated KVP is returned as well.
func (mods *inMemModifications) Get(keyHash hash.Hash) (kvp *doltKVP, added, deleted bool) {
	kvp, added = mods.adds[keyHash]

	if added {
		return kvp, true, false
	}

	_, deleted = mods.deletes[keyHash]

	return nil, false, deleted
}

// tableEditAccumulatorImpl accumulates edits that need to be applied to the table row data.  It needs to be able to
// support rollback and commit without having to materialize the types.Map. To do this it tracks committed and uncommitted
// modifications in memory. When a commit occurs the list of uncommitted changes are added to the list of committed changes.
// When a rollback occurs uncommitted changes are dropped.
//
// In addition to the in memory edits, the changes are applied to committedEA when a commit occurs. It is possible
// for the uncommitted changes to become so large that they need to be flushed to disk. At this point we change modes to write all edits
// to a separate map edit accumulator as they occur until the next commit occurs.
type tableEditAccumulatorImpl struct {
	vr types.ValueReader

	// initial state of the map
	rowData types.Map

	// in memory changes which will be applied to the rowData when the map is materialized
	committed   *inMemModifications
	uncommitted *inMemModifications

	// accumulatorIdx defines the order in which types.EditAccumulators will be applied
	accumulatorIdx uint64

	// flusher manages flushing of the types.EditAccumulators to disk when needed
	flusher *edits.DiskEditFlusher

	// committedEaIds tracks ids of edit accumulators which have changes that have been committed
	committedEaIds *set.Uint64Set
	// uncommittedEAIds tracks ids of edit accumulators which have not been committed yet.
	uncommittedEaIds *set.Uint64Set

	// commitEA is the types.EditAccumulator containing the committed changes that are being accumulated currently
	commitEA types.EditAccumulator
	// commitEAId is the id used for ordering the commitEA with other types.EditAccumulators that will be applied when
	// materializing all changes.
	commitEAId uint64

	// flushingUncommitted is a flag that tracks whether we are in a state where we write uncommitted map edits to uncommittedEA
	flushingUncommitted bool
	// lastFlush is the number of uncommitted ops that had occurred at the time of the last flush
	lastFlush int64
	// uncommittedEA is a types.EditAccumulator that we write to as uncommitted edits come in when the number of uncommitted
	// edits becomes large
	uncommittedEA types.EditAccumulator
	// uncommittedEAId is the id used for ordering the uncommittedEA with other types.EditAccumulators that will be applied
	// when materializing all changes
	uncommittedEAId uint64
}

// Get returns a *doltKVP if the current TableEditAccumulator contains the given key, or it exists in the row data.
// This assumes that the given hash is for the given key.
func (tea *tableEditAccumulatorImpl) Get(ctx context.Context, keyHash hash.Hash, key types.Tuple) (*doltKVP, bool, error) {
	// in order of the most recent changes to the least recent falling back to what is in the materialized row data
	orderedMods := []*inMemModifications{tea.uncommitted, tea.committed}
	for _, mods := range orderedMods {
		kvp, added, deleted := mods.Get(keyHash)

		if added {
			return kvp, true, nil
		} else if deleted {
			return nil, false, nil
		}
	}

	v, ok, err := tea.rowData.MaybeGetTuple(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	return &doltKVP{k: key, v: v}, true, err
}

func (tea *tableEditAccumulatorImpl) HasPartial(ctx context.Context, idxSch schema.Schema, partialKeyHash hash.Hash, partialKey types.Tuple) ([]hashedTuple, error) {
	var err error
	var matches []hashedTuple
	var mapIter table.ReadCloser = noms.NewNomsRangeReader(tea.vr, idxSch, tea.rowData, []*noms.ReadRange{
		{Start: partialKey, Inclusive: true, Reverse: false, Check: noms.InRangeCheckPartial(partialKey)}})
	defer mapIter.Close(ctx)
	var r row.Row
	for r, err = mapIter.ReadRow(ctx); err == nil; r, err = mapIter.ReadRow(ctx) {
		tplKeyVal, err := r.NomsMapKey(idxSch).Value(ctx)
		if err != nil {
			return nil, err
		}
		key := tplKeyVal.(types.Tuple)
		tplValVal, err := r.NomsMapValue(idxSch).Value(ctx)
		if err != nil {
			return nil, err
		}
		val := tplValVal.(types.Tuple)
		keyHash, err := key.Hash(key.Format())
		if err != nil {
			return nil, err
		}
		matches = append(matches, hashedTuple{key, val, keyHash})
	}

	if err != io.EOF {
		return nil, err
	}

	orderedMods := []*inMemModifications{tea.committed, tea.uncommitted}
	for _, mods := range orderedMods {
		for i := len(matches) - 1; i >= 0; i-- {
			if _, ok := mods.adds[matches[i].hash]; ok {
				matches[i] = matches[len(matches)-1]
				matches = matches[:len(matches)-1]
			}
		}
		if added, ok := mods.adds[partialKeyHash]; ok {
			matches = append(matches, hashedTuple{key: added.k, value: added.v})
		}
	}

	return matches, nil
}

func (tea *tableEditAccumulatorImpl) flushUncommitted() {
	// if we are not already actively writing edits to the uncommittedEA then change the state and push all in mem edits
	// to a types.EditAccumulator
	if !tea.flushingUncommitted {
		tea.flushingUncommitted = true

		if tea.commitEA != nil && tea.commitEA.EditsAdded() > 0 {
			// if there are uncommitted flushed changes we need to flush the committed changes first
			// so they can be applied before the uncommitted flushed changes and future changes can be applied after
			tea.committedEaIds.Add(tea.commitEAId)
			tea.flusher.Flush(tea.commitEA, tea.commitEAId)

			tea.commitEA = nil
			tea.commitEAId = invalidEaId
		}

		tea.uncommittedEA = edits.NewAsyncSortedEditsWithDefaults(tea.vr)
		tea.uncommittedEAId = tea.accumulatorIdx
		tea.accumulatorIdx++

		for _, kvp := range tea.uncommitted.adds {
			tea.uncommittedEA.AddEdit(kvp.k, kvp.v)
		}

		for _, key := range tea.uncommitted.deletes {
			tea.uncommittedEA.AddEdit(key, nil)
		}
	}

	// flush uncommitted
	tea.lastFlush = tea.uncommitted.ops
	tea.uncommittedEaIds.Add(tea.uncommittedEAId)
	tea.flusher.Flush(tea.uncommittedEA, tea.uncommittedEAId)

	// initialize a new types.EditAccumulator for additional uncommitted edits to be written to.
	tea.uncommittedEA = edits.NewAsyncSortedEditsWithDefaults(tea.vr)
	tea.uncommittedEAId = tea.accumulatorIdx
	tea.accumulatorIdx++
}

// Delete adds a row to be deleted when these edits are eventually applied. Updates are modeled as a delete and an insert
func (tea *tableEditAccumulatorImpl) Delete(keyHash hash.Hash, key types.Tuple) error {
	delete(tea.uncommitted.adds, keyHash)
	tea.uncommitted.deletes[keyHash] = key
	tea.uncommitted.ops++

	if tea.flushingUncommitted {
		tea.uncommittedEA.AddEdit(key, nil)

		if tea.uncommitted.ops-tea.lastFlush > flushThreshold {
			tea.flushUncommitted()
		}
	} else if tea.uncommitted.ops > flushThreshold {
		tea.flushUncommitted()
	}

	return nil
}

// Insert adds a row to be inserted when these edits are eventually applied. Updates are modeled as a delete and an insert.
func (tea *tableEditAccumulatorImpl) Insert(keyHash hash.Hash, key types.Tuple, val types.Tuple) error {
	delete(tea.uncommitted.deletes, keyHash)
	tea.uncommitted.adds[keyHash] = &doltKVP{k: key, v: val}
	tea.uncommitted.ops++

	if tea.flushingUncommitted {
		tea.uncommittedEA.AddEdit(key, val)

		if tea.uncommitted.ops-tea.lastFlush > flushThreshold {
			tea.flushUncommitted()
		}
	} else if tea.uncommitted.ops > flushThreshold {
		tea.flushUncommitted()
	}

	return nil
}

// Commit applies the in memory edits to the list of committed in memory edits
func (tea *tableEditAccumulatorImpl) Commit(ctx context.Context, nbf *types.NomsBinFormat) error {
	if tea.uncommitted.ops > 0 {
		if !tea.flushingUncommitted {
			// if there are uncommitted changes add them to the committed list of map edits
			for _, kvp := range tea.uncommitted.adds {
				tea.commitEA.AddEdit(kvp.k, kvp.v)
			}

			for _, key := range tea.uncommitted.deletes {
				tea.commitEA.AddEdit(key, nil)
			}
		} else {
			// if we were flushing to the uncommittedEA make the current uncommittedEA the active committedEA and add
			// any uncommittedEA IDs that we already flushed
			tea.commitEA = tea.uncommittedEA
			tea.commitEAId = tea.uncommittedEAId
			tea.committedEaIds.Add(tea.uncommittedEaIds.AsSlice()...)

			// reset state to not be flushing uncommitted
			tea.uncommittedEA = nil
			tea.uncommittedEAId = invalidEaId
			tea.uncommittedEaIds = set.NewUint64Set(nil)
			tea.lastFlush = 0
			tea.flushingUncommitted = false
		}

		// apply in memory uncommitted changes to the committed in memory edits
		tea.committed.MergeIn(tea.uncommitted)

		// initialize uncommitted to future in memory edits
		tea.uncommitted = newInMemModifications()
	}

	return nil
}

// Rollback rolls back in memory edits until it reaches the state represented by the savedTea
func (tea *tableEditAccumulatorImpl) Rollback(ctx context.Context) error {
	// drop uncommitted ea IDs
	tea.uncommittedEaIds = set.NewUint64Set(nil)

	if tea.uncommitted.ops > 0 {
		tea.uncommitted = newInMemModifications()

		if tea.flushingUncommitted {
			_ = tea.uncommittedEA.Close(ctx)
			tea.uncommittedEA = nil
			tea.uncommittedEAId = invalidEaId
			tea.uncommittedEaIds = set.NewUint64Set(nil)
			tea.lastFlush = 0
			tea.flushingUncommitted = false
		}
	}

	return nil
}

// MaterializeEdits applies the in memory edits to the row data and returns types.Map
func (tea *tableEditAccumulatorImpl) MaterializeEdits(ctx context.Context, nbf *types.NomsBinFormat) (m types.Map, err error) {
	// In the case where the current edits become so large that they need to be flushed to disk, the committed edits will also be flushed
	// to disk first before the uncommitted edits.  When commit gets run now the uncommitted edits will then become committed edits,
	// but they need to be applied after the flushed edits.  So in the loop below where we build the list of EditProviders the newly
	// committed edits must be applied last.
	err = tea.Commit(ctx, nbf)
	if err != nil {
		return types.EmptyMap, err
	}

	if tea.committed.ops == 0 {
		return tea.rowData, nil
	}

	committedEP, err := tea.commitEA.FinishedEditing(ctx)
	tea.commitEA = nil
	if err != nil {
		return types.EmptyMap, err
	}

	flushedEPs, err := tea.flusher.WaitForIDs(ctx, tea.committedEaIds)
	if err != nil {
		return types.EmptyMap, err
	}

	eps := make([]types.EditProvider, 0, len(flushedEPs)+1)
	for i := 0; i < len(flushedEPs); i++ {
		eps = append(eps, flushedEPs[i].Edits)
	}
	eps = append(eps, committedEP)

	defer func() {
		for _, ep := range eps {
			_ = ep.Close(ctx)
		}
	}()

	accEdits, err := edits.NewEPMerger(ctx, tea.vr, eps)
	if err != nil {
		return types.EmptyMap, err
	}

	// We are guaranteed that rowData is valid, as we process teas sequentially.
	updatedMap, _, err := types.ApplyEdits(ctx, accEdits, tea.rowData)
	if err != nil {
		return types.EmptyMap, err
	}

	tea.rowData = updatedMap
	tea.committed = newInMemModifications()
	tea.commitEAId = tea.accumulatorIdx
	tea.accumulatorIdx++
	tea.commitEA = edits.NewAsyncSortedEditsWithDefaults(tea.vr)
	tea.committedEaIds = set.NewUint64Set(nil)
	tea.uncommittedEaIds = set.NewUint64Set(nil)

	return updatedMap, nil
}

// DbEaFactory is an interface for a factory object used to make table and index edit accumulators
type DbEaFactory interface {
	// NewTableEA creates a TableEditAccumulator
	NewTableEA(ctx context.Context, rowData types.Map) TableEditAccumulator
	// NewIndexEA creates an IndexEditAccumulator
	NewIndexEA(ctx context.Context, rowData types.Map) IndexEditAccumulator
}

type dbEaFactory struct {
	directory string
	vrw       types.ValueReadWriter
}

// NewDbEaFactory creates a DbEaFatory which uses the provided directory to hold temp files
func NewDbEaFactory(directory string, vrw types.ValueReadWriter) DbEaFactory {
	return &dbEaFactory{
		directory: directory,
		vrw:       vrw,
	}
}

// NewTableEA creates a TableEditAccumulator
func (deaf *dbEaFactory) NewTableEA(ctx context.Context, rowData types.Map) TableEditAccumulator {
	return &tableEditAccumulatorImpl{
		vr:                  deaf.vrw,
		rowData:             rowData,
		committed:           newInMemModifications(),
		uncommitted:         newInMemModifications(),
		accumulatorIdx:      1,
		flusher:             edits.NewDiskEditFlusher(ctx, deaf.directory, deaf.vrw),
		committedEaIds:      set.NewUint64Set(nil),
		uncommittedEaIds:    set.NewUint64Set(nil),
		commitEA:            edits.NewAsyncSortedEditsWithDefaults(deaf.vrw),
		commitEAId:          0,
		flushingUncommitted: false,
		lastFlush:           0,
		uncommittedEA:       nil,
		uncommittedEAId:     invalidEaId,
	}
}

// NewIndexEA creates an IndexEditAccumulator
func (deaf *dbEaFactory) NewIndexEA(ctx context.Context, rowData types.Map) IndexEditAccumulator {
	return &indexEditAccumulatorImpl{
		vr:                  deaf.vrw,
		rowData:             rowData,
		committed:           newInMemIndexEdits(),
		uncommitted:         newInMemIndexEdits(),
		commitEA:            edits.NewAsyncSortedEditsWithDefaults(deaf.vrw),
		commitEAId:          0,
		accumulatorIdx:      1,
		flusher:             edits.NewDiskEditFlusher(ctx, deaf.directory, deaf.vrw),
		committedEaIds:      set.NewUint64Set(nil),
		uncommittedEaIds:    set.NewUint64Set(nil),
		flushingUncommitted: false,
		lastFlush:           0,
		uncommittedEA:       nil,
		uncommittedEAId:     invalidEaId,
	}
}
