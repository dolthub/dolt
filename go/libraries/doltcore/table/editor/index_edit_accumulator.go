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

// var for testing
var indexFlushThreshold int64 = 256 * 1024

type IndexEditAccumulator interface {
	// Delete adds a row to be deleted when these edits are eventually applied.
	Delete(ctx context.Context, keyHash, partialKeyHash hash.Hash, key, value types.Tuple) error

	// Insert adds a row to be inserted when these edits are eventually applied.
	Insert(ctx context.Context, keyHash, partialKeyHash hash.Hash, key, value types.Tuple) error

	// Has returns true if the current TableEditAccumulator contains the given key, or it exists in the row data.
	Has(ctx context.Context, keyHash hash.Hash, key types.Tuple) (bool, error)

	// HasPartial returns true if the current TableEditAccumulator contains the given partialKey
	HasPartial(ctx context.Context, idxSch schema.Schema, partialKeyHash hash.Hash, partialKey types.Tuple) ([]hashedTuple, error)

	// Commit applies the in memory edits to the list of committed in memory edits
	Commit(ctx context.Context, nbf *types.NomsBinFormat) error

	// Rollback rolls back in memory edits until it reaches the state represented by the savedTea
	Rollback(ctx context.Context) error

	// MaterializeEdits commits and applies the in memory edits to the row data
	MaterializeEdits(ctx context.Context, nbf *types.NomsBinFormat) (types.Map, error)
}

// hashedTuple is a tuple accompanied by a hash. The representing value of the hash is dependent on the function
// it is obtained from.
type hashedTuple struct {
	key   types.Tuple
	value types.Tuple
	hash  hash.Hash
}

// inMemIndexEdits represent row adds and deletes that have not been written to the underlying storage and only exist in memory
type inMemIndexEdits struct {
	// addedPartialKeys is a map of partial keys to a map of full keys that match the partial key
	partialAdds map[hash.Hash]map[hash.Hash]types.Tuple
	// These hashes represent the hash of the partial key, with the tuple being the full key
	deletes map[hash.Hash]*hashedTuple
	// These hashes represent the hash of the partial key, with the tuple being the full key
	adds map[hash.Hash]*hashedTuple
	ops  int64
}

func newInMemIndexEdits() *inMemIndexEdits {
	return &inMemIndexEdits{
		partialAdds: make(map[hash.Hash]map[hash.Hash]types.Tuple),
		deletes:     make(map[hash.Hash]*hashedTuple),
		adds:        make(map[hash.Hash]*hashedTuple),
	}
}

// MergeIn merges changes from another inMemIndexEdits object into this instance
func (edits *inMemIndexEdits) MergeIn(other *inMemIndexEdits) {
	for keyHash, ht := range other.deletes {
		delete(edits.adds, keyHash)
		edits.deletes[keyHash] = ht
	}

	for keyHash, ht := range other.adds {
		delete(edits.deletes, keyHash)
		edits.adds[keyHash] = ht
	}

	for partialKeyHash, keyHashToPartialKey := range other.partialAdds {
		if dest, ok := edits.partialAdds[partialKeyHash]; !ok {
			edits.partialAdds[partialKeyHash] = keyHashToPartialKey
		} else {
			for keyHash, partialKey := range keyHashToPartialKey {
				dest[keyHash] = partialKey
			}
		}
	}

	edits.ops += other.ops
}

// Has returns whether a key hash has been added as an insert, or a delete in this inMemIndexEdits object
func (edits *inMemIndexEdits) Has(keyHash hash.Hash) (added, deleted bool) {
	if _, ok := edits.adds[keyHash]; ok {
		return true, false
	}
	if _, ok := edits.deletes[keyHash]; ok {
		return false, true
	}
	return false, false
}

// indexEditAccumulatorImpl is the index equivalent of the tableEditAccumulatorImpl.
//
// indexEditAccumulatorImpl accumulates edits that need to be applied to the index row data.  It needs to be able to
// support rollback and commit without having to materialize the types.Map. To do this it tracks committed and uncommitted
// modifications in memory. When a commit occurs the list of uncommitted changes are added to the list of committed changes.
// When a rollback occurs uncommitted changes are dropped.
//
// In addition to the in memory edits, the changes are applied to committedEA when a commit occurs. It is possible
// for the uncommitted changes to become so large that they need to be flushed to disk. At this point we change modes to write all edits
// to a separate map edit accumulator as they occur until the next commit occurs.
type indexEditAccumulatorImpl struct {
	nbf *types.NomsBinFormat

	// state of the index last time edits were applied
	rowData types.Map

	// in memory changes which will be applied to the rowData when the map is materialized
	committed   *inMemIndexEdits
	uncommitted *inMemIndexEdits

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

var _ IndexEditAccumulator = (*indexEditAccumulatorImpl)(nil)

func (iea *indexEditAccumulatorImpl) flushUncommitted() {
	// if we are not already actively writing edits to the uncommittedEA then change the state and push all in mem edits
	// to a types.EditAccumulator
	if !iea.flushingUncommitted {
		iea.flushingUncommitted = true

		if iea.commitEA != nil && iea.commitEA.EditsAdded() > 0 {
			// if there are uncommitted flushed changes we need to flush the committed changes first
			// so they can be applied before the uncommitted flushed changes and future changes can be applied after
			iea.committedEaIds.Add(iea.commitEAId)
			iea.flusher.Flush(iea.commitEA, iea.commitEAId)

			iea.commitEA = nil
			iea.commitEAId = invalidEaId
		}

		iea.uncommittedEA = edits.NewAsyncSortedEditsWithDefaults(iea.nbf)
		iea.uncommittedEAId = iea.accumulatorIdx
		iea.accumulatorIdx++

		for _, ht := range iea.uncommitted.adds {
			iea.uncommittedEA.AddEdit(ht.key, ht.value)
		}

		for _, ht := range iea.uncommitted.deletes {
			iea.uncommittedEA.AddEdit(ht.key, nil)
		}
	}

	// flush uncommitted
	iea.lastFlush = iea.uncommitted.ops
	iea.uncommittedEaIds.Add(iea.uncommittedEAId)
	iea.flusher.Flush(iea.uncommittedEA, iea.uncommittedEAId)

	// initialize a new types.EditAccumulator for additional uncommitted edits to be written to.
	iea.uncommittedEA = edits.NewAsyncSortedEditsWithDefaults(iea.nbf)
	iea.uncommittedEAId = iea.accumulatorIdx
	iea.accumulatorIdx++
}

// Insert adds a row to be inserted when these edits are eventually applied.
func (iea *indexEditAccumulatorImpl) Insert(ctx context.Context, keyHash, partialKeyHash hash.Hash, key, value types.Tuple) error {
	if _, ok := iea.uncommitted.deletes[keyHash]; ok {
		delete(iea.uncommitted.deletes, keyHash)
	} else {
		iea.uncommitted.adds[keyHash] = &hashedTuple{key, value, partialKeyHash}
		if matchingMap, ok := iea.uncommitted.partialAdds[partialKeyHash]; ok {
			matchingMap[keyHash] = key
		} else {
			iea.uncommitted.partialAdds[partialKeyHash] = map[hash.Hash]types.Tuple{keyHash: key}
		}
	}

	iea.uncommitted.ops++
	if iea.flushingUncommitted {
		iea.uncommittedEA.AddEdit(key, value)

		if iea.uncommitted.ops-iea.lastFlush > indexFlushThreshold {
			iea.flushUncommitted()
		}
	} else if iea.uncommitted.ops > indexFlushThreshold {
		iea.flushUncommitted()
	}
	return nil
}

// Delete adds a row to be deleted when these edits are eventually applied.
func (iea *indexEditAccumulatorImpl) Delete(ctx context.Context, keyHash, partialKeyHash hash.Hash, key, value types.Tuple) error {
	if _, ok := iea.uncommitted.adds[keyHash]; ok {
		delete(iea.uncommitted.adds, keyHash)
		delete(iea.uncommitted.partialAdds[partialKeyHash], keyHash)
	} else {
		iea.uncommitted.deletes[keyHash] = &hashedTuple{key, value, partialKeyHash}
	}

	iea.uncommitted.ops++
	if iea.flushingUncommitted {
		iea.uncommittedEA.AddEdit(key, nil)

		if iea.uncommitted.ops-iea.lastFlush > indexFlushThreshold {
			iea.flushUncommitted()
		}
	} else if iea.uncommitted.ops > indexFlushThreshold {
		iea.flushUncommitted()
	}
	return nil
}

// Has returns whether the current indexEditAccumulatorImpl contains the given key. This assumes that the given hash is for
// the given key.
func (iea *indexEditAccumulatorImpl) Has(ctx context.Context, keyHash hash.Hash, key types.Tuple) (bool, error) {
	// in order of most recent changes to least recent falling back to whats in the materialized row data
	orderedMods := []*inMemIndexEdits{iea.uncommitted, iea.committed}
	for _, mods := range orderedMods {
		added, deleted := mods.Has(keyHash)

		if added {
			return true, nil
		} else if deleted {
			return false, nil
		}
	}

	_, ok, err := iea.rowData.MaybeGetTuple(ctx, key)
	return ok, err
}

// HasPartial returns whether the current indexEditAccumulatorImpl contains the given partial key. This assumes that the
// given hash is for the given key. The hashes returned represent the hash of the returned tuple.
func (iea *indexEditAccumulatorImpl) HasPartial(ctx context.Context, idxSch schema.Schema, partialKeyHash hash.Hash, partialKey types.Tuple) ([]hashedTuple, error) {
	if hasNulls, err := partialKey.Contains(types.NullValue); err != nil {
		return nil, err
	} else if hasNulls { // rows with NULL are considered distinct, and therefore we do not match on them
		return nil, nil
	}

	var err error
	var matches []hashedTuple
	var mapIter table.TableReadCloser = noms.NewNomsRangeReader(idxSch, iea.rowData, []*noms.ReadRange{
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

	// reapply partial key edits in order
	orderedMods := []*inMemIndexEdits{iea.committed, iea.uncommitted}
	for _, mods := range orderedMods {
		for i := len(matches) - 1; i >= 0; i-- {
			// If we've removed a key that's present here, remove it from the slice
			if _, ok := mods.deletes[matches[i].hash]; ok {
				matches[i] = matches[len(matches)-1]
				matches = matches[:len(matches)-1]
			}
		}
		for addedHash, addedTpl := range mods.partialAdds[partialKeyHash] {
			matches = append(matches, hashedTuple{addedTpl, types.EmptyTuple(addedTpl.Format()), addedHash})
		}
	}
	return matches, nil
}

// Commit applies the in memory edits to the list of committed in memory edits
func (iea *indexEditAccumulatorImpl) Commit(ctx context.Context, nbf *types.NomsBinFormat) error {
	if iea.uncommitted.ops > 0 {
		if !iea.flushingUncommitted {
			// if there are uncommitted changes add them to the committed list of map edits
			for _, ht := range iea.uncommitted.adds {
				iea.commitEA.AddEdit(ht.key, ht.value)
			}

			for _, ht := range iea.uncommitted.deletes {
				iea.commitEA.AddEdit(ht.key, nil)
			}
		} else {
			// if we were flushing to the uncommittedEA make the current uncommittedEA the active committedEA and add
			// any uncommittedEA IDs that we already flushed
			iea.commitEA = iea.uncommittedEA
			iea.commitEAId = iea.uncommittedEAId
			iea.committedEaIds.Add(iea.uncommittedEaIds.AsSlice()...)

			// reset state to not be flushing uncommitted
			iea.uncommittedEA = nil
			iea.uncommittedEAId = invalidEaId
			iea.uncommittedEaIds = set.NewUint64Set(nil)
			iea.lastFlush = 0
			iea.flushingUncommitted = false
		}

		// apply in memory uncommitted changes to the committed in memory edits
		iea.committed.MergeIn(iea.uncommitted)

		// initialize uncommitted to future in memory edits
		iea.uncommitted = newInMemIndexEdits()
	}

	return nil
}

// Rollback rolls back in memory edits until it reaches the state represented by the savedTea
func (iea *indexEditAccumulatorImpl) Rollback(ctx context.Context) error {
	// drop uncommitted ea IDs
	iea.uncommittedEaIds = set.NewUint64Set(nil)

	if iea.uncommitted.ops > 0 {
		iea.uncommitted = newInMemIndexEdits()

		if iea.flushingUncommitted {
			_ = iea.uncommittedEA.Close(ctx)
			iea.uncommittedEA = nil
			iea.uncommittedEAId = invalidEaId
			iea.uncommittedEaIds = set.NewUint64Set(nil)
			iea.lastFlush = 0
			iea.flushingUncommitted = false
		}
	}

	return nil
}

// MaterializeEdits applies the in memory edits to the row data and returns types.Map
func (iea *indexEditAccumulatorImpl) MaterializeEdits(ctx context.Context, nbf *types.NomsBinFormat) (m types.Map, err error) {
	err = iea.Commit(ctx, nbf)
	if err != nil {
		return types.EmptyMap, err
	}

	if iea.committed.ops == 0 {
		return iea.rowData, nil
	}

	committedEP, err := iea.commitEA.FinishedEditing()
	iea.commitEA = nil
	if err != nil {
		return types.EmptyMap, err
	}

	flushedEPs, err := iea.flusher.WaitForIDs(ctx, iea.committedEaIds)
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

	accEdits, err := edits.NewEPMerger(ctx, nbf, eps)
	if err != nil {
		return types.EmptyMap, err
	}

	// We are guaranteed that rowData is valid, as we process ieas sequentially.
	updatedMap, _, err := types.ApplyEdits(ctx, accEdits, iea.rowData)
	if err != nil {
		return types.EmptyMap, err
	}

	iea.rowData = updatedMap
	iea.committed = newInMemIndexEdits()
	iea.commitEAId = iea.accumulatorIdx
	iea.accumulatorIdx++
	iea.commitEA = edits.NewAsyncSortedEditsWithDefaults(iea.nbf)
	iea.committedEaIds = set.NewUint64Set(nil)
	iea.uncommittedEaIds = set.NewUint64Set(nil)

	return updatedMap, nil
}
