package editor

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/types/edits"
)

type doltKVP struct {
	k types.Tuple
	v types.Tuple
}

type TableEditAccumulator interface {
	// Delete adds a row to be deleted when these edits are eventually applied. Updates are modeled as a delete and an insert
	Delete(keyHash hash.Hash, key types.Tuple)

	// Insert adds a row to be inserted when these edits are eventually applied. Updates are modeled as a delete and an insert.
	Insert(keyHash hash.Hash, key types.Tuple, val types.Tuple)

	// Get returns a *doltKVP if the current TableEditAccumulator contains the given key, or it exists in the row data.
	// This assumes that the given hash is for the given key.
	Get(ctx context.Context, keyHash hash.Hash, key types.Tuple) (*doltKVP, bool, error)

	// Commit applies the in memory edits to the list of committed in memory edits
	Commit(ctx context.Context, nbf *types.NomsBinFormat) error

	// Rollback rolls back in memory edits until it reaches the state represented by the savedTea
	Rollback(ctx context.Context) error

	// MaterializeEdits commits and applies the in memory edits to the row data
	MaterializeEdits(ctx context.Context, nbf *types.NomsBinFormat) (m types.Map, err error)
}

const flushThreshold = 256 * 1024

// inMemModifications represent row adds and deletes that have not been written to the underlying storage and only exist
// in memory
type inMemModifications struct {
	ops     int64
	adds    map[hash.Hash]*doltKVP
	deletes map[hash.Hash]types.Tuple
}

// NewInMemModifications returns a pointer to a newly created inMemModifications object
func NewInMemModifications() *inMemModifications {
	return &inMemModifications{
		adds:    make(map[hash.Hash]*doltKVP),
		deletes: make(map[hash.Hash]types.Tuple),
	}
}

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
// for the uncommitted changes to become so large that they need to be flushed to disk.  When this happens we track the
// in memory changes in uncommittedFlushed. When materializing the map, edits must be applied to the row data in order
// so the types.EditAccumulators are numbered so their changes can be applied in order.
type tableEditAccumulatorImpl struct {
	nbf *types.NomsBinFormat

	// initial state of the map
	rowData types.Map

	// in memory changes which will be applied to the
	committed          *inMemModifications
	uncommitted        *inMemModifications
	uncommittedFlushed *inMemModifications

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
}

// Get returns a *doltKVP if the current TableEditAccumulator contains the given key, or it exists in the row data.
// This assumes that the given hash is for the given key.
func (tea *tableEditAccumulatorImpl) Get(ctx context.Context, keyHash hash.Hash, key types.Tuple) (*doltKVP, bool, error) {
	// in order of most recent changes to least recent falling back to whats in the materialized row data
	orderedMods := []*inMemModifications{tea.uncommitted, tea.uncommittedFlushed, tea.committed}
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

func (tea *tableEditAccumulatorImpl) flushUncommitted() {
	ea := edits.NewAsyncSortedEditsWithDefaults(tea.nbf)
	for _, kvp := range tea.uncommitted.adds {
		ea.AddEdit(kvp.k, kvp.v)
	}

	for _, key := range tea.uncommitted.deletes {
		ea.AddEdit(key, nil)
	}

	tea.uncommittedEaIds.Add(tea.accumulatorIdx)
	tea.flusher.Flush(ea, tea.accumulatorIdx)
	tea.accumulatorIdx++

	tea.uncommittedFlushed.MergeIn(tea.uncommitted)
	tea.uncommitted = NewInMemModifications()
}

// Delete adds a row to be deleted when these edits are eventually applied. Updates are modeled as a delete and an insert
func (tea *tableEditAccumulatorImpl) Delete(keyHash hash.Hash, key types.Tuple) {
	delete(tea.uncommitted.adds, keyHash)
	tea.uncommitted.deletes[keyHash] = key
	tea.uncommitted.ops++

	if tea.uncommitted.ops > flushThreshold {
		tea.flushUncommitted()
	}
}

// Insert adds a row to be inserted when these edits are eventually applied. Updates are modeled as a delete and an insert.
func (tea *tableEditAccumulatorImpl) Insert(keyHash hash.Hash, key types.Tuple, val types.Tuple) {
	delete(tea.uncommitted.deletes, keyHash)
	tea.uncommitted.adds[keyHash] = &doltKVP{k: key, v: val}
	tea.uncommitted.ops++
	if tea.uncommitted.ops > flushThreshold {
		tea.flushUncommitted()
	}
}

// Commit applies the in memory edits to the list of committed in memory edits
func (tea *tableEditAccumulatorImpl) Commit(ctx context.Context, nbf *types.NomsBinFormat) error {
	if tea.uncommittedFlushed.ops > 0 {
		if tea.committed.ops > 0 {
			// if there are uncommitted flushed changes we need to flush the committed changes first
			// so they can be applied before the uncommitted flushed changes and future changes can be applied after
			tea.committedEaIds.Add(tea.commitEAId)
			tea.flusher.Flush(tea.commitEA, tea.commitEAId)

			// initialize next types.EditAccumulator to handle future edits
			tea.commitEA = types.CreateEditAccForMapEdits(nbf)
			tea.commitEAId = tea.accumulatorIdx
			tea.accumulatorIdx++
		}

		// add uncommitted types.EditAccumulators which hold flushed uncommitted edits to the list of types.EditAccumulators
		// that have been committed
		tea.committedEaIds.Add(tea.uncommittedEaIds.AsSlice()...)
		tea.uncommittedEaIds = set.NewUint64Set(nil)

		// apply uncommitted flushed inmemory edits to the committed in memory edits
		tea.committed.MergeIn(tea.uncommittedFlushed)

		// initialize empty inMemoryModifications to handle future uncommitted flushed edits
		tea.uncommittedFlushed = NewInMemModifications()
	}

	if tea.uncommitted.ops > 0 {
		// if there are uncommitted changes add them to the committed list of map edits
		for _, kvp := range tea.uncommitted.adds {
			tea.commitEA.AddEdit(kvp.k, kvp.v)
		}

		for _, key := range tea.uncommitted.deletes {
			tea.commitEA.AddEdit(key, nil)
		}

		// apply in memory uncommitted changes to the committed in memory edits
		tea.committed.MergeIn(tea.uncommitted)

		// initialize uncommitted to future in memory edits
		tea.uncommitted = NewInMemModifications()
	}

	return nil
}

// Rollback rolls back in memory edits until it reaches the state represented by the savedTea
func (tea *tableEditAccumulatorImpl) Rollback(ctx context.Context) error {
	// drop uncommitted ea IDs
	tea.uncommittedEaIds = set.NewUint64Set(nil)

	// clear all in memory modifications
	if tea.uncommittedFlushed.ops > 0 {
		tea.uncommittedFlushed = NewInMemModifications()
	}

	if tea.uncommitted.ops > 0 {
		tea.uncommitted = NewInMemModifications()
	}

	return nil
}

func (tea *tableEditAccumulatorImpl) MaterializeEdits(ctx context.Context, nbf *types.NomsBinFormat) (m types.Map, err error) {
	err = tea.Commit(ctx, nbf)
	if err != nil {
		return types.EmptyMap, err
	}

	if tea.committed.ops == 0 {
		return tea.rowData, nil
	}

	committedEP, err := tea.commitEA.FinishedEditing()
	tea.commitEA = nil
	if err != nil {
		return types.EmptyMap, err
	}

	flushedEPs, err := tea.flusher.WaitForIDs(ctx, tea.committedEaIds)
	if err != nil {
		return types.EmptyMap, err
	}

	eps := make([]types.EditProvider, 0, len(flushedEPs)+1)
	eps = append(eps, committedEP)
	for i := 0; i < len(flushedEPs); i++ {
		eps = append(eps, flushedEPs[i].Edits)
	}

	defer func() {
		for _, ep := range eps {
			_ = ep.Close(ctx)
		}
	}()

	accEdits, err := edits.NewEPMerger(ctx, nbf, eps)
	if err != nil {
		return types.EmptyMap, err
	}

	// We are guaranteed that rowData is valid, as we process teas sequentially.
	updatedMap, _, err := types.ApplyEdits(ctx, accEdits, tea.rowData)
	if err != nil {
		return types.EmptyMap, err
	}

	tea.rowData = updatedMap
	tea.committed = NewInMemModifications()
	tea.commitEAId = tea.accumulatorIdx
	tea.accumulatorIdx++
	tea.commitEA = edits.NewAsyncSortedEditsWithDefaults(tea.nbf)
	tea.committedEaIds = set.NewUint64Set(nil)
	tea.uncommittedEaIds = set.NewUint64Set(nil)

	return updatedMap, nil
}

type TEAFactory interface {
	NewTEA(ctx context.Context, rowData types.Map) TableEditAccumulator
}

type teaFactoryImpl struct {
	directory string
	vrw       types.ValueReadWriter
}

func (teaf *teaFactoryImpl) NewTEA(ctx context.Context, rowData types.Map) TableEditAccumulator {
	return &tableEditAccumulatorImpl{
		nbf:                rowData.Format(),
		rowData:            rowData,
		committed:          NewInMemModifications(),
		uncommitted:        NewInMemModifications(),
		uncommittedFlushed: NewInMemModifications(),
		commitEA:           edits.NewAsyncSortedEditsWithDefaults(rowData.Format()),
		commitEAId:         0,
		accumulatorIdx:     1,
		flusher:            edits.NewDiskEditFlusher(ctx, teaf.directory, rowData.Format(), teaf.vrw),
		committedEaIds:     set.NewUint64Set(nil),
		uncommittedEaIds:   set.NewUint64Set(nil),
	}
}
