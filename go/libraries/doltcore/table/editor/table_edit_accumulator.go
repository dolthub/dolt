package editor

import (
	"context"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

type doltKVP struct {
	k types.Tuple
	v types.Tuple
}

type tableEditAccumulator interface {
	// OpCount returns the number of operations have been applied to the edit accumulator
	OpCount() int64

	// Delete adds a row to be deleted when these edits are eventually applied. Updates are modeled as a delete and an insert
	Delete(keyHash hash.Hash, key types.Tuple)

	// Insert adds a row to be inserted when these edits are eventually applied. Updates are modeled as a delete and an insert.
	Insert(keyHash hash.Hash, key types.Tuple, val types.Tuple)

	// MaybeGet returns a *doltKVP if the current tableEditAccumulator contains the given key, or it exists in the row data.
	// This assumes that the given hash is for the given key.
	MaybeGet(ctx context.Context, keyHash hash.Hash, key types.Tuple) (*doltKVP, bool, error)

	// Commit applies the in memory edits to the list of committed in memory edits
	Commit() (tableEditAccumulator, error)

	// Rollback rolls back in memory edits until it reaches the state represented by the savedTea
	Rollback(savedTea tableEditAccumulator) (tableEditAccumulator, error)

	// ProcessEdits commits and applies the in memory edits to the row data
	ProcessEdits(ctx context.Context, nbf *types.NomsBinFormat) (m types.Map, err error)
}

type tableEditAccumulatorImpl struct {
	nbf *types.NomsBinFormat

	// prevTea contains the last committed tea
	prevTea *tableEditAccumulatorImpl

	// last materialized types.Map with edits applied
	rowData types.Map

	// rowDataEA contains edits that have been committed
	rowDataEA types.EditAccumulator

	// opCount contains the number of edits that would be applied in materializing the edits
	opCount     int64
	addedKeys   map[hash.Hash]*doltKVP
	removedKeys map[hash.Hash]types.LesserValuable
}

// MaybeGet returns a *doltKVP if the current tableEditAccumulator contains the given key, or it exists in the row data.
// This assumes that the given hash is for the given key.
func (tea *tableEditAccumulatorImpl) MaybeGet(ctx context.Context, keyHash hash.Hash, key types.Tuple) (*doltKVP, bool, error) {
	// No locks as all calls and modifications to tea are done from a lock that the caller handles
	if kvp, ok := tea.addedKeys[keyHash]; ok {
		return kvp, true, nil
	}
	if _, ok := tea.removedKeys[keyHash]; !ok {
		// When rowData is updated, prevTea is set to nil. Therefore, if prevTea is non-nil, we use it.
		if tea.prevTea != nil {
			return tea.prevTea.MaybeGet(ctx, keyHash, key)
		} else {
			keyVal, err := key.Value(ctx)
			if err != nil {
				return nil, false, err
			}

			keyTup := keyVal.(types.Tuple)
			v, ok, err := tea.rowData.MaybeGetTuple(ctx, keyTup)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, nil
			}

			return &doltKVP{k: keyTup, v: v}, true, err
		}
	}
	return nil, false, nil
}


// Delete adds a row to be deleted when these edits are eventually applied. Updates are modeled as a delete and an insert
func (tea *tableEditAccumulatorImpl) Delete(keyHash hash.Hash, key types.Tuple) {
	delete(tea.addedKeys, keyHash)
	tea.removedKeys[keyHash] = key
	tea.opCount++
}

// Insert adds a row to be inserted when these edits are eventually applied. Updates are modeled as a delete and an insert.
func (tea *tableEditAccumulatorImpl) Insert(keyHash hash.Hash, key types.Tuple, val types.Tuple) {
	delete(tea.removedKeys, keyHash)
	tea.addedKeys[keyHash] = &doltKVP{k: key, v: val}
	tea.opCount++
}

// Commit applies the in memory edits to the list of committed in memory edits
func (tea *tableEditAccumulatorImpl) Commit() (tableEditAccumulator, error) {
	targetTea := tea

	// We collapse the changes in this tea to the last to reduce the number of map editors that will need to be opened
	if tea.prevTea != nil {
		targetTea = tea.prevTea
		if targetTea.rowDataEA == nil {
			targetTea.rowDataEA = types.CreateEditAccForMapEdits(targetTea.nbf)
		}

		for keyHash, key := range tea.removedKeys {
			delete(targetTea.addedKeys, keyHash)
			targetTea.removedKeys[keyHash] = key
			targetTea.rowDataEA.AddEdit(key, nil)
		}
		for keyHash, kvp := range tea.addedKeys {
			delete(targetTea.removedKeys, keyHash)
			targetTea.addedKeys[keyHash] = kvp
			targetTea.rowDataEA.AddEdit(kvp.k, kvp.v)
		}

		targetTea.opCount = tea.opCount
		// An opCount of -1 lets us know that this tea was processed
		tea.opCount = -1
		tea.rowData = types.EmptyMap
		tea.prevTea = nil
		tea.addedKeys = nil
		tea.removedKeys = nil
	} else {
		if targetTea.rowDataEA == nil {
			targetTea.rowDataEA = types.CreateEditAccForMapEdits(targetTea.nbf)
		}

		for _, key := range tea.removedKeys {
			tea.rowDataEA.AddEdit(key, nil)
		}

		for _, kvp := range tea.addedKeys {
			tea.rowDataEA.AddEdit(kvp.k, kvp.v)
		}
	}

	return targetTea, nil
}

// Rollback rolls back in memory edits until it reaches the state represented by the savedTea
func (tea *tableEditAccumulatorImpl) Rollback(savedTea tableEditAccumulator) (tableEditAccumulator, error) {
	savedTeaImpl := savedTea.(*tableEditAccumulatorImpl)
	currentTea := tea

	// Loop and remove all newer teas
	for {
		if currentTea == nil || currentTea == savedTeaImpl {
			break
		}
		prevTea := currentTea.prevTea
		// We're essentially deleting currentTea, so we're closing and removing everything.
		// Some of this is taken from the steps followed when flushing, such as the map nils.
		currentTea.prevTea = nil
		currentTea.rowData = types.EmptyMap
		currentTea.addedKeys = nil
		currentTea.removedKeys = nil
		currentTea = prevTea
	}

	// If the savedTea was processed create a new one.
	if savedTea.OpCount() == -1 {
		return (&teaFactoryImpl{tea.nbf}).NewTEA(savedTeaImpl.rowData), nil
	} else {
		return savedTea, nil
	}
}

func (tea *tableEditAccumulatorImpl) ProcessEdits(ctx context.Context, nbf *types.NomsBinFormat) (m types.Map, err error) {
	if tea.OpCount() < 1 {
		return tea.rowData, nil
	}

	currTea, err := tea.Commit()
	if err != nil {
		return types.EmptyMap, err
	}

	currImpl := currTea.(*tableEditAccumulatorImpl)
	defer func() {
		currImpl.rowDataEA.Close()
		currImpl.rowDataEA = nil
	}()

	// If we encounter an error and return, then we need to remove this tea from the chain and update the next's rowData
	encounteredErr := true
	defer func() {
		//TODO: need some way to reset an index editor to a previous point as well
		if encounteredErr {
			// As this is in a defer and we're attempting to capture all errors, that includes panics as well.
			// Naturally a panic doesn't set the err variable, so we have to recover it.
			if recoveredErr := recover(); recoveredErr != nil && err == nil {
				err = recoveredErr.(error)
			}
			// All tea modifications are guarded by writeMutex locks, so we have to acquire it
			tea.prevTea = nil
			tea.rowData = currImpl.rowData
		}
	}()

	accEdits, err := currImpl.rowDataEA.FinishedEditing()
	if err != nil {
		return types.EmptyMap, err
	}

	// We are guaranteed that rowData is valid, as we process teas sequentially.
	updatedMap, _, err := types.ApplyEdits(ctx, accEdits, currImpl.rowData)
	if err != nil {
		return types.EmptyMap, err
	}

	encounteredErr = false

	tea.prevTea = nil
	tea.rowData = updatedMap

	tea.opCount = 0
	tea.addedKeys = make(map[hash.Hash]*doltKVP)
	tea.removedKeys = make(map[hash.Hash]types.LesserValuable)

	return updatedMap, nil
}

// OpCount returns the number of operations have been applied to the edit accumulator
func (tea *tableEditAccumulatorImpl) OpCount() int64 {
	return tea.opCount
}

type teaFactory interface {
	NewTEA(rowData types.Map) tableEditAccumulator
	TEAFromCurrent(tea tableEditAccumulator, opCount int64) tableEditAccumulator
}

type teaFactoryImpl struct {
	nbf *types.NomsBinFormat
}

func (teaf *teaFactoryImpl) NewTEA(rowData types.Map) tableEditAccumulator {
	return &tableEditAccumulatorImpl {
		nbf:         teaf.nbf,
		prevTea:     nil,
		rowData:     rowData,
		addedKeys:   make(map[hash.Hash]*doltKVP),
		removedKeys: make(map[hash.Hash]types.LesserValuable),
	}
}

// TEAFromCurrent returns a new tableEditAccumulator that references the current tableEditAccumulator.
func (teaf *teaFactoryImpl) TEAFromCurrent(tea tableEditAccumulator, opCount int64) tableEditAccumulator {
	return &tableEditAccumulatorImpl {
		nbf:         teaf.nbf,
		prevTea:     tea.(*tableEditAccumulatorImpl),
		rowData:     types.EmptyMap,
		opCount:     opCount,
		addedKeys:   make(map[hash.Hash]*doltKVP),
		removedKeys: make(map[hash.Hash]types.LesserValuable),
	}
}