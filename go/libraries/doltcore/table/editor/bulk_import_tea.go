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
	"errors"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/types/edits"
)

type capacityMonitor interface {
	capacityExceeded(size int) bool
}

type uncapped struct{}

func (cap uncapped) capacityExceeded(size int) bool {
	return false
}

type fixedCapacity struct {
	capacity int64
	currSize int64
}

func (cap *fixedCapacity) capacityExceeded(size int) bool {
	cap.currSize += int64(size)
	return cap.currSize > cap.capacity
}

var _ TableEditAccumulator = (*BulkImportTEA)(nil)

// BulkImportTEA is a TableEditAccumulator implementation used to improve the perf of bulk edits.  It does not implement
// commit and rollback
type BulkImportTEA struct {
	teaf       DbEaFactory
	capMon     capacityMonitor
	emptyTuple types.Tuple

	ea      types.EditAccumulator
	rowData types.Map

	// opCount contains the number of edits that would be applied in materializing the edits
	opCount int64
	adds    map[hash.Hash]bool
	deletes map[hash.Hash]bool
}

// Delete adds a row to be deleted when these edits are eventually applied. Updates are modeled as a delete and an insert
func (tea *BulkImportTEA) Delete(keyHash hash.Hash, key types.Tuple) error {
	// key is stored in the tea.ea, hash is stored in tea.deletes. Capacity is just an estimate and gets off if a
	//	// key is added and/or deleted more than once.
	size := key.Size() + hash.ByteLen
	if tea.capMon.capacityExceeded(size) {
		return errors.New("capacity exceeded")
	}

	tea.opCount++
	tea.ea.AddEdit(key, nil)

	tea.deletes[keyHash] = true
	delete(tea.adds, keyHash)
	return nil
}

// Insert adds a row to be inserted when these edits are eventually applied. Updates are modeled as a delete and an insert.
func (tea *BulkImportTEA) Insert(keyHash hash.Hash, key types.Tuple, val types.Tuple) error {
	// key and val are stored in the tea.ea, hash is stored in tea.adds. Capacity is just an estimate and gets off if a
	// key is added and/or deleted more than once.
	size := key.Size() + val.Size() + hash.ByteLen
	if tea.capMon.capacityExceeded(size) {
		return errors.New("capacity exceeded")
	}

	tea.opCount++
	tea.ea.AddEdit(key, val)

	tea.adds[keyHash] = true
	delete(tea.deletes, keyHash)
	return nil
}

// Get returns a *doltKVP if the current TableEditAccumulator contains the given key, or it exists in the row data.
// This assumes that the given hash is for the given key.
func (tea *BulkImportTEA) Get(ctx context.Context, keyHash hash.Hash, key types.Tuple) (*doltKVP, bool, error) {
	if tea.deletes[keyHash] {
		return nil, false, nil
	}

	if tea.adds[keyHash] {
		return &doltKVP{k: key, v: tea.emptyTuple}, true, nil
	}

	v, ok, err := tea.rowData.MaybeGetTuple(ctx, key)

	if err != nil {
		return nil, false, err
	}

	if !ok {
		return nil, false, nil
	}

	return &doltKVP{k: key, v: v}, true, nil
}

// Commit is the default behavior and does nothing
func (tea *BulkImportTEA) Commit(ctx context.Context, nbf *types.NomsBinFormat) error {
	return nil
}

// Rollback operation not supported on BulkImportTEA
func (tea *BulkImportTEA) Rollback(ctx context.Context) error {
	return errors.New("not supported")
}

// MaterializeEdits applies the in memory edits to the row data and returns types.Map
func (tea *BulkImportTEA) MaterializeEdits(ctx context.Context, nbf *types.NomsBinFormat) (m types.Map, err error) {
	ea := tea.ea
	defer ea.Close(ctx)

	itr, err := ea.FinishedEditing()
	if err != nil {
		return types.EmptyMap, err
	}

	currMap := tea.rowData
	for !itr.ReachedEOF() {
		currMap, _, err = types.ApplyNEdits(ctx, itr, currMap, 256*1024)
		if err != nil {
			return types.EmptyMap, err
		}
	}

	*tea = *(tea.teaf.NewTableEA(ctx, currMap).(*BulkImportTEA))
	return currMap, nil
}

var _ IndexEditAccumulator = (*BulkImportIEA)(nil)

// BulkImportIEA is a IndexEditAccumulator implementation used to improve the perf of bulk edits.  It does not implement
// commit and rollback
type BulkImportIEA struct {
	teaf       DbEaFactory
	capMon     capacityMonitor
	emptyTuple types.Tuple

	ea      types.EditAccumulator
	rowData types.Map

	// opCount contains the number of edits that would be applied in materializing the edits
	opCount     int64
	adds        map[hash.Hash]struct{}
	deletes     map[hash.Hash]struct{}
	partialAdds map[hash.Hash]hashedTuple
}

// Delete adds a row to be deleted when these edits are eventually applied.
func (iea *BulkImportIEA) Delete(ctx context.Context, keyHash, partialKeyHash hash.Hash, key, value types.Tuple) error {
	// key is stored in iea.ea, keyHash is stored in iea.deletes.  Capacity is just an estimate and gets off if a key is added and/or deleted more than once.
	if iea.capMon.capacityExceeded(key.Size()) {
		return errors.New("capacity exceeded")
	}

	iea.opCount++
	iea.ea.AddEdit(key, nil)

	iea.deletes[keyHash] = struct{}{}
	delete(iea.adds, keyHash)
	delete(iea.partialAdds, keyHash)

	return nil
}

// Insert adds a row to be inserted when these edits are eventually applied.
func (iea *BulkImportIEA) Insert(ctx context.Context, keyHash, partialKeyHash hash.Hash, key, val types.Tuple) error {
	// key and val are stored in the iea.ea, keyHash is stored in iea.adds, and iea.partialAdds. partialKeyHash is stored in iea.partialAdds[keyHash].
	// Capacity is just an estimate and gets off if a key is added and/or deleted more than once.
	size := key.Size() + val.Size() + (3 * hash.ByteLen)
	if iea.capMon.capacityExceeded(size) {
		return errors.New("capacity exceeded")
	}

	iea.opCount++
	iea.ea.AddEdit(key, val)

	iea.adds[keyHash] = struct{}{}
	delete(iea.deletes, keyHash)

	if _, ok := iea.partialAdds[partialKeyHash]; !ok {
		iea.partialAdds[partialKeyHash] = hashedTuple{key, iea.emptyTuple, keyHash}
	}

	return nil
}

// Has returns true if the current TableEditAccumulator contains the given key, or it exists in the row data.
func (iea *BulkImportIEA) Has(ctx context.Context, keyHash hash.Hash, key types.Tuple) (bool, error) {
	if _, ok := iea.deletes[keyHash]; ok {
		return false, nil
	}

	if _, ok := iea.adds[keyHash]; ok {
		return true, nil
	}

	ok, err := iea.rowData.Has(ctx, key)

	if err != nil {
		return false, err
	} else if !ok {
		return false, nil
	}

	return true, nil
}

// HasPartial returns true if the current TableEditAccumulator contains the given partialKey
func (iea *BulkImportIEA) HasPartial(ctx context.Context, idxSch schema.Schema, partialKeyHash hash.Hash, partialKey types.Tuple) ([]hashedTuple, error) {
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

	for i := len(matches) - 1; i >= 0; i-- {
		// If we've removed a key that's present here, remove it from the slice
		if _, ok := iea.deletes[matches[i].hash]; ok {
			matches[i] = matches[len(matches)-1]
			matches = matches[:len(matches)-1]
		}
	}
	match, ok := iea.partialAdds[partialKeyHash]
	if ok {
		matches = append(matches, match)
	}
	return matches, nil
}

// Commit is the default behavior and does nothing
func (iea *BulkImportIEA) Commit(ctx context.Context, nbf *types.NomsBinFormat) error {
	return nil
}

// Rollback operation not supported on BulkImportIEA
func (iea *BulkImportIEA) Rollback(ctx context.Context) error {
	return errors.New("not supported")
}

// MaterializeEdits commits and applies the in memory edits to the row data
func (iea *BulkImportIEA) MaterializeEdits(ctx context.Context, nbf *types.NomsBinFormat) (m types.Map, err error) {
	ea := iea.ea
	defer ea.Close(ctx)

	itr, err := ea.FinishedEditing()
	if err != nil {
		return types.EmptyMap, err
	}

	currMap := iea.rowData
	for !itr.ReachedEOF() {
		currMap, _, err = types.ApplyNEdits(ctx, itr, currMap, 256*1024)
		if err != nil {
			return types.EmptyMap, err
		}
	}

	*iea = *(iea.teaf.NewIndexEA(ctx, currMap).(*BulkImportIEA))
	return currMap, nil
}

var _ DbEaFactory = (*BulkImportTEAFactory)(nil)

type BulkImportTEAFactory struct {
	nbf       *types.NomsBinFormat
	vrw       types.ValueReadWriter
	directory string
}

func NewBulkImportTEAFactory(nbf *types.NomsBinFormat, vrw types.ValueReadWriter, directory string) *BulkImportTEAFactory {
	return &BulkImportTEAFactory{
		nbf:       nbf,
		vrw:       vrw,
		directory: directory,
	}
}

func (b *BulkImportTEAFactory) NewTableEA(ctx context.Context, rowData types.Map) TableEditAccumulator {
	const flushInterval = 256 * 1024

	createMapEA := func() types.EditAccumulator {
		return types.CreateEditAccForMapEdits(b.nbf)
	}

	ea := edits.NewDiskBackedEditAcc(ctx, b.nbf, b.vrw, flushInterval, b.directory, createMapEA)
	return &BulkImportTEA{
		teaf:       b,
		capMon:     uncapped{},
		rowData:    rowData,
		ea:         ea,
		adds:       make(map[hash.Hash]bool),
		deletes:    make(map[hash.Hash]bool),
		emptyTuple: types.EmptyTuple(b.nbf),
	}
}

func (b *BulkImportTEAFactory) NewIndexEA(ctx context.Context, rowData types.Map) IndexEditAccumulator {
	const flushInterval = 256 * 1024

	createMapEA := func() types.EditAccumulator {
		return types.CreateEditAccForMapEdits(b.nbf)
	}

	ea := edits.NewDiskBackedEditAcc(ctx, b.nbf, b.vrw, flushInterval, b.directory, createMapEA)
	return &BulkImportIEA{
		teaf:        b,
		capMon:      uncapped{},
		rowData:     rowData,
		ea:          ea,
		adds:        make(map[hash.Hash]struct{}),
		deletes:     make(map[hash.Hash]struct{}),
		partialAdds: make(map[hash.Hash]hashedTuple),
		emptyTuple:  types.EmptyTuple(b.nbf),
	}
}

var _ DbEaFactory = (*InMemDEAF)(nil)

type InMemDEAF struct {
	nbf    *types.NomsBinFormat
	capMon capacityMonitor
}

func NewInMemDeafWithMaxCapacity(nbf *types.NomsBinFormat, maxCapacity int64) DbEaFactory {
	var capMon capacityMonitor
	if maxCapacity > 0 {
		capMon = &fixedCapacity{capacity: maxCapacity}
	} else {
		capMon = uncapped{}
	}

	return &InMemDEAF{nbf: nbf, capMon: capMon}
}

func NewInMemDeaf(nbf *types.NomsBinFormat) DbEaFactory {
	return NewInMemDeafWithMaxCapacity(nbf, -1)
}

func (i *InMemDEAF) NewTableEA(ctx context.Context, rowData types.Map) TableEditAccumulator {
	ea := edits.NewAsyncSortedEditsWithDefaults(i.nbf)
	return &BulkImportTEA{
		teaf:       i,
		capMon:     i.capMon,
		rowData:    rowData,
		ea:         ea,
		adds:       make(map[hash.Hash]bool),
		deletes:    make(map[hash.Hash]bool),
		emptyTuple: types.EmptyTuple(i.nbf),
	}
}

func (i *InMemDEAF) NewIndexEA(ctx context.Context, rowData types.Map) IndexEditAccumulator {
	ea := edits.NewAsyncSortedEditsWithDefaults(i.nbf)
	return &BulkImportIEA{
		teaf:        i,
		capMon:      i.capMon,
		rowData:     rowData,
		ea:          ea,
		adds:        make(map[hash.Hash]struct{}),
		deletes:     make(map[hash.Hash]struct{}),
		partialAdds: make(map[hash.Hash]hashedTuple),
		emptyTuple:  types.EmptyTuple(i.nbf),
	}
}
