package noms

import (
	"context"
	"errors"
	"fmt"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

type StatsCB func(stats types.AppliedEditStats)

const maxEdits = 256 * 1024

type updateMapRes struct {
	m   types.Map
	err error
}

// NomsMapUpdater is a TableWriter that writes rows to a noms types.Map. Once all rows are written Close() should be
// called and GetMap will then return the new map.
type NomsMapUpdater struct {
	sch schema.Schema
	vrw types.ValueReadWriter

	count int64
	acc   types.EditAccumulator

	mapChan chan types.EditProvider
	resChan chan updateMapRes

	result *updateMapRes
}

// NewNomsMapUpdater creates a new NomsMapUpdater for a given map.
func NewNomsMapUpdater(ctx context.Context, vrw types.ValueReadWriter, m types.Map, sch schema.Schema, statsCB StatsCB) *NomsMapUpdater {
	if sch.GetPKCols().Size() == 0 {
		panic("NomsMapUpdater requires a schema with a primary key.")
	}

	mapChan := make(chan types.EditProvider, 1)
	resChan := make(chan updateMapRes)

	go func() {
		var totalStats types.AppliedEditStats
		for edits := range mapChan {
			var stats types.AppliedEditStats
			// TODO(binformat)
			m, stats = types.ApplyEdits(ctx, types.Format_7_18, edits, m)
			totalStats = totalStats.Add(stats)

			if statsCB != nil {
				statsCB(totalStats)
			}
		}

		resChan <- updateMapRes{m, nil}
	}()

	return &NomsMapUpdater{sch, vrw, 0, types.CreateEditAccForMapEdits(types.Format_7_18), mapChan, resChan, nil}
}

// GetSchema gets the schema of the rows that this writer writes
func (nmu *NomsMapUpdater) GetSchema() schema.Schema {
	return nmu.sch
}

// WriteRow will write a row to a table
func (nmu *NomsMapUpdater) WriteRow(ctx context.Context, r row.Row) error {
	if nmu.acc == nil {
		panic("Attempting to write after closing.")
	}

	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic occured when writing: %v", r)
			}
		}()

		pk := r.NomsMapKey(nmu.sch)
		fieldVals := r.NomsMapValue(nmu.sch)

		nmu.acc.AddEdit(pk, fieldVals)
		nmu.count++

		if nmu.count%maxEdits == 0 {
			nmu.mapChan <- nmu.acc.FinishedEditing()
			nmu.acc = types.CreateEditAccForMapEdits(nmu.vrw.Format())
		}
	}()

	return err
}

// Close should flush all writes, release resources being held
func (nmu *NomsMapUpdater) Close(ctx context.Context) error {
	if nmu.result != nil {
		return errors.New("Already closed.")
	}

	nmu.mapChan <- nmu.acc.FinishedEditing()
	nmu.acc = nil

	close(nmu.mapChan)

	result := <-nmu.resChan
	nmu.result = &result

	if nmu.result.err != nil {
		return nmu.result.err
	}

	return nil
}

// GetMap retrieves the resulting types.Map once close is called
func (nmu *NomsMapUpdater) GetMap() *types.Map {
	return &nmu.result.m
}
