// Copyright 2019 Liquidata, Inc.
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

package noms

import (
	"context"
	"errors"
	"github.com/liquidata-inc/dolt/go/store/atomicerr"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
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
	ae *atomicerr.AtomicError

	result *updateMapRes
}

// NewNomsMapUpdater creates a new NomsMapUpdater for a given map.
func NewNomsMapUpdater(ctx context.Context, vrw types.ValueReadWriter, m types.Map, sch schema.Schema, statsCB StatsCB) *NomsMapUpdater {
	if sch.GetPKCols().Size() == 0 {
		panic("NomsMapUpdater requires a schema with a primary key.")
	}

	ae := atomicerr.New()
	mapChan := make(chan types.EditProvider, 1)
	resChan := make(chan updateMapRes)

	go func() {
		var totalStats types.AppliedEditStats
		for edits := range mapChan {
			if ae.IsSet() {
				continue // drain
			}

			var stats types.AppliedEditStats
			var err error

			m, stats, err = types.ApplyEdits(ctx, edits, m)

			if ae.SetIfError(err) {
				continue
			}

			totalStats = totalStats.Add(stats)

			if statsCB != nil {
				statsCB(totalStats)
			}
		}

		resChan <- updateMapRes{m, nil}
	}()

	return &NomsMapUpdater{sch, vrw, 0, types.CreateEditAccForMapEdits(vrw.Format()), mapChan, resChan, ae, nil}
}

// GetSchema gets the schema of the rows that this writer writes
func (nmu *NomsMapUpdater) GetSchema() schema.Schema {
	return nmu.sch
}

// WriteRow will write a row to a table
func (nmu *NomsMapUpdater) WriteRow(ctx context.Context, r row.Row) error {
	if nmu.acc == nil {
		return errors.New("Attempting to write after closing.")
	}

	if err := nmu.ae.Get(); err != nil {
		return err
	}

	err := func() error {
		pk := r.NomsMapKey(nmu.sch)
		fieldVals := r.NomsMapValue(nmu.sch)

		nmu.acc.AddEdit(pk, fieldVals)
		nmu.count++

		if nmu.count%maxEdits == 0 {
			edits, err := nmu.acc.FinishedEditing()

			if err != nil {
				return err
			}

			nmu.mapChan <- edits
			nmu.acc = types.CreateEditAccForMapEdits(nmu.vrw.Format())
		}

		return nil
	}()

	if err != nil {
		return err
	}

	return nil
}

// Close should flush all writes, release resources being held
func (nmu *NomsMapUpdater) Close(ctx context.Context) error {
	if nmu.result != nil {
		return errors.New("Already closed.")
	}

	edits, err := nmu.acc.FinishedEditing()

	if err != nil {
		return err
	}

	nmu.mapChan <- edits
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
