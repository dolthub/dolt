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
	"fmt"

	"github.com/liquidata-inc/dolt/go/store/atomicerr"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// NomsMapUpdater is a TableWriter that creates a new noms types.Map.  It is backed by a StreamingMap which requires
// the rows to be written in order.  If the keys being written to WriteRow are not sorted an error will be returned from
// WriteRow.  Once all rows are written Close() should be called and GetMap will then return the new map.
type NomsMapCreator struct {
	sch schema.Schema
	vrw types.ValueReadWriter

	lastPK  types.LesserValuable
	kvsChan chan<- types.Value
	mapChan <-chan types.Map
	ae      *atomicerr.AtomicError

	result *types.Map
}

// NewNomsMapCreator creates a new NomsMapCreator.
func NewNomsMapCreator(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema) *NomsMapCreator {
	ae := atomicerr.New()
	kvsChan := make(chan types.Value)
	mapChan := types.NewStreamingMap(ctx, vrw, ae, kvsChan)

	return &NomsMapCreator{sch, vrw, nil, kvsChan, mapChan, ae, nil}
}

// GetSchema gets the schema of the rows that this writer writes
func (nmc *NomsMapCreator) GetSchema() schema.Schema {
	return nmc.sch
}

// WriteRow will write a row to a table.  The primary key for each row must be greater than the primary key of the row
// written before it.
func (nmc *NomsMapCreator) WriteRow(ctx context.Context, r row.Row) error {
	if nmc.kvsChan == nil {
		return errors.New("writing to NomsMapCreator after closing")
	}

	if err := nmc.ae.Get(); err != nil {
		return err
	}

	err := func() error {
		pk := r.NomsMapKey(nmc.sch)
		fieldVals := r.NomsMapValue(nmc.sch)

		isOK := nmc.lastPK == nil
		if !isOK {
			var err error
			isOK, err = nmc.lastPK.Less(nmc.vrw.Format(), pk)

			if err != nil {
				return err
			}
		}

		if isOK {
			pkVal, err := pk.Value(ctx)

			if err != nil {
				return err
			}

			fv, err := fieldVals.Value(ctx)

			if err != nil {
				return err
			}

			nmc.kvsChan <- pkVal
			nmc.kvsChan <- fv
			nmc.lastPK = pk

			return nil
		} else {
			return errors.New("Input was not sorted by the primary key")
		}
	}()

	nmc.ae.SetIfError(err)
	return err
}

// Close should flush all writes, release resources being held.  After this call is made no more rows may be written,
// and the value of GetMap becomes valid.
func (nmc *NomsMapCreator) Close(ctx context.Context) error {
	if nmc.result == nil {
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic occured during closing: %v", r)
				}
			}()

			close(nmc.kvsChan)

			result := <-nmc.mapChan
			nmc.result = &result

			nmc.kvsChan = nil
			nmc.mapChan = nil
		}()

		return err
	} else {
		return errors.New("Already closed.")
	}
}

// GetMap retrieves the resulting types.Map once close is called
func (nmc *NomsMapCreator) GetMap() *types.Map {
	// Might want to panic if this was never closed
	return nmc.result
}
