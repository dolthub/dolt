package noms

import (
	"context"
	"errors"
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
)

// NomsMapUpdater is a TableWriter that writes rows to a noms types.Map. Once all rows are written Close() should be
// called and GetMap will then return the new map.
type NomsMapUpdater struct {
	sch schema.Schema
	vrw types.ValueReadWriter

	me     *types.MapEditor
	result *types.Map
}

// NewNomsMapUpdater creates a new NomsMapUpdater for a given map.
func NewNomsMapUpdater(vrw types.ValueReadWriter, m types.Map, sch schema.Schema) *NomsMapUpdater {
	if sch.GetPKCols().Size() == 0 {
		panic("NomsMapUpdater requires a schema with a primary key.")
	}

	me := m.Edit()

	return &NomsMapUpdater{sch, vrw, me, nil}
}

// GetSchema gets the schema of the rows that this writer writes
func (nmu *NomsMapUpdater) GetSchema() schema.Schema {
	return nmu.sch
}

// WriteRow will write a row to a table
func (nmu *NomsMapUpdater) WriteRow(r row.Row) error {
	if nmu.me == nil {
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

		nmu.me = nmu.me.Set(pk, fieldVals)
	}()

	return err
}

// Close should flush all writes, release resources being held
func (nmu *NomsMapUpdater) Close() error {
	if nmu.result == nil {
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic occured during closing: %v", r)
				}

				result := nmu.me.Map(context.TODO())
				nmu.result = &result

				nmu.me = nil
			}()
		}()

		return err
	} else {
		return errors.New("Already closed.")
	}
}

// GetMap retrieves the resulting types.Map once close is called
func (nmu *NomsMapUpdater) GetMap() *types.Map {
	return nmu.result
}
