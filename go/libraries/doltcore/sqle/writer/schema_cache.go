// Copyright 2024 Dolthub, Inc.
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

package writer

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/val"
)

// writerSchema returns the state required to map logical sql.Row values to
// primary and secondary val.Tuple that will be written to disk.
func writerSchema(ctx *sql.Context, t *doltdb.Table, tableName string, dbName string) (*dsess.WriterState, error) {
	sess, ok := ctx.Session.(*dsess.DoltSession)
	if !ok {
		return newWriterSchema(ctx, t, tableName, dbName)
	}

	dbState, ok, err := sess.LookupDbState(ctx, dbName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no state for database %s", dbName)
	}

	// cannot use root value schema cache, |t| is not in root value
	schHash, err := t.GetSchemaHash(ctx)
	if err != nil {
		return nil, err
	}

	schKey := doltdb.DataCacheKey{Hash: schHash}
	schState, ok := dbState.SessionCache().GetCachedWriterState(schKey)
	if !ok {
		schState, err = newWriterSchema(ctx, t, tableName, dbName)
		if err != nil {
			return nil, err
		}
		if !schKey.IsEmpty() {
			dbState.SessionCache().CacheWriterState(schKey, schState)
		}
	}
	return schState, nil
}

func newWriterSchema(ctx *sql.Context, t *doltdb.Table, tableName string, dbName string) (*dsess.WriterState, error) {
	var err error
	schState := new(dsess.WriterState)
	schState.DoltSchema, err = t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	schState.PkKeyDesc, schState.PkValDesc = schState.DoltSchema.GetMapDescriptors()
	schState.PkSchema, err = sqlutil.FromDoltSchema(dbName, tableName, schState.DoltSchema)
	if err != nil {
		return nil, err
	}

	schState.AutoIncCol = autoIncrementColFromSchema(schState.DoltSchema)

	keyMap, valMap := ordinalMappingsFromSchema(schState.PkSchema.Schema, schState.DoltSchema)
	schState.PriIndex = dsess.IndexState{KeyMapping: keyMap, ValMapping: valMap}

	definitions := schState.DoltSchema.Indexes().AllIndexes()
	for _, def := range definitions {
		keyMap, valMap := ordinalMappingsFromSchema(schState.PkSchema.Schema, def.Schema())
		pkMap := makeIndexToIndexMapping(def.Schema().GetPKCols(), schState.DoltSchema.GetPKCols())
		idxState := dsess.IndexState{
			KeyMapping:    keyMap,
			ValMapping:    valMap,
			PkMapping:     pkMap,
			Name:          def.Name(),
			Schema:        def.Schema(),
			Count:         def.Count(),
			IsFullText:    def.IsFullText(),
			IsUnique:      def.IsUnique(),
			IsSpatial:     def.IsSpatial(),
			PrefixLengths: def.PrefixLengths(),
		}
		schState.SecIndexes = append(schState.SecIndexes, idxState)
	}
	return schState, nil
}

func ordinalMappingsFromSchema(from sql.Schema, to schema.Schema) (km, vm val.OrdinalMapping) {
	km = makeOrdinalMapping(from, to.GetPKCols())
	vm = makeOrdinalMapping(from, to.GetNonPKCols())
	return
}

func makeOrdinalMapping(from sql.Schema, to *schema.ColCollection) (m val.OrdinalMapping) {
	m = make(val.OrdinalMapping, to.StoredSize())
	for i := range m {
		col := to.GetByStoredIndex(i)
		name := col.Name
		colIdx := from.IndexOfColName(name)
		m[i] = colIdx
	}
	return
}

// NB: only works for primary-key tables/indexes
func makeIndexToIndexMapping(from, to *schema.ColCollection) (m val.OrdinalMapping) {
	m = make(val.OrdinalMapping, len(to.GetColumns()))
	for i, col := range to.GetColumns() {
		m[i] = from.TagToIdx[col.Tag]
	}
	return
}

func autoIncrementColFromSchema(sch schema.Schema) schema.Column {
	var autoCol schema.Column
	_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.AutoIncrement {
			autoCol = col
			stop = true
		}
		return
	})
	return autoCol
}
