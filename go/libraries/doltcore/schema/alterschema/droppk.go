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

package alterschema

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

func DropPrimaryKeyFromTable(ctx context.Context, table *doltdb.Table, nbf *types.NomsBinFormat, opts editor.Options) (*doltdb.Table, error) {
	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if sch.GetPKCols().Size() == 0 {
		return nil, sql.ErrCantDropFieldOrKey.New("PRIMARY")
	}

	// Modify the schema to convert the primary key cols into non primary key cols
	newCollection := schema.MapColCollection(sch.GetAllCols(), func(col schema.Column) schema.Column {
		if col.IsPartOfPK {
			col.Constraints = append(col.Constraints, schema.NotNullConstraint{})
		}
		col.IsPartOfPK = false
		return col
	})

	newSchema, err := schema.SchemaFromCols(newCollection)
	if err != nil {
		return nil, err
	}

	newSchema.Indexes().AddIndex(sch.Indexes().AllIndexes()...)

	// Copy over all checks from the old schema
	for _, check := range sch.Checks().AllChecks() {
		_, err := newSchema.Checks().AddCheck(check.Name(), check.Expression(), check.Enforced())
		if err != nil {
			return nil, err
		}
	}

	table, err = table.UpdateSchema(ctx, newSchema)
	if err != nil {
		return nil, err
	}

	// Convert all of the keyed row data to keyless row data
	rowData, err := table.GetNomsRowData(ctx)
	if err != nil {
		return nil, err
	}

	newRowData, err := keyedRowDataToKeylessRowData(ctx, nbf, table.ValueReadWriter(), rowData, newSchema)
	if err != nil {
		return nil, err
	}

	table, err = table.UpdateNomsRows(ctx, newRowData)
	if err != nil {
		return nil, err
	}

	// Rebuild all of the indexes now that the primary key has been changed
	return editor.RebuildAllIndexes(ctx, table, opts)
}

func keyedRowDataToKeylessRowData(ctx context.Context, nbf *types.NomsBinFormat, vrw types.ValueReadWriter, rowData types.Map, newSch schema.Schema) (types.Map, error) {
	newMap, err := types.NewMap(ctx, vrw)
	if err != nil {
		return types.Map{}, err
	}

	mapEditor := newMap.Edit()

	err = rowData.Iter(ctx, func(key types.Value, value types.Value) (stop bool, err error) {
		taggedVals, err := row.TaggedValuesFromTupleKeyAndValue(key.(types.Tuple), value.(types.Tuple))
		if err != nil {
			return true, err
		}

		keyedRow, err := row.New(nbf, newSch, taggedVals)
		if err != nil {
			return true, nil
		}

		mapEditor = mapEditor.Set(keyedRow.NomsMapKey(newSch), keyedRow.NomsMapValue(newSch))

		return false, nil
	})

	if err != nil {
		return types.Map{}, err
	}

	return mapEditor.Map(ctx)
}
