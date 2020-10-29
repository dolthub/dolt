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

package alterschema

import (
	"context"
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
)

// DropColumn drops a column from a table, and removes its associated cell values
func DropColumn(ctx context.Context, tbl *doltdb.Table, colName string, foreignKeys []doltdb.ForeignKey) (*doltdb.Table, error) {
	if tbl == nil {
		panic("invalid parameters")
	}

	tblSch, err := tbl.GetSchema(ctx)

	if err != nil {
		return nil, err
	}

	allCols := tblSch.GetAllCols()

	var dropTag uint64
	if col, ok := allCols.GetByName(colName); !ok {
		return nil, schema.ErrColNotFound
	} else if col.IsPartOfPK {
		return nil, errors.New("Cannot drop column in primary key")
	} else {
		dropTag = col.Tag
	}

	for _, foreignKey := range foreignKeys {
		for _, fkTag := range foreignKey.TableColumns {
			if dropTag == fkTag {
				return nil, fmt.Errorf("cannot drop column `%s` as it is used in foreign key `%d`", colName, dropTag)
			}
		}
		for _, fkTag := range foreignKey.ReferencedTableColumns {
			if dropTag == fkTag {
				return nil, fmt.Errorf("cannot drop column `%s` as it is used in foreign key `%d`", colName, dropTag)
			}
		}
	}

	for _, index := range tblSch.Indexes().IndexesWithColumn(colName) {
		_, err = tblSch.Indexes().RemoveIndex(index.Name())
		if err != nil {
			return nil, err
		}
		tbl, err = tbl.DeleteIndexRowData(ctx, index.Name())
		if err != nil {
			return nil, err
		}
	}

	cols := make([]schema.Column, 0)
	err = allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.Name != colName {
			cols = append(cols, col)
		}
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	colColl, err := schema.NewColCollection(cols...)
	if err != nil {
		return nil, err
	}

	newSch := schema.SchemaFromCols(colColl)
	newSch.Indexes().AddIndex(tblSch.Indexes().AllIndexes()...)

	vrw := tbl.ValueReadWriter()
	schemaVal, err := encoding.MarshalSchemaAsNomsValue(ctx, vrw, newSch)

	if err != nil {
		return nil, err
	}

	rd, err := tbl.GetRowData(ctx)

	indexData, err := tbl.GetIndexData(ctx)
	if err != nil {
		return nil, err
	}
	newTable, err := doltdb.NewTable(ctx, vrw, schemaVal, rd, &indexData)

	if err != nil {
		return nil, err
	}

	return newTable, nil
}
