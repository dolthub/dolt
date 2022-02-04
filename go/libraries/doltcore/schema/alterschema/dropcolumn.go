// Copyright 2019 Dolthub, Inc.
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
)

var ErrKeylessAltTbl = errors.New("schema alterations not supported for keyless tables")

// DropColumn drops a column from a table, and removes its associated cell values
func DropColumn(ctx context.Context, tbl *doltdb.Table, colName string, foreignKeys []doltdb.ForeignKey) (*doltdb.Table, error) {
	if tbl == nil {
		panic("invalid parameters")
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if schema.IsKeyless(sch) {
		return nil, ErrKeylessAltTbl
	}

	var dropTag uint64
	if col, ok := sch.GetAllCols().GetByName(colName); !ok {
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

	for _, index := range sch.Indexes().IndexesWithColumn(colName) {
		_, err = sch.Indexes().RemoveIndex(index.Name())
		if err != nil {
			return nil, err
		}
		tbl, err = tbl.DeleteIndexRowData(ctx, index.Name())
		if err != nil {
			return nil, err
		}
	}

	cols := make([]schema.Column, 0)
	for _, col := range sch.GetAllCols().GetColumns() {
		if col.Name == colName {
			continue
		}
		cols = append(cols, col)
	}

	colColl := schema.NewColCollection(cols...)
	newSch, err := schema.SchemaFromCols(colColl)
	if err != nil {
		return nil, err
	}
	newSch.Indexes().AddIndex(sch.Indexes().AllIndexes()...)

	// Copy over all checks from the old schema
	for _, check := range sch.Checks().AllChecks() {
		_, err := newSch.Checks().AddCheck(check.Name(), check.Expression(), check.Enforced())
		if err != nil {
			return nil, err
		}
	}

	pkOrds, err := modifyPkOrdinals(sch, newSch)
	if err != nil {
		return nil, err
	}
	err = newSch.SetPkOrdinals(pkOrds)
	if err != nil {
		return nil, err
	}

	return tbl.UpdateSchema(ctx, newSch)
}
