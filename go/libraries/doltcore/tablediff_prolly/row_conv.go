// Copyright 2022 Dolthub, Inc.
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

package tablediff_prolly

import (
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

// RowConverter can be used to convert key, value val.Tuple's from |inSchema|
// to |outSchema|. Columns are matched based on names and primary key
// membership. The output of the conversion process is a sql.Row.
type RowConverter struct {
	inSchema         schema.Schema
	outSchema        schema.Schema
	keyProj, valProj val.OrdinalMapping
	keyDesc          val.TupleDesc
	valDesc          val.TupleDesc
}

func NewRowConverter(inSch, outSch schema.Schema) (RowConverter, error) {
	keyProj, valProj, err := MapSchemaBasedOnName(inSch, outSch)
	if err != nil {
		return RowConverter{}, err
	}
	kd, vd := prolly.MapDescriptorsFromScheam(inSch)
	return RowConverter{
		inSchema:  inSch,
		outSchema: outSch,
		keyProj:   keyProj,
		valProj:   valProj,
		keyDesc:   kd,
		valDesc:   vd,
	}, nil
}

// PutConverted converts the |key| and |value| val.Tuple from |inSchema| to |outSchema|
// and places the converted row in |dstRow|.
func (c RowConverter) PutConverted(key, value val.Tuple, dstRow []interface{}) error {
	for i, j := range c.keyProj {
		if j == -1 {
			continue
		}
		inColType := c.inSchema.GetPKCols().GetByIndex(i).TypeInfo.ToSqlType()
		outColType := c.outSchema.GetAllCols().GetByIndex(j).TypeInfo.ToSqlType()
		f, err := index.GetField(c.keyDesc, i, key)
		if err != nil {
			return err
		}
		if inColType.Equals(outColType) {
			dstRow[j] = f
		} else {
			dstRow[j], err = outColType.Convert(f)
			if err != nil {
				return err
			}
		}
	}

	for i, j := range c.valProj {
		if j == -1 {
			continue
		}
		inColType := c.inSchema.GetNonPKCols().GetByIndex(i).TypeInfo.ToSqlType()
		outColType := c.outSchema.GetAllCols().GetByIndex(j).TypeInfo.ToSqlType()
		f, err := index.GetField(c.valDesc, i, value)
		if err != nil {
			return err
		}
		if inColType.Equals(outColType) {
			dstRow[j] = f
		} else {
			dstRow[j], err = outColType.Convert(f)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// MapSchemaBasedOnName can be used to map column values from one schema to
// another schema. A column in |inSch| is mapped to |outSch| if they share the
// same name and primary key membership status. It returns ordinal mappings that
// can be use to map key, value val.Tuple's of schema |inSch| to a sql.Row of
// |outSch|. The first ordinal map is for keys, and the second is for values. If
// a column of |inSch| is missing in |outSch| then that column's index in the
// ordinal map holds -1.
func MapSchemaBasedOnName(inSch, outSch schema.Schema) (val.OrdinalMapping, val.OrdinalMapping, error) {
	keyMapping := make(val.OrdinalMapping, inSch.GetPKCols().Size())
	valMapping := make(val.OrdinalMapping, inSch.GetNonPKCols().Size())

	err := inSch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		i := inSch.GetPKCols().TagToIdx[tag]
		if col, ok := outSch.GetPKCols().GetByName(col.Name); ok {
			j := outSch.GetAllCols().TagToIdx[col.Tag]
			keyMapping[i] = j
		} else {
			return true, fmt.Errorf("could not map primary key column %s", col.Name)
		}
		return false, nil
	})
	if err != nil {
		return nil, nil, err
	}

	err = inSch.GetNonPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		i := inSch.GetNonPKCols().TagToIdx[col.Tag]
		if col, ok := outSch.GetNonPKCols().GetByName(col.Name); ok {
			j := outSch.GetAllCols().TagToIdx[col.Tag]
			valMapping[i] = j
		} else {
			valMapping[i] = -1
		}
		return false, nil
	})
	if err != nil {
		return nil, nil, err
	}

	return keyMapping, valMapping, nil
}
