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

package dtables

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/val"
)

// ProllyRowConverter can be used to convert key, value val.Tuple's from |inSchema|
// to |outSchema|. Columns are matched based on names and primary key
// membership. The output of the conversion process is a sql.Row.
type ProllyRowConverter struct {
	inSchema         schema.Schema
	outSchema        schema.Schema
	keyProj, valProj val.OrdinalMapping
	keyDesc          val.TupleDesc
	valDesc          val.TupleDesc
	pkTargetTypes    []sql.Type
	nonPkTargetTypes []sql.Type
	warnFn           rowconv.WarnFunction
}

func NewProllyRowConverter(inSch, outSch schema.Schema, warnFn rowconv.WarnFunction) (ProllyRowConverter, error) {
	keyProj, valProj, err := diff.MapSchemaBasedOnName(inSch, outSch)
	if err != nil {
		return ProllyRowConverter{}, err
	}

	pkTargetTypes := make([]sql.Type, inSch.GetPKCols().Size())
	nonPkTargetTypes := make([]sql.Type, inSch.GetNonPKCols().Size())

	// Populate pkTargetTypes and nonPkTargetTypes with non-nil sql.Type if we need to do a type conversion
	for i, j := range keyProj {
		if j == -1 {
			continue
		}
		inColType := inSch.GetPKCols().GetByIndex(i).TypeInfo.ToSqlType()
		outColType := outSch.GetAllCols().GetByIndex(j).TypeInfo.ToSqlType()
		if !inColType.Equals(outColType) {
			pkTargetTypes[i] = outColType
		}
	}

	for i, j := range valProj {
		if j == -1 {
			continue
		}
		inColType := inSch.GetNonPKCols().GetByIndex(i).TypeInfo.ToSqlType()
		outColType := outSch.GetAllCols().GetByIndex(j).TypeInfo.ToSqlType()
		if !inColType.Equals(outColType) {
			nonPkTargetTypes[i] = outColType
		}
	}

	kd, vd := shim.MapDescriptorsFromSchema(inSch)
	return ProllyRowConverter{
		inSchema:         inSch,
		outSchema:        outSch,
		keyProj:          keyProj,
		valProj:          valProj,
		keyDesc:          kd,
		valDesc:          vd,
		pkTargetTypes:    pkTargetTypes,
		nonPkTargetTypes: nonPkTargetTypes,
		warnFn:           warnFn,
	}, nil
}

// PutConverted converts the |key| and |value| val.Tuple from |inSchema| to |outSchema|
// and places the converted row in |dstRow|.
func (c ProllyRowConverter) PutConverted(key, value val.Tuple, dstRow []interface{}) error {
	err := c.putFields(key, c.keyProj, c.keyDesc, c.pkTargetTypes, dstRow)
	if err != nil {
		return err
	}

	err = c.putFields(value, c.valProj, c.valDesc, c.nonPkTargetTypes, dstRow)
	if err != nil {
		return err
	}

	return nil
}

func (c ProllyRowConverter) putFields(tup val.Tuple, proj val.OrdinalMapping, desc val.TupleDesc, targetTypes []sql.Type, dstRow []interface{}) error {
	for i, j := range proj {
		if j == -1 {
			continue
		}
		f, err := index.GetField(desc, i, tup)
		if err != nil {
			return err
		}
		if t := targetTypes[i]; t != nil {
			dstRow[j], err = t.Convert(f)
			if sql.ErrInvalidValue.Is(err) && c.warnFn != nil {
				col := c.inSchema.GetAllCols().GetByIndex(i)
				c.warnFn(rowconv.DatatypeCoercionFailureWarningCode, rowconv.DatatypeCoercionFailureWarning, col.Name)
				dstRow[j] = nil
				err = nil
			}
			if err != nil {
				return err
			}
		} else {
			dstRow[j] = f
		}
	}
	return nil
}
