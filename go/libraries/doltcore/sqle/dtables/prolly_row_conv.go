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
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly/tree"
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
	ns               tree.NodeStore
}

func NewProllyRowConverter(inSch, outSch schema.Schema, warnFn rowconv.WarnFunction, ns tree.NodeStore) (ProllyRowConverter, error) {
	keyProj, valProj, err := schema.MapSchemaBasedOnTagAndName(inSch, outSch)
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
		outColType := outSch.GetPKCols().GetByIndex(j).TypeInfo.ToSqlType()
		if !inColType.Equals(outColType) {
			pkTargetTypes[i] = outColType
		}
		// translate tuple offset to row placement
		t := outSch.GetPKCols().GetByIndex(j).Tag
		keyProj[i] = outSch.GetAllCols().TagToIdx[t]
	}

	for i, j := range valProj {
		if j == -1 {
			continue
		}
		inColType := inSch.GetNonPKCols().GetByIndex(i).TypeInfo.ToSqlType()
		outColType := outSch.GetNonPKCols().GetByIndex(j).TypeInfo.ToSqlType()
		if !inColType.Equals(outColType) {
			nonPkTargetTypes[i] = outColType
		}

		// translate tuple offset to row placement
		t := outSch.GetNonPKCols().GetByIndex(j).Tag
		valProj[i] = outSch.GetAllCols().TagToIdx[t]
	}

	if len(valProj) != 0 && schema.IsKeyless(inSch) {
		// Adjust for cardinality
		valProj = append(val.OrdinalMapping{-1}, valProj...)
		nonPkTargetTypes = append([]sql.Type{nil}, nonPkTargetTypes...)
	}

	kd, vd := inSch.GetMapDescriptors()
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
		ns:               ns,
	}, nil
}

// PutConverted converts the |key| and |value| val.Tuple from |inSchema| to |outSchema|
// and places the converted row in |dstRow|.
func (c ProllyRowConverter) PutConverted(ctx context.Context, key, value val.Tuple, dstRow sql.Row) error {
	err := c.putFields(ctx, key, c.keyProj, c.keyDesc, c.pkTargetTypes, dstRow, true)
	if err != nil {
		return err
	}

	return c.putFields(ctx, value, c.valProj, c.valDesc, c.nonPkTargetTypes, dstRow, false)
}

func (c ProllyRowConverter) putFields(ctx context.Context, tup val.Tuple, proj val.OrdinalMapping, desc val.TupleDesc, targetTypes []sql.Type, dstRow sql.Row, isPk bool) error {
	virtualOffset := 0
	for i, j := range proj {
		if j == -1 {
			nonPkCols := c.inSchema.GetNonPKCols()
			if len(nonPkCols.GetColumns()) > i {
				// Skip over virtual columns in non-pk cols as they are not stored
				if !isPk && nonPkCols.GetByIndex(i).Virtual {
					virtualOffset++
				}
			}

			continue
		}

		f, err := tree.GetField(ctx, desc, i-virtualOffset, tup, c.ns)
		if err != nil {
			return err
		}
		if t := targetTypes[i]; t != nil {
			var inRange sql.ConvertInRange
			dstRow[j], inRange, err = t.Convert(f)
			if sql.ErrInvalidValue.Is(err) && c.warnFn != nil {
				col := c.inSchema.GetAllCols().GetByIndex(i)
				c.warnFn(rowconv.DatatypeCoercionFailureWarningCode, rowconv.DatatypeCoercionFailureWarning, col.Name)
				dstRow[j] = nil
				err = nil
			} else if !inRange {
				c.warnFn(rowconv.TruncatedOutOfRangeValueWarningCode, rowconv.TruncatedOutOfRangeValueWarning, t, f)
				dstRow[j] = nil
			} else if err != nil {
				return err
			}
		} else {
			dstRow[j] = f
		}
	}
	return nil
}
