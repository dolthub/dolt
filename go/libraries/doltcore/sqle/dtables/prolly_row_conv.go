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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// ProllyRowConverter can be used to convert key, value val.Tuple's from |inSchema|
// to |outSchema|. Columns are matched by tag and name, preferring a column with the
// same primary key membership. The output of the conversion process is a sql.Row.
type ProllyRowConverter struct {
	inSchema         schema.Schema
	outSchema        schema.Schema
	ns               tree.NodeStore
	warnFn           rowconv.WarnFunction
	keyDesc          *val.TupleDesc
	valDesc          *val.TupleDesc
	keyProj          val.OrdinalMapping
	valProj          val.OrdinalMapping
	pkTargetTypes    []sql.Type
	nonPkTargetTypes []sql.Type
}

func NewProllyRowConverter(ctx *sql.Context, inSch, outSch schema.Schema, warnFn rowconv.WarnFunction, ns tree.NodeStore) (ProllyRowConverter, error) {
	keyProj, pkTargetTypes, err := projectKeyColumns(inSch, outSch)
	if err != nil {
		return ProllyRowConverter{}, err
	}
	valProj, nonPkTargetTypes := projectNonKeyColumns(inSch, outSch)

	if len(valProj) != 0 && schema.IsKeyless(inSch) {
		// Adjust for cardinality
		valProj = append(val.OrdinalMapping{-1}, valProj...)
		nonPkTargetTypes = append([]sql.Type{nil}, nonPkTargetTypes...)
	}

	kd, vd := inSch.GetMapDescriptors(ns)
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

func (c ProllyRowConverter) putFields(ctx context.Context, tup val.Tuple, proj val.OrdinalMapping, desc *val.TupleDesc, targetTypes []sql.Type, dstRow sql.Row, isPk bool) error {
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
			dstRow[j], inRange, err = t.Convert(ctx, f)
			if (sql.ErrInvalidValue.Is(err) || sql.ErrTruncatedIncorrect.Is(err)) && c.warnFn != nil {
				col := c.inSchema.GetAllCols().GetByIndex(i)
				c.warnFn(rowconv.DatatypeCoercionFailureWarningCode, rowconv.DatatypeCoercionFailureWarning, col.Name)
				dstRow[j] = nil
				err = nil
			} else if inRange != sql.InRange && c.warnFn != nil {
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

// projectKeyColumns maps each primary key column of |inSch| to its position in rows of |outSch|, where it may be a
// non-key column after a primary key change, and returns the type to convert it to when that differs. It matches a key
// column of |outSch| by tag, then by name and type, and then any column of |outSch| by tag, then by name.
func projectKeyColumns(inSch, outSch schema.Schema) (val.OrdinalMapping, []sql.Type, error) {
	inPK := inSch.GetPKCols()
	proj := make(val.OrdinalMapping, inPK.Size())
	targetTypes := make([]sql.Type, inPK.Size())
	if inSch.GetAllCols().Size() == 0 || outSch.GetAllCols().Size() == 0 {
		for i := range proj {
			proj[i] = -1
		}
		return proj, targetTypes, nil
	}
	outPK, outCols := outSch.GetPKCols(), outSch.GetAllCols()
	for i, col := range inPK.GetColumns() {
		inType := col.TypeInfo.ToSqlType()
		outCol, ok := outPK.GetByTag(col.Tag)
		if !ok {
			outCol, ok = outPK.GetByNameCaseInsensitive(col.Name)
			ok = ok && outCol.TypeInfo.ToSqlType().Equals(inType)
		}
		if !ok {
			outCol, ok = outCols.GetByTag(col.Tag)
		}
		if !ok {
			outCol, ok = outCols.GetByName(col.Name)
		}
		if !ok {
			return nil, nil, fmt.Errorf("could not map primary key column %s", col.Name)
		}
		proj[i] = outCols.TagToIdx[outCol.Tag]
		if outType := outCol.TypeInfo.ToSqlType(); !inType.Equals(outType) {
			targetTypes[i] = outType
		}
	}
	return proj, targetTypes, nil
}

// projectNonKeyColumns maps each non-key column of |inSch| to its position in rows of |outSch| by name, where it may
// be a key column after a primary key change, or to -1 when |outSch| lacks it, and returns the type to convert it to
// when that differs.
func projectNonKeyColumns(inSch, outSch schema.Schema) (val.OrdinalMapping, []sql.Type) {
	inNonPK := inSch.GetNonPKCols()
	proj := make(val.OrdinalMapping, inNonPK.Size())
	targetTypes := make([]sql.Type, inNonPK.Size())
	outNonPK, outCols := outSch.GetNonPKCols(), outSch.GetAllCols()
	for i, col := range inNonPK.GetColumns() {
		proj[i] = -1
		if col.Virtual || outCols.Size() == 0 {
			continue
		}
		outCol, ok := outNonPK.GetByName(col.Name)
		if !ok {
			outCol, ok = outSch.GetPKCols().GetByName(col.Name)
		}
		if !ok {
			continue
		}
		proj[i] = outCols.TagToIdx[outCol.Tag]
		if inType, outType := col.TypeInfo.ToSqlType(), outCol.TypeInfo.ToSqlType(); !inType.Equals(outType) {
			targetTypes[i] = outType
		}
	}
	return proj, targetTypes
}
