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
// to |outSchema|. Columns are matched by tag or name as rowPlacements
// describes, whether or not they are in the primary key. The output of the
// conversion process is a [sql.Row].
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

// NewProllyRowConverter returns a ProllyRowConverter of |inSch| rows into
// |outSch| rows.
//
// A diff uses it to render each side in its target schema.
func NewProllyRowConverter(ctx *sql.Context, inSch, outSch schema.Schema, warnFn rowconv.WarnFunction, ns tree.NodeStore) (ProllyRowConverter, error) {
	keyProj, valProj, err := rowPlacements(inSch, outSch)
	if err != nil {
		return ProllyRowConverter{}, err
	}

	pkTargetTypes := targetTypes(inSch.GetPKCols(), outSch, keyProj)
	nonPkTargetTypes := targetTypes(inSch.GetNonPKCols(), outSch, valProj)

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

// rowPlacements returns where each key and value column of |inSch| goes in
// |outSch|'s rows, or -1. Key columns match as keyPlacement describes, and
// value columns match by name.
//
// Either kind may match a column in or out of the primary key of |outSch|,
// since rekeying a diff side to the other side's key moves columns into or
// out of its key.
func rowPlacements(inSch, outSch schema.Schema) (val.OrdinalMapping, val.OrdinalMapping, error) {
	keyCols := inSch.GetPKCols().GetColumns()
	if len(keyCols) == 0 && inSch.GetNonPKCols().Size() == 0 {
		// The table is added or dropped on this side of the diff.
		return val.OrdinalMapping{}, val.OrdinalMapping{}, nil
	}
	outCols := outSch.GetAllCols()
	keyProj := make(val.OrdinalMapping, len(keyCols))
	for i, col := range keyCols {
		j, ok := keyPlacement(col, outCols)
		if !ok {
			return nil, nil, fmt.Errorf("could not map primary key column %s", col.Name)
		}
		keyProj[i] = j
	}

	valCols := inSch.GetNonPKCols().GetColumns()
	valProj := make(val.OrdinalMapping, len(valCols))
	for i, col := range valCols {
		out, ok := outCols.GetByName(col.Name)
		if col.Virtual || !ok {
			valProj[i] = -1
			continue
		}
		valProj[i] = outCols.TagToIdx[out.Tag]
	}
	return keyProj, valProj, nil
}

// keyPlacement returns the index in |outCols| of the key column |col|, or
// false when none matches. It matches by tag, or else by case-insensitive
// name and the same SQL type.
//
// A key value of another type has other key bytes, so a name alone is not a
// match.
func keyPlacement(col schema.Column, outCols *schema.ColCollection) (int, bool) {
	if j, ok := outCols.TagToIdx[col.Tag]; ok {
		return j, true
	}
	out, ok := outCols.GetByNameCaseInsensitive(col.Name)
	if !ok || !out.TypeInfo.ToSqlType().Equals(col.TypeInfo.ToSqlType()) {
		return -1, false
	}
	return outCols.TagToIdx[out.Tag], true
}

// targetTypes returns the types to convert |inCols| values to in |outSch|,
// where |proj| places them. A type is nil when its column is not placed or
// already has that type.
func targetTypes(inCols *schema.ColCollection, outSch schema.Schema, proj val.OrdinalMapping) []sql.Type {
	types := make([]sql.Type, len(proj))
	for i, j := range proj {
		if j == -1 {
			continue
		}
		inColType := inCols.GetByIndex(i).TypeInfo.ToSqlType()
		outColType := outSch.GetAllCols().GetByIndex(j).TypeInfo.ToSqlType()
		if !inColType.Equals(outColType) {
			types[i] = outColType
		}
	}
	return types
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
