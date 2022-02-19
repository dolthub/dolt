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

package rowconv

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"

	"github.com/dolthub/go-mysql-server/sql"
)

var IdentityConverter = &RowConverter{nil, true, nil}

// WarnFunction is a callback function that callers can optionally provide during row conversion
// to take an extra action when a value cannot be automatically converted to the output data type.
type WarnFunction func(int, string, ...string)

var DatatypeCoercionFailureWarning = "unable to coerce value from field '%s' into latest column schema"

const DatatypeCoercionFailureWarningCode int = 1105 // Since this our own custom warning we'll use 1105, the code for an unknown error

// RowConverter converts rows from one schema to another
type RowConverter struct {
	// FieldMapping is a mapping from source column to destination column
	*FieldMapping
	// IdentityConverter is a bool which is true if the converter is doing nothing.
	IdentityConverter bool
	ConvFuncs         map[uint64]types.MarshalCallback
}

func newIdentityConverter(mapping *FieldMapping) *RowConverter {
	return &RowConverter{mapping, true, nil}
}

// NewRowConverter creates a row converter from a given FieldMapping.
func NewRowConverter(ctx context.Context, vrw types.ValueReadWriter, mapping *FieldMapping) (*RowConverter, error) {
	if nec, err := IsNecessary(mapping.SrcSch, mapping.DestSch, mapping.SrcToDest); err != nil {
		return nil, err
	} else if !nec {
		return newIdentityConverter(mapping), nil
	}

	// Panic if there are any duplicate columns mapped to the same destination tag.
	panicOnDuplicateMappings(mapping)

	convFuncs := make(map[uint64]types.MarshalCallback, len(mapping.SrcToDest))
	for srcTag, destTag := range mapping.SrcToDest {
		destCol, destOk := mapping.DestSch.GetAllCols().GetByTag(destTag)
		srcCol, srcOk := mapping.SrcSch.GetAllCols().GetByTag(srcTag)

		if !destOk || !srcOk {
			return nil, fmt.Errorf("Could not find column being mapped. src tag: %d, dest tag: %d", srcTag, destTag)
		}

		tc, _, err := typeinfo.GetTypeConverter(ctx, srcCol.TypeInfo, destCol.TypeInfo)
		if err != nil {
			return nil, err
		}
		convFuncs[srcTag] = func(v types.Value) (types.Value, error) {
			return tc(ctx, vrw, v)
		}
	}

	return &RowConverter{mapping, false, convFuncs}, nil
}

// panicOnDuplicateMappings checks if more than one input field is mapped to the same output field.
// Multiple input fields mapped to the same output field results in a race condition.
func panicOnDuplicateMappings(mapping *FieldMapping) {
	destToSrcMapping := make(map[uint64]uint64, len(mapping.SrcToDest))
	for srcTag, destTag := range mapping.SrcToDest {
		if _, found := destToSrcMapping[destTag]; found {
			panic("multiple columns mapped to the same destination tag '" + types.Uint(destTag).HumanReadableString() + "'")
		}
		destToSrcMapping[destTag] = srcTag
	}
}

// ConvertWithWarnings takes an input row, maps its columns to their destination columns, performing any type
// conversions needed to create a row of the expected destination schema, and uses the optional WarnFunction
// callback to let callers handle logging a warning when a field cannot be cleanly converted.
func (rc *RowConverter) ConvertWithWarnings(inRow row.Row, warnFn WarnFunction) (row.Row, error) {
	return rc.convert(inRow, warnFn)
}

// Convert takes an input row, maps its columns to destination columns, and performs any type conversion needed to
// create a row of the expected destination schema.
func (rc *RowConverter) Convert(inRow row.Row) (row.Row, error) {
	return rc.convert(inRow, nil)
}

// convert takes a row and maps its columns to their destination columns, automatically performing any type conversion
// needed, and using the optional WarnFunction to let callers log warnings on any type conversion errors.
func (rc *RowConverter) convert(inRow row.Row, warnFn WarnFunction) (row.Row, error) {
	if rc.IdentityConverter {
		return inRow, nil
	}

	outTaggedVals := make(row.TaggedValues, len(rc.SrcToDest))
	_, err := inRow.IterCols(func(tag uint64, val types.Value) (stop bool, err error) {
		convFunc, ok := rc.ConvFuncs[tag]

		if ok {
			outTag := rc.SrcToDest[tag]
			outVal, err := convFunc(val)

			if sql.ErrInvalidValue.Is(err) && warnFn != nil {
				col, _ := rc.SrcSch.GetAllCols().GetByTag(tag)
				warnFn(DatatypeCoercionFailureWarningCode, DatatypeCoercionFailureWarning, col.Name)
				outVal = types.NullValue
				err = nil
			}

			if err != nil {
				return false, err
			}

			if types.IsNull(outVal) {
				return false, nil
			}

			outTaggedVals[outTag] = outVal
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return row.New(inRow.Format(), rc.DestSch, outTaggedVals)
}

func IsNecessary(srcSch, destSch schema.Schema, destToSrc map[uint64]uint64) (bool, error) {
	srcCols := srcSch.GetAllCols()
	destCols := destSch.GetAllCols()

	if len(destToSrc) != srcCols.Size() || len(destToSrc) != destCols.Size() {
		return true, nil
	}

	for k, v := range destToSrc {
		if k != v {
			return true, nil
		}

		srcCol, srcOk := srcCols.GetByTag(v)
		destCol, destOk := destCols.GetByTag(k)

		if !srcOk || !destOk {
			panic("There is a bug.  FieldMapping creation should prevent this from happening")
		}

		if srcCol.IsPartOfPK != destCol.IsPartOfPK {
			return true, nil
		}

		if !srcCol.TypeInfo.Equals(destCol.TypeInfo) {
			return true, nil
		}
	}

	srcPKCols := srcSch.GetPKCols()
	destPKCols := destSch.GetPKCols()

	if srcPKCols.Size() != destPKCols.Size() {
		return true, nil
	}

	i := 0
	err := destPKCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		srcPKCol := srcPKCols.GetByIndex(i)

		if srcPKCol.Tag != col.Tag {
			return true, nil
		}

		i++
		return false, nil
	})

	if err != nil {
		return false, err
	}

	return false, nil
}
