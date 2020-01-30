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

package actions

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// StrMapper is a simple interface for mapping a string to another string
type StrMapper interface {
	// Map maps a string to another string.  If a string is not in the mapping ok will be false, otherwise it is true.
	Map(str string) (mappedStr string, ok bool)
}

// IdentityMapper maps any string to itself
type IdentityMapper struct{}

// Map maps a string to another string.  For the identity mapper the input string always maps to the output string
func (m IdentityMapper) Map(str string) (string, bool) {
	return str, true
}

// MapMapper is a StrMapper implementation that is backed by a map[string]string
type MapMapper map[string]string

// Map maps a string to another string.  If a string is not in the mapping ok will be false, otherwise it is true.
func (m MapMapper) Map(str string) (string, bool) {
	v, ok := m[str]
	return v, ok
}

// InferenceArgs are arguments that can be passed to the schema inferrer to modify it's inference behavior.
type InferenceArgs struct {
	// ExistingSch is the schema for the existing schema.  If no schema exists schema.EmptySchema is expected.
	ExistingSch schema.Schema
	// ColMapper allows columns named X in the schema to be named Y in the inferred schema.
	ColMapper StrMapper
	// FloatThreshold is the threshold at which a string representing a floating point number should be interpreted as
	// a float versus an int.  If FloatThreshold is 0.0 then any number with a decimal point will be interpreted as a
	// float (such as 0.0, 1.0, etc).  If FloatThreshold is 1.0 then any number with a decimal point will be converted
	// to an int (0.5 will be the int 0, 1.99 will be the int 1, etc.  If the FloatThreshold is 0.001 then numbers with
	// a fractional component greater than or equal to 0.001 will be treated as a float (1.0 would be an int, 1.0009 would
	// be an int, 1.001 would be a float, 1.1 would be a float, etc)
	FloatThreshold float64
	// KeepTypes is a flag which tells the inferrer, that if a column already exists in the ExistinchSch then use it's type
	// without modification.
	KeepTypes bool
	// Update is a flag which tells the inferrer, not to change existing columns
	Update bool
}

// InferSchemaFromTableReader will infer a tables schema.
func InferSchemaFromTableReader(ctx context.Context, rd table.TableReadCloser, pkCols []string, args *InferenceArgs) (schema.Schema, error) {
	pkColToIdx := make(map[string]int, len(pkCols))
	for i, colName := range pkCols {
		pkColToIdx[colName] = i
	}

	inferrer := newInferrer(pkColToIdx, rd.GetSchema(), args)

	rdProcFunc := pipeline.ProcFuncForReader(ctx, rd)
	p := pipeline.NewAsyncPipeline(rdProcFunc, inferrer.sinkRow, nil, inferrer.badRow)
	p.Start()

	err := p.Wait()

	if err != nil {
		return nil, err
	}

	if inferrer.rowFailure != nil {
		return nil, inferrer.rowFailure
	}

	return inferrer.inferSchema()
}

type inferrer struct {
	sch        schema.Schema
	pkColToIdx map[string]int
	impArgs    *InferenceArgs

	colNames  []string
	colCount  int
	colType   []map[types.NomsKind]int
	negatives []bool

	rowFailure *pipeline.TransformRowFailure
}

func newInferrer(pkColToIdx map[string]int, sch schema.Schema, args *InferenceArgs) *inferrer {
	colColl := sch.GetAllCols()
	colNames := make([]string, 0, colColl.Size())

	_ = colColl.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		colNames = append(colNames, col.Name)
		return false, nil
	})

	colCount := len(colNames)
	colType := make([]map[types.NomsKind]int, colCount)
	negatives := make([]bool, colCount)
	for i := 0; i < colCount; i++ {
		colType[i] = make(map[types.NomsKind]int)
	}

	return &inferrer{sch, pkColToIdx, args, colNames, colCount, colType, negatives, nil}
}

func (inf *inferrer) inferSchema() (schema.Schema, error) {
	nonPkCols, _ := schema.NewColCollection()
	pkCols, _ := schema.NewColCollection()

	if inf.impArgs.Update {
		nonPkCols = inf.impArgs.ExistingSch.GetNonPKCols()
		pkCols = inf.impArgs.ExistingSch.GetPKCols()
	}

	existingCols := inf.impArgs.ExistingSch.GetAllCols()

	tag := uint64(0)
	colNamesSet := set.NewStrSet(inf.colNames)
	for i, name := range inf.colNames {
		if mappedName, ok := inf.impArgs.ColMapper.Map(name); ok {
			name = mappedName
		}

		colNamesSet.Add(name)
		_, partOfPK := inf.pkColToIdx[name]
		typeToCount := inf.colType[i]
		hasNegatives := inf.negatives[i]
		kind, nullable := typeCountsToKind(name, typeToCount, hasNegatives)

		tag = nextTag(tag, existingCols)
		thisTag := tag
		var col *schema.Column
		if existingCol, ok := existingCols.GetByName(name); ok {
			if inf.impArgs.Update {
				if nullable {
					if partOfPK {
						pkCols = checkNullConstraint(pkCols, existingCol)
					} else {
						nonPkCols = checkNullConstraint(nonPkCols, existingCol)
					}
				}

				continue
			} else if inf.impArgs.KeepTypes {
				col = &existingCol
			} else {
				thisTag = existingCol.Tag
			}
		} else {
			tag++
		}

		if col == nil {
			constraints := make([]schema.ColConstraint, 0, 1)
			if !nullable {
				constraints = append(constraints, schema.NotNullConstraint{})
			}

			tmp := schema.NewColumn(name, thisTag, kind, partOfPK, constraints...)
			col = &tmp
		}

		var err error
		if col.IsPartOfPK {
			pkCols, err = pkCols.Append(*col)
		} else {
			nonPkCols, err = nonPkCols.Append(*col)
		}

		if err != nil {
			return nil, err
		}
	}

	if pkCols.Size() != len(inf.pkColToIdx) {
		return nil, errors.New("some pk columns were not found")
	}

	pkCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if !colNamesSet.Contains(col.Name) {
			pkCols = checkNullConstraint(pkCols, col)
		}
		return false, nil
	})

	nonPkCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if !colNamesSet.Contains(col.Name) {
			nonPkCols = checkNullConstraint(nonPkCols, col)
		}
		return false, nil
	})

	orderedPKCols := make([]schema.Column, pkCols.Size())
	err := pkCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		idx, ok := inf.pkColToIdx[col.Name]

		if !ok {
			return false, errors.New("could not find key column")
		}

		orderedPKCols[idx] = col
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	pkColColl, err := schema.NewColCollection(orderedPKCols...)

	if err != nil {
		return nil, err
	}

	return schema.SchemaFromPKAndNonPKCols(pkColColl, nonPkCols)
}

func checkNullConstraint(colColl *schema.ColCollection, col schema.Column) *schema.ColCollection {
	_, ok := colColl.GetByTag(col.Tag)
	if !ok {
		return colColl
	}

	constraints := col.Constraints
	numConstraints := len(constraints)
	if numConstraints > 0 {
		notNullConstraintIdx := schema.ConstraintOfTypeIndex(constraints, schema.NotNullConstraintType)

		if notNullConstraintIdx != -1 {
			if notNullConstraintIdx == 0 {
				constraints = constraints[1:]
			} else if notNullConstraintIdx == numConstraints-1 {
				constraints = constraints[:notNullConstraintIdx]
			} else {
				constraints[notNullConstraintIdx] = constraints[numConstraints-1]
				constraints = constraints[:numConstraints-1]
			}

			newCol := schema.NewColumn(col.Name, col.Tag, col.Kind, col.IsPartOfPK, constraints...)
			colColl, _ = colColl.Replace(col, newCol)
		}
	}

	return colColl
}

func nextTag(tag uint64, cols *schema.ColCollection) uint64 {
	for {
		_, ok := cols.GetByTag(tag)

		if !ok {
			return tag
		}

		tag++
	}
}

func typeCountsToKind(name string, typeToCount map[types.NomsKind]int, hasNegatives bool) (types.NomsKind, bool) {
	var nullable bool
	kind := types.NullKind

	for t := range typeToCount {
		if t == types.NullKind {
			nullable = true
			continue
		} else if kind == types.NullKind {
			kind = t
		}

		if kind == t {
			continue
		}

		switch kind {
		case types.StringKind:
			if nullable {
				return types.StringKind, true
			}

		case types.UUIDKind:
			//cli.PrintErrln(color.YellowString("warning: column %s has a mix of uuids and non uuid strings.", name))
			kind = types.StringKind

		case types.BoolKind:
			kind = types.StringKind

		case types.IntKind:
			if t == types.FloatKind {
				kind = types.FloatKind
			} else if t == types.UintKind {
				if !hasNegatives {
					kind = types.UintKind
				} else {
					//cli.PrintErrln(color.YellowString("warning: %s has values larger than a 64 bit signed integer can hold, and negative numbers.  This will be interpreted as a string.", name))
					kind = types.StringKind
				}
			} else {
				kind = types.StringKind
			}

		case types.UintKind:
			if t == types.IntKind {
				if hasNegatives {
					//cli.PrintErrln(color.YellowString("warning: %s has values larger than a 64 bit signed integer can hold, and negative numbers.  This will be interpreted as a string.", name))
					kind = types.StringKind
				}
			} else {
				kind = types.StringKind
			}

		case types.FloatKind:
			if t != types.IntKind {
				kind = types.StringKind
			}
		}
	}

	if kind == types.NullKind {
		kind = types.StringKind
	}

	return kind, nullable
}

func (inf *inferrer) sinkRow(p *pipeline.Pipeline, ch <-chan pipeline.RowWithProps, badRowChan chan<- *pipeline.TransformRowFailure) {
	for r := range ch {
		i := 0
		_, _ = r.Row.IterSchema(inf.sch, func(tag uint64, val types.Value) (stop bool, err error) {
			defer func() {
				i++
			}()

			if val == nil {
				inf.colType[i][types.NullKind]++
				return false, nil
			}

			strVal := string(val.(types.String))
			kind, hasNegs := leastPermissiveKind(strVal, inf.impArgs.FloatThreshold)

			if hasNegs {
				inf.negatives[i] = true
			}

			inf.colType[i][kind]++

			return false, nil
		})
	}
}

func leastPermissiveKind(strVal string, floatThreshold float64) (types.NomsKind, bool) {
	if len(strVal) == 0 {
		return types.NullKind, false
	}

	strVal = strings.TrimSpace(strVal)
	kind := types.StringKind
	hasNegativeNums := false

	if _, err := uuid.Parse(strVal); err == nil {
		kind = types.UUIDKind
	} else if negs, numKind := leastPermissiveNumericKind(strVal, floatThreshold); numKind != types.NullKind {
		kind = numKind
		hasNegativeNums = negs
	} else if _, err := strconv.ParseBool(strVal); err == nil {
		kind = types.BoolKind
	}

	return kind, hasNegativeNums
}

var lenDecEncodedMaxInt = len(strconv.FormatInt(math.MaxInt64, 10))

func leastPermissiveNumericKind(strVal string, floatThreshold float64) (isNegative bool, kind types.NomsKind) {
	isNum, isFloat, isNegative := stringNumericProperties(strVal)

	if !isNum {
		return false, types.NullKind
	} else if isFloat {
		if floatThreshold != 0.0 {
			floatParts := strings.Split(strVal, ".")
			decimalPart, err := strconv.ParseFloat("0."+floatParts[1], 64)

			if err != nil {
				panic(err)
			}

			if decimalPart >= floatThreshold {
				return isNegative, types.FloatKind
			}

			return isNegative, types.IntKind
		}
		return isNegative, types.FloatKind
	} else if len(strVal) < lenDecEncodedMaxInt {
		// Prefer Ints if everything fits
		return isNegative, types.IntKind
	} else if isNegative {
		_, sErr := strconv.ParseInt(strVal, 10, 64)

		if sErr == nil {
			return isNegative, types.IntKind
		}
	} else {
		_, uErr := strconv.ParseUint(strVal, 10, 64)
		_, sErr := strconv.ParseInt(strVal, 10, 64)

		if sErr == nil {
			return false, types.IntKind
		} else if uErr == nil {
			return false, types.UintKind
		}
	}

	return false, types.NullKind
}

func stringNumericProperties(strVal string) (isNum, isFloat, isNegative bool) {
	if len(strVal) == 0 {
		return false, false, false
	}

	isNum = true
	for i, c := range strVal {
		if i == 0 && c == '-' {
			isNegative = true
			continue
		} else if i == 0 && c == '0' && len(strVal) > 1 && strVal[i+1] != '.' {
			// by default treat leading zeroes as invalid
			return false, false, false
		}

		if c != '.' && (c < '0' || c > '9') {
			return false, false, false
		}

		if c == '.' {
			if isFloat {
				// found 2 decimal points
				return false, false, false
			} else {
				isFloat = true
			}
		}
	}

	return isNum, isFloat, isNegative
}

func (inf *inferrer) badRow(trf *pipeline.TransformRowFailure) (quit bool) {
	inf.rowFailure = trf
	return false
}
