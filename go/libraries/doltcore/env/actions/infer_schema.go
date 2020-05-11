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
	"math"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/utils/funcitr"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// StrMapper is a simple interface for mapping a string to another string
type StrMapper interface {
	// Map maps a string to another string.  If a string is not in the mapping ok will be false, otherwise it is true.
	MaybeMap(str string) string
}

// IdentityMapper maps any string to itself
type IdentityMapper struct{}

// Map maps a string to another string.  For the identity mapper the input string always maps to the output string
func (m IdentityMapper) MaybeMap(str string) string {
	return str
}

// MapMapper is a StrMapper implementation that is backed by a map[string]string
type MapMapper map[string]string

// Map maps a string to another string.  If a string is not in the mapping ok will be false, otherwise it is true.
func (m MapMapper) MaybeMap(str string) string {
	v, ok := m[str]
	if ok {
		return v
	}
	return str
}

type typeInfoSet map[typeinfo.TypeInfo]struct{}

type SchImportOp int

const (
	CreateOp SchImportOp = iota
	UpdateOp
	ReplaceOp
)

const (
	maxUint24 = 1<<24 - 1
	minInt24  = -1 << 23
)

// InferenceArgs are arguments that can be passed to the schema inferrer to modify it's inference behavior.
type InferenceArgs struct {
	TableName   string
	SchImportOp SchImportOp
	// ExistingSch is the schema for the existing schema.  If no schema exists schema.EmptySchema is expected.
	ExistingSch schema.Schema
	// PKCols are the columns from the input file that should be used as primary keys in the output schema
	PkCols []string
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
}

// InferSchemaFromTableReader will infer a tables schema.
func InferSchemaFromTableReader(ctx context.Context, rd table.TableReadCloser, args *InferenceArgs, root *doltdb.RootValue) (schema.Schema, error) {
	inferrer := newInferrer(rd.GetSchema(), args)

	var rowFailure *pipeline.TransformRowFailure
	badRow := func(trf *pipeline.TransformRowFailure) (quit bool) {
		rowFailure = trf
		return false
	}

	rdProcFunc := pipeline.ProcFuncForReader(ctx, rd)
	p := pipeline.NewAsyncPipeline(rdProcFunc, inferrer.sinkRow, nil, badRow)
	p.Start()

	err := p.Wait()

	if err != nil {
		return nil, err
	}

	if rowFailure != nil {
		return nil, rowFailure
	}

	return inferrer.inferSchema(ctx, root)
}

type inferrer struct {
	readerSch schema.Schema
	inferSets map[uint64]typeInfoSet
	nullable  *set.Uint64Set

	inferArgs *InferenceArgs
}

func newInferrer(readerSch schema.Schema, args *InferenceArgs) *inferrer {
	inferSets := make(map[uint64]typeInfoSet, readerSch.GetAllCols().Size())
	_ = readerSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		inferSets[tag] = make(typeInfoSet)
		return false, nil
	})

	return &inferrer{
		readerSch: readerSch,
		inferSets: inferSets,
		nullable:  set.NewUint64Set(nil),
		inferArgs: args,
	}
}

func (inf *inferrer) inferSchema(ctx context.Context, root *doltdb.RootValue) (schema.Schema, error) {
	existingSch := inf.inferArgs.ExistingSch
	if existingSch == nil {
		existingSch = schema.EmptySchema
	}

	op := inf.inferArgs.SchImportOp

	// use post-mapping column names for all column name matching
	mapper := inf.inferArgs.ColMapper
	readerColsMapped := funcitr.MapStrings(inf.readerSch.GetAllCols().GetColumnNames(), mapper.MaybeMap)
	existingCols := set.NewStrSet(existingSch.GetAllCols().GetColumnNames())

	inter, missing := existingCols.IntersectAndMissing(readerColsMapped)

	pkCols, _ := schema.NewColCollection()
	nonPKCols, _ := schema.NewColCollection()

	interCols := set.NewStrSet(inter)
	_ = inf.inferArgs.ExistingSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		keep := op == UpdateOp && !interCols.Contains(col.Name) || inf.inferArgs.KeepTypes && interCols.Contains(col.Name)
		if keep {
			if col.IsPartOfPK {
				pkCols, err = pkCols.Append(col)
			} else {
				nonPKCols, err = nonPKCols.Append(col)
			}
		}
		stop = err != nil
		return stop, err
	})

	newCols := set.NewStrSet(nil)
	if op == CreateOp {
		// inter == nil
		newCols.Add(missing...)
	} else {
		// UpdateOp || ReplaceOp
		if inf.inferArgs.KeepTypes {
			newCols.Add(missing...)

		} else {
			newCols.Add(inter...)
			newCols.Add(missing...)
		}
	}

	inferredTypes := make(map[uint64]typeinfo.TypeInfo)
	for tag, ts := range inf.inferSets {
		inferredTypes[tag] = findCommonType(ts)
	}

	pkSet := set.NewStrSet(inf.inferArgs.PkCols)

	var newColNames []string
	var newColKinds []types.NomsKind
	var newColTypes []typeinfo.TypeInfo
	var newColIsPk []bool
	var newColNullable []bool
	_ = inf.readerSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		name := mapper.MaybeMap(col.Name)
		if newCols.Contains(name) {
			ti := inferredTypes[tag]
			newColKinds = append(newColKinds, ti.NomsKind())
			newColTypes = append(newColTypes, ti)
			newColNames = append(newColNames, name)
			newColIsPk = append(newColIsPk, pkSet.Contains(name))
			newColNullable = append(newColNullable, inf.nullable.Contains(tag))
		}
		return false, nil
	})

	newColTags, err := root.GenerateTagsForNewColumns(ctx, inf.inferArgs.TableName, newColNames, newColKinds)
	if err != nil {
		return nil, err
	}

	for i := range newColNames {
		constraint := []schema.ColConstraint(nil)
		if !newColNullable[i] && newColIsPk[i] {
			constraint = []schema.ColConstraint{schema.NotNullConstraint{}}
		}

		c, err := schema.NewColumnWithTypeInfo(
			newColNames[i],
			newColTags[i],
			newColTypes[i],
			newColIsPk[i],
			constraint...,
		)

		if err != nil {
			return nil, err
		}

		if c.IsPartOfPK {
			pkCols, err = pkCols.Append(c)
		} else {
			nonPKCols, err = nonPKCols.Append(c)
		}

		if err != nil {
			return nil, err
		}
	}

	return schema.SchemaFromPKAndNonPKCols(pkCols, nonPKCols)
}

func (inf *inferrer) sinkRow(p *pipeline.Pipeline, ch <-chan pipeline.RowWithProps, badRowChan chan<- *pipeline.TransformRowFailure) {
	for r := range ch {
		_, _ = r.Row.IterSchema(inf.readerSch, func(tag uint64, val types.Value) (stop bool, err error) {
			if val == nil {
				inf.nullable.Add(tag)
				return false, nil
			}
			strVal := string(val.(types.String))
			typeInfo := leastPermissiveType(strVal, inf.inferArgs.FloatThreshold)
			inf.inferSets[tag][typeInfo] = struct{}{}
			return false, nil
		})
	}
}

func leastPermissiveType(strVal string, floatThreshold float64) typeinfo.TypeInfo {
	if len(strVal) == 0 {
		return typeinfo.UnknownType
	}
	strVal = strings.TrimSpace(strVal)

	numType := leastPermissiveNumericType(strVal, floatThreshold)
	if numType != typeinfo.UnknownType {
		return numType
	}

	chronoType := leastPermissiveChronoType(strVal)
	if chronoType != typeinfo.UnknownType {
		return chronoType
	}

	_, err := uuid.Parse(strVal)
	if err == nil {
		return typeinfo.UuidType
	}

	strVal = strings.ToLower(strVal)
	if strVal == "true" || strVal == "false" {
		return typeinfo.BoolType
	}

	return typeinfo.StringDefaultType
}

func leastPermissiveNumericType(strVal string, floatThreshold float64) (ti typeinfo.TypeInfo) {
	if strings.Contains(strVal, ".") {
		f, err := strconv.ParseFloat(strVal, 64)
		if err != nil {
			return typeinfo.UnknownType
		}

		if math.Abs(f) < math.MaxFloat32 {
			ti = typeinfo.Float32Type
		} else {
			ti = typeinfo.Float64Type
		}

		if floatThreshold != 0.0 {
			floatParts := strings.Split(strVal, ".")
			decimalPart, err := strconv.ParseFloat("0."+floatParts[1], 64)

			if err != nil {
				panic(err)
			}

			if decimalPart < floatThreshold {
				// we could be more specific with these casts if necessary
				if ti == typeinfo.Float32Type {
					ti = typeinfo.Int32Type
				} else {
					ti = typeinfo.Int64Type
				}
			}
		}
		return ti
	}

	i, err := strconv.ParseInt(strVal, 10, 64)
	if err != nil {
		return typeinfo.UnknownType
	}
	if i >= int64(0) {
		ui := uint64(i)
		switch {
		case ui <= math.MaxUint8:
			return typeinfo.Uint8Type
		case ui <= math.MaxUint16:
			return typeinfo.Uint16Type
		case ui <= maxUint24:
			return typeinfo.Uint24Type
		case ui <= math.MaxUint32:
			return typeinfo.Uint32Type
		case ui <= math.MaxUint64:
			return typeinfo.Uint64Type
		}
	} else {
		switch {
		case i >= math.MinInt8:
			return typeinfo.Int8Type
		case i >= math.MinInt16:
			return typeinfo.Int16Type
		case i >= minInt24:
			return typeinfo.Int24Type
		case i >= math.MinInt32:
			return typeinfo.Int32Type
		case i >= math.MinInt64:
			return typeinfo.Int64Type
		}
	}

	return typeinfo.UnknownType
}

func leastPermissiveChronoType(strVal string) typeinfo.TypeInfo {
	// todo: be more specific with chrono types
	_, err := typeinfo.DatetimeType.ParseValue(&strVal)
	if err != nil {
		return typeinfo.UnknownType
	}
	return typeinfo.DatetimeType
}

func chronoTypes() []typeinfo.TypeInfo {
	return []typeinfo.TypeInfo{
		// chrono types YEAR, DATE, and TIME can also be parsed as DATETIME
		// we prefer less permissive types if possible
		typeinfo.YearType,
		typeinfo.DateType,
		typeinfo.TimeType,
		typeinfo.TimestampType,
		typeinfo.DatetimeType,
	}
}

// ordered from least to most permissive
func numericTypes() []typeinfo.TypeInfo {
	// prefer:
	//   ints over floats
	//   unsigned over signed
	//   smaller over larger
	return []typeinfo.TypeInfo{
		typeinfo.Uint8Type,
		typeinfo.Uint16Type,
		typeinfo.Uint24Type,
		typeinfo.Uint32Type,
		typeinfo.Uint64Type,

		typeinfo.Int8Type,
		typeinfo.Int16Type,
		typeinfo.Int24Type,
		typeinfo.Int32Type,
		typeinfo.Int64Type,

		typeinfo.Float32Type,
		typeinfo.Float64Type,
	}
}

func setHasType(ts typeInfoSet, t typeinfo.TypeInfo) bool {
	_, found := ts[t]
	return found
}

// findCommonType takes a set of types and finds the least permissive
// (ie most specific) common type between all types in the set
func findCommonType(ts typeInfoSet) typeinfo.TypeInfo {

	// empty values were inferred as UnknownType
	delete(ts, typeinfo.UnknownType)

	if len(ts) == 0 {
		// use strings if all values were empty
		return typeinfo.StringDefaultType
	}

	if len(ts) == 1 {
		for ti := range ts {
			return ti
		}
	}

	// len(ts) > 1

	if setHasType(ts, typeinfo.StringDefaultType) {
		return typeinfo.StringDefaultType
	}

	hasNumeric := false
	for _, nt := range numericTypes() {
		if setHasType(ts, nt) {
			hasNumeric = true
			break
		}
	}

	hasNonNumeric := false
	for _, nnt := range chronoTypes() {
		if setHasType(ts, nnt) {
			hasNonNumeric = true
			break
		}
	}
	if setHasType(ts, typeinfo.BoolType) || setHasType(ts, typeinfo.UuidType) {
		hasNonNumeric = true
	}

	if hasNumeric && hasNonNumeric {
		return typeinfo.StringDefaultType
	}

	if hasNumeric {
		return findCommonNumericType(ts)
	}

	// find a common nonNumeric type

	nonChronoTypes := []typeinfo.TypeInfo{
		// todo: BIT implementation parses all uint8
		//typeinfo.PseudoBoolType,
		typeinfo.BoolType,
		typeinfo.UuidType,
	}
	for _, nct := range nonChronoTypes {
		if setHasType(ts, nct) {
			// types in nonChronoTypes have only string
			// as a common type with any other type
			return typeinfo.StringDefaultType
		}
	}

	return findCommonChronoType(ts)
}

func findCommonNumericType(nums typeInfoSet) typeinfo.TypeInfo {
	// find a common numeric type
	// iterate through types from most to least permissive
	// return the most permissive type found
	//   ints are a subset of floats
	//   uints are a subset of ints
	//   smaller widths are a subset of larger widths
	mostToLeast := []typeinfo.TypeInfo{
		typeinfo.Float64Type,
		typeinfo.Float32Type,

		// todo: can all Int64 fit in Float64?
		typeinfo.Int64Type,
		typeinfo.Int32Type,
		typeinfo.Int24Type,
		typeinfo.Int16Type,
		typeinfo.Int8Type,

		typeinfo.Uint64Type,
		typeinfo.Uint32Type,
		typeinfo.Uint24Type,
		typeinfo.Uint16Type,
		typeinfo.Uint8Type,
	}
	for _, numType := range mostToLeast {
		if setHasType(nums, numType) {
			return numType
		}
	}

	panic("unreachable")
}

func findCommonChronoType(chronos typeInfoSet) typeinfo.TypeInfo {
	if len(chronos) == 1 {
		for ct := range chronos {
			return ct
		}
	}

	if setHasType(chronos, typeinfo.DatetimeType) {
		return typeinfo.DatetimeType
	}

	hasTime := setHasType(chronos, typeinfo.TimeType) || setHasType(chronos, typeinfo.TimestampType)
	hasDate := setHasType(chronos, typeinfo.DateType) || setHasType(chronos, typeinfo.YearType)

	if hasTime && !hasDate {
		return typeinfo.TimeType
	}

	if !hasTime && hasDate {
		return typeinfo.DateType
	}

	if hasDate && hasTime {
		return typeinfo.DatetimeType
	}

	panic("unreachable")
}
