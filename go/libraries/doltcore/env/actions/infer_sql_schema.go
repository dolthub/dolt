// Copyright 2021 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
)


type sqlInferrer struct {
	readerSch      sql.Schema
	inferSets      map[string]sqlTypeInfoSet
	nullable       *set.StrSet
	mapper         rowconv.NameMapper
	floatThreshold float64

	//inferArgs *InferenceArgs
}

func newSQLInferrer(ctx context.Context, readerSch sql.Schema, args InferenceArgs) *sqlInferrer {
	inferSets := make(map[string]sqlTypeInfoSet, len(readerSch))

	for _, col := range readerSch {
		inferSets[col.Name] = make(sqlTypeInfoSet) // can use id instead of name maybe?
	}

	return &sqlInferrer{
		readerSch:      readerSch,
		inferSets:      inferSets,
		nullable:       set.NewStrSet(nil),
		mapper:         args.ColNameMapper(),
		floatThreshold: args.FloatThreshold(),
	}
}


func InferSqlSchemaFromTableReader(ctx context.Context, root *doltdb.RootValue, rd table.TableReadCloser, args InferenceArgs) (sql.Schema, error) {
	inferrer := newSQLInferrer(ctx, rd.GetSqlSchema(), args)

	// start the pipeline
	g, ctx := errgroup.WithContext(ctx)

	parsedRowChan := make(chan sql.Row)
	g.Go(func() error {
		defer close(parsedRowChan)
		for {
			r, err := rd.ReadSqlRow(ctx)

			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}


			select {
			case <-ctx.Done():
				return ctx.Err()
			case parsedRowChan <- r:
			}
		}
	})


	g.Go(func() error {
		for r := range parsedRowChan {
			for i, col := range rd.GetSqlSchema() {
				val := r[i]
				if val == nil {
					inferrer.nullable.Add(col.Name)
				}
				strVal, err := sql.Text.Convert(val)
				if err != nil {
					return err
				}

				typ := sqlLeastPermissiveType(strVal.(string), inferrer.floatThreshold)
				inferrer.inferSets[col.Name][typ] = struct{}{}
			}
		}

		return nil
	})

	err := g.Wait()
	if err != nil {
		return nil, err
	}

}

func sqlLeastPermissiveType(strVal string, floatThreshold float64) sql.Type {
	if len(strVal) == 0 {
		return sql.Null
	}
	strVal = strings.TrimSpace(strVal)

	numType, ok := leastPermissiveSqlNumericType(strVal, floatThreshold)
	if ok {
		return numType
	}

	chronoType, ok := leastPermissiveSqlChronoType(strVal)
	if ok {
		return chronoType
	}

	_, err := uuid.Parse(strVal)
	if err == nil {
		return sql.MustCreateStringWithDefaults(sqltypes.VarChar, 36) // TODO: Return uuid function type??
	}

	return sql.Text
}

func leastPermissiveSqlNumericType(strVal string, floatThreshold float64) (ti sql.Type, ok bool) {
	if strings.Contains(strVal, ".") {
		f, err := strconv.ParseFloat(strVal, 64)
		if err != nil {
			return sql.Null, false
		}

		if math.Abs(f) < math.MaxFloat32 {
			ti = sql.Float32
		} else {
			ti = sql.Float64
		}

		if floatThreshold != 0.0 {
			floatParts := strings.Split(strVal, ".")
			decimalPart, err := strconv.ParseFloat("0."+floatParts[1], 64)

			if err != nil {
				panic(err)
			}

			if decimalPart < floatThreshold {
				if ti == sql.Float32 {
					ti = sql.Int32
				} else {
					ti = sql.Int64
				}
			}
		}
		return ti, true
	}

	if strings.Contains(strVal, "-") {
		i, err := strconv.ParseInt(strVal, 10, 64)
		if err != nil {
			return sql.Null, false
		}
		if i >= math.MinInt32 && i <= math.MaxInt32 {
			return sql.Int32, true
		} else {
			return sql.Int64, true
		}
	} else {
		ui, err := strconv.ParseUint(strVal, 10, 64)
		if err != nil {
			return sql.Null, false
		}

		// handle leading zero case
		if len(strVal) > 1 && strVal[0] == '0' {
			return sql.Text, true
		}

		if ui <= math.MaxUint32 {
			return sql.Uint32, true
		} else {
			return sql.Uint64, true
		}
	}
}

func leastPermissiveSqlChronoType(strVal string) (sql.Type, bool) {
	if strVal == "" {
		return sql.Null, false
	}

	// TODO: Replace this lololol
	_, err := typeinfo.TimeType.ParseValue(context.Background(), nil, &strVal)
	if err == nil {
		return sql.Time, true
	}

	dt, err := typeinfo.DatetimeType.ParseValue(context.Background(), nil, &strVal)
	if err != nil {
		return sql.Datetime, true
	}

	t := time.Time(dt.(types.Timestamp))
	if t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 {
		return sql.Date, true
	}

	return sql.Datetime, true
}


// TOOD: Move this to the engine
func sqlChronoTypes() []sql.Type {
	return []sql.Type{
		sql.Year,
		sql.Date,
		sql.Datetime,
		sql.Time,
		sql.Timestamp,
	}
}

func sqlNumericTypes() []sql.Type {
	return []sql.Type{
		sql.Int32,
		sql.Uint32,
		sql.Int64,
		sql.Uint64,
		sql.Float32,
		sql.Float64,
	}
}

// findCommonType takes a set of types and finds the least permissive
// (ie most specific) common type between all types in the set
func findCommonSQlType(ts sqlTypeInfoSet) sql.Type {

	// empty values were inferred as UnknownType
	delete(ts, typeinfo.UnknownType)

	if len(ts) == 0 {
		// use strings if all values were empty
		return sql.Text
	}

	if len(ts) == 1 {
		for ti := range ts {
			return ti
		}
	}

	// len(ts) > 1

	if _, found := ts[sql.Text]; found {
		return sql.Text
	}

	hasNumeric := false
	for _, nt := range sqlNumericTypes() {
		if _, found := ts[nt]; found {
			hasNumeric = true
			break
		}
	}

	hasNonNumeric := false
	for _, nnt := range sqlChronoTypes() {
		if _, found := ts[nnt]; found {
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


func sqlSetHasType(ts sqlTypeInfoSet, t sql.Type) bool {
	_, found := ts[t]
	return found
}




