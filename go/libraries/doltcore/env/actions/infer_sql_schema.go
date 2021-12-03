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
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
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

func (inf *sqlInferrer) inferColumnTypes(ctx context.Context) sql.Schema {
	inferredTypes := make(map[string]sql.Type)
	for colName, typ := range inf.inferSets {
		inferredTypes[inf.mapper.Map(colName)] = findCommonSQlType(typ)
	}

	var ret sql.Schema
	for _, col := range inf.readerSch {
		col.Name = inf.mapper.Map(col.Name)
		col.Type = inferredTypes[col.Name]
		// TODO: col.source
		if inf.nullable.Contains(col.Name) {
			col.Nullable = true
		}
		ret = append(ret, col)
	}

	return ret
}

func InferSqlSchemaFromTableReader(ctx context.Context, rd table.TableReadCloser, args InferenceArgs) (sql.Schema, error) {
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

				if strVal == nil {
					inferrer.inferSets[col.Name][sql.Null] = struct{}{}
				} else {
					typ := sqlLeastPermissiveType(strVal.(string), inferrer.floatThreshold) // TODO: This is sus
					inferrer.inferSets[col.Name][typ] = struct{}{}
				}
			}
		}

		return nil
	})

	err := g.Wait()
	if err != nil {
		return nil, err
	}

	err = rd.Close(ctx)
	if err != nil {
		return nil, err
	}

	return inferrer.inferColumnTypes(ctx), nil
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

	return sql.Text // be more rigorous with string type
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
		return sql.Null, false
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
	delete(ts, sql.Null)

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
	if sqlSetHasType(ts, sql.Boolean) || sqlSetHasType(ts, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 36)) {
		hasNonNumeric = true
	}

	if hasNumeric && hasNonNumeric {
		return sql.Text
	}

	if hasNumeric {
		return findCommonSqlNumericType(ts)
	}

	// find a common nonNumeric type

	nonChronoTypes := []sql.Type{
		// todo: BIT implementation parses all uint8
		//typeinfo.PseudoBoolType,
		sql.Boolean,
		sql.MustCreateStringWithDefaults(sqltypes.VarChar, 36),
	}
	for _, nct := range nonChronoTypes {
		if sqlSetHasType(ts, nct) {
			// types in nonChronoTypes have only string
			// as a common type with any other type
			return sql.Text
		}
	}

	return findCommonSqlChronoType(ts)
}

func findCommonSqlNumericType(nums sqlTypeInfoSet) sql.Type {
	// find a common numeric type
	// iterate through types from most to least permissive
	// return the most permissive type found
	//   ints are a subset of floats
	//   uints are a subset of ints
	//   smaller widths are a subset of larger widths
	mostToLeast := []sql.Type{
		sql.Float64,
		sql.Float32,

		// todo: can all Int64 fit in Float64?
		sql.Int64,
		sql.Int32,
		sql.Int24,
		sql.Int16,
		sql.Int8,

		sql.Uint64,
		sql.Uint32,
		sql.Uint24,
		sql.Uint16,
		sql.Uint8,
	}
	for _, numType := range mostToLeast {
		if sqlSetHasType(nums, numType) {
			return numType
		}
	}

	panic("unreachable")
}

func findCommonSqlChronoType(chronos sqlTypeInfoSet) sql.Type {
	if len(chronos) == 1 {
		for ct := range chronos {
			return ct
		}
	}

	if sqlSetHasType(chronos, sql.Datetime) {
		return sql.Datetime
	}

	hasTime := sqlSetHasType(chronos, sql.Time) || sqlSetHasType(chronos, sql.Timestamp)
	hasDate := sqlSetHasType(chronos, sql.Date) || sqlSetHasType(chronos, sql.Year)

	if hasTime && !hasDate {
		return sql.Time
	}

	if !hasTime && hasDate {
		return sql.Date
	}

	if hasDate && hasTime {
		return sql.Datetime
	}

	panic("unreachable")
}

func sqlSetHasType(ts sqlTypeInfoSet, t sql.Type) bool {
	_, found := ts[t]
	return found
}
