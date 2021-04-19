// Copyright 2020 Dolthub, Inc.
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

package noms

import (
	"context"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// InRangeCheck is a call made as the reader reads through values to check that the next value
// being read is in the range]
type InRangeCheck func(tuple types.Tuple) (bool, error)

// ReadRange represents a range of values to be read
type ReadRange struct {
	// Start is a Dolt map key which is the starting point (or ending point if Reverse is true)
	Start types.Tuple
	// Inclusive says whether the Start key should be included in the range.
	Inclusive bool
	// Reverse says if the range should be read in reverse (from high to low) instead of the default (low to high)
	Reverse bool
	// Check is a callb made as the reader reads through values to check that the next value being read is in the range.
	Check InRangeCheck
}

// NewRangeEndingAt creates a range with a starting key which will be iterated in reverse
func NewRangeEndingAt(key types.Tuple, inRangeCheck InRangeCheck) *ReadRange {
	return &ReadRange{
		Start:     key,
		Inclusive: true,
		Reverse:   true,
		Check:     inRangeCheck,
	}
}

// NewRangeEndingBefore creates a range starting before the provided key iterating in reverse
func NewRangeEndingBefore(key types.Tuple, inRangeCheck InRangeCheck) *ReadRange {
	return &ReadRange{
		Start:     key,
		Inclusive: false,
		Reverse:   true,
		Check:     inRangeCheck,
	}
}

// NewRangeStartingAt creates a range with a starting key
func NewRangeStartingAt(key types.Tuple, inRangeCheck InRangeCheck) *ReadRange {
	return &ReadRange{
		Start:     key,
		Inclusive: true,
		Reverse:   false,
		Check:     inRangeCheck,
	}
}

// NewRangeStartingAfter creates a range starting after the provided key
func NewRangeStartingAfter(key types.Tuple, inRangeCheck InRangeCheck) *ReadRange {
	return &ReadRange{
		Start:     key,
		Inclusive: false,
		Reverse:   false,
		Check:     inRangeCheck,
	}
}

// NomsRangeReader reads values in one or more ranges from a map
type NomsRangeReader struct {
	sch       schema.Schema
	m         types.Map
	ranges    []*ReadRange
	idx       int
	itr       types.MapIterator
	currCheck InRangeCheck
}

// NewNomsRangeReader creates a NomsRangeReader
func NewNomsRangeReader(sch schema.Schema, m types.Map, ranges []*ReadRange) *NomsRangeReader {
	return &NomsRangeReader{
		sch,
		m,
		ranges,
		0,
		nil,
		nil,
	}
}

// GetSchema gets the schema of the rows being read.
func (nrr *NomsRangeReader) GetSchema() schema.Schema {
	return nrr.sch
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and calling
// IsBadRow(err) will be return true. This is a potentially non-fatal error and callers can decide if they want to
// continue on a bad row, or fail.
func (nrr *NomsRangeReader) ReadRow(ctx context.Context) (row.Row, error) {
	k, v, err := nrr.ReadKV(ctx)

	if err != nil {
		return nil, err
	}

	return row.FromNoms(nrr.sch, k, v)
}

func (nrr *NomsRangeReader) ReadKey(ctx context.Context) (types.Tuple, error) {
	k, _, err := nrr.ReadKV(ctx)

	return k, err
}

func (nrr *NomsRangeReader) ReadKV(ctx context.Context) (types.Tuple, types.Tuple, error) {
	var err error
	var k types.Tuple
	var v types.Tuple
	for nrr.itr != nil || nrr.idx < len(nrr.ranges) {
		if nrr.itr == nil {
			r := nrr.ranges[nrr.idx]
			nrr.idx++

			if r.Reverse {
				nrr.itr, err = nrr.m.IteratorBackFrom(ctx, r.Start)
			} else {
				nrr.itr, err = nrr.m.IteratorFrom(ctx, r.Start)
			}

			if err != nil {
				return types.Tuple{}, types.Tuple{}, err
			}

			nrr.currCheck = r.Check

			k, v, err = nrr.itr.NextTuple(ctx)

			if err == nil && !r.Inclusive && r.Start.Compare(k) == 0 {
				k, v, err = nrr.itr.NextTuple(ctx)
			}
		} else {
			k, v, err = nrr.itr.NextTuple(ctx)
		}

		if err != nil && err != io.EOF {
			return types.Tuple{}, types.Tuple{}, err
		}

		var inRange bool
		if err != io.EOF {
			inRange, err = nrr.currCheck(k)

			if err != nil {
				return types.Tuple{}, types.Tuple{}, err
			}

			if inRange {
				return k, v, nil
			}
		}

		nrr.itr = nil
		nrr.currCheck = nil
	}

	return types.Tuple{}, types.Tuple{}, io.EOF
}

// VerifySchema checks that the incoming schema matches the schema from the existing table
func (nrr *NomsRangeReader) VerifySchema(outSch schema.Schema) (bool, error) {
	return schema.VerifyInSchema(nrr.sch, outSch)
}

// Close should release resources being held
func (nrr *NomsRangeReader) Close(ctx context.Context) error {
	return nil
}

// SqlRowFromTuples constructs a go-mysql-server/sql.Row from Noms tuples.
func SqlRowFromTuples(sch schema.Schema, key, val types.Tuple) (sql.Row, error) {
	allCols := sch.GetAllCols()
	colVals := make(sql.Row, allCols.Size())

	keySl, err := key.AsSlice()
	if err != nil {
		return nil, err
	}
	valSl, err := val.AsSlice()
	if err != nil {
		return nil, err
	}

	for _, sl := range []types.TupleValueSlice{keySl, valSl} {
		var convErr error
		err := row.IterPkTuple(sl, func(tag uint64, val types.Value) (stop bool, err error) {
			if idx, ok := allCols.TagToIdx[tag]; ok {
				col := allCols.GetByIndex(idx)
				colVals[idx], convErr = col.TypeInfo.ConvertNomsValueToValue(val)

				if convErr != nil {
					return false, err
				}
			}

			return false, nil
		})

		if err != nil {
			return nil, err
		}
	}

	return sql.NewRow(colVals...), nil
}
