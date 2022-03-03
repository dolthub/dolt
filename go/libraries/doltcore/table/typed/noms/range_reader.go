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
	"errors"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// InRangeCheck evaluates tuples to determine whether they are valid and/or should be skipped.
type InRangeCheck interface {
	// Check is a call made as the reader reads through values to check that the next value either being read is valid
	// and whether it should be skipped or returned.
	Check(ctx context.Context, tuple types.Tuple) (valid bool, skip bool, err error)
}

// InRangeCheckAlways will always return that the given tuple is valid and not to be skipped.
type InRangeCheckAlways struct{}

func (InRangeCheckAlways) Check(context.Context, types.Tuple) (valid bool, skip bool, err error) {
	return true, false, nil
}

// InRangeCheckNever will always return that the given tuple is not valid.
type InRangeCheckNever struct{}

func (InRangeCheckNever) Check(context.Context, types.Tuple) (valid bool, skip bool, err error) {
	return false, false, nil
}

// InRangeCheckPartial will check if the given tuple contains the aliased tuple as a partial key.
type InRangeCheckPartial types.Tuple

func (ircp InRangeCheckPartial) Check(_ context.Context, t types.Tuple) (valid bool, skip bool, err error) {
	return t.StartsWith(types.Tuple(ircp)), false, nil
}

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
	sch         schema.Schema
	m           types.Map
	ranges      []*ReadRange
	idx         int
	itr         types.MapIterator
	currCheck   InRangeCheck
	cardCounter *CardinalityCounter
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
		NewCardinalityCounter(),
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
	nbf := nrr.m.Format()

	var err error
	var k types.Tuple
	var v types.Tuple
	for nrr.itr != nil || nrr.idx < len(nrr.ranges) {
		if !nrr.cardCounter.empty() {
			if nrr.cardCounter.done() {
				nrr.cardCounter.reset()
			} else {
				return nrr.cardCounter.next()
			}
		}

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

			if err == nil && !r.Inclusive {
				var res int
				res, err = r.Start.Compare(nbf, k)
				if err == nil && res == 0 {
					k, v, err = nrr.itr.NextTuple(ctx)
				}
			}
		} else {
			k, v, err = nrr.itr.NextTuple(ctx)
		}

		if err != nil && err != io.EOF {
			return types.Tuple{}, types.Tuple{}, err
		}

		if err != io.EOF {
			valid, skip, err := nrr.currCheck.Check(ctx, k)
			if err != nil {
				return types.Tuple{}, types.Tuple{}, err
			}

			if valid {
				if skip {
					continue
				}
				if !v.Empty() {
					nrr.cardCounter.updateWithKV(k, v)
					if !nrr.cardCounter.empty() && !nrr.cardCounter.done() {
						return nrr.cardCounter.next()
					}
				}
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

type CardinalityCounter struct {
	key   *types.Tuple
	value *types.Tuple
	card  int
	idx   int
}

func NewCardinalityCounter() *CardinalityCounter {
	return &CardinalityCounter{
		nil,
		nil,
		-1,
		-1,
	}
}

func (cc *CardinalityCounter) updateWithKV(k, v types.Tuple) error {
	if !v.Empty() {
		cardTagVal, err := v.Get(0)
		if err != nil {
			return err
		}
		cardTag, ok := cardTagVal.(types.Uint)
		if !ok {
			return errors.New("index cardinality invalid tag type")
		}

		if uint64(cardTag) != schema.KeylessRowCardinalityTag {
			return errors.New("index cardinality tag invalid")
		}

		cardVal, err := v.Get(1)
		if err != nil {
			return err
		}
		card, ok := cardVal.(types.Uint)
		if !ok {
			return errors.New("index cardinality value invalid type")
		}
		if int(card) > 1 {
			cc.card = int(card)
			cc.idx = 0
			cc.key = &k
			cc.value = &v
			return nil
		} else {
			cc.card = -1
			cc.idx = -1
			cc.key = nil
			cc.value = nil
		}
	}
	return nil
}

func (cc *CardinalityCounter) empty() bool {
	return cc.key == nil || cc.value == nil
}

func (cc *CardinalityCounter) done() bool {
	return cc.card < 1 || cc.idx >= cc.card
}

func (cc *CardinalityCounter) next() (types.Tuple, types.Tuple, error) {
	if cc.key == nil || cc.value == nil {
		return types.Tuple{}, types.Tuple{}, errors.New("cannot increment empty cardinality counter")
	}
	cc.idx++
	return *cc.key, *cc.value, nil

}

func (cc *CardinalityCounter) reset() {
	cc.card = -1
	cc.idx = -1
	cc.key = nil
	cc.value = nil
}
