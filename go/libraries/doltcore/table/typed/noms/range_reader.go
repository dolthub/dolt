// Copyright 2020 Liquidata, Inc.
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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
	"io"
)

type InRangeCheck func(tuple types.Tuple) (bool, error)

type ReadRange struct {
	Start     types.Tuple
	Inclusive bool
	Reverse   bool
	Check     InRangeCheck
}

func NewRangeEndingAt(key types.Tuple, inRangeCheck InRangeCheck) *ReadRange {
	return &ReadRange{
		Start:     key,
		Inclusive: true,
		Reverse:   true,
		Check:     inRangeCheck,
	}
}

func NewRangeEndingBefore(key types.Tuple, inRangeCheck InRangeCheck) *ReadRange {
	return &ReadRange{
		Start:     key,
		Inclusive: false,
		Reverse:   true,
		Check:     inRangeCheck,
	}
}

func NewRangeStartingAt(key types.Tuple, inRangeCheck InRangeCheck) *ReadRange {
	return &ReadRange{
		Start:     key,
		Inclusive: true,
		Reverse:   false,
		Check:     inRangeCheck,
	}
}

func NewRangeStartingAfter(key types.Tuple, inRangeCheck InRangeCheck) *ReadRange {
	return &ReadRange{
		Start:     key,
		Inclusive: false,
		Reverse:   false,
		Check:     inRangeCheck,
	}
}

type NomsRangeReader struct {
	sch       schema.Schema
	m         types.Map
	ranges    []*ReadRange
	idx       int
	itr       types.MapIterator
	currCheck InRangeCheck
}

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

func (nrr *NomsRangeReader) GetSchema() schema.Schema {
	return nrr.sch
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and calling
// IsBadRow(err) will be return true. This is a potentially non-fatal error and callers can decide if they want to
// continue on a bad row, or fail.
func (nrr *NomsRangeReader) ReadRow(ctx context.Context) (row.Row, error) {
	var err error
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
				return nil, err
			}

			nrr.currCheck = r.Check
		}

		k, v, err := nrr.itr.Next(ctx)

		if err != nil {
			return nil, err
		}

		var inRange bool
		if k != nil {
			inRange, err = nrr.currCheck(k.(types.Tuple))

			if err != nil {
				return nil, err
			}

			if !inRange {
				nrr.itr = nil
				nrr.currCheck = nil
				continue
			} else {
				return row.FromNoms(nrr.sch, k.(types.Tuple), v.(types.Tuple))
			}
		}

	}

	return nil, io.EOF
}

// VerifySchema checks that the incoming schema matches the schema from the existing table
func (nrr *NomsRangeReader) VerifySchema(outSch schema.Schema) (bool, error) {
	return schema.VerifyInSchema(nrr.sch, outSch)
}

// Close should release resources being held
func (nrr *NomsRangeReader) Close(ctx context.Context) error {
	return nil
}
