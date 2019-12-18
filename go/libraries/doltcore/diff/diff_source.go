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

package diff

import (
	"errors"
	"io"
	"time"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	From = "from"
	To   = "to"
)

type RowDiffSource struct {
	ad         *AsyncDiffer
	joiner     *rowconv.Joiner
	oldRowConv *rowconv.RowConverter
	newRowConv *rowconv.RowConverter
}

func NewRowDiffSource(ad *AsyncDiffer, joiner *rowconv.Joiner) *RowDiffSource {
	return &RowDiffSource{
		ad,
		joiner,
		rowconv.IdentityConverter,
		rowconv.IdentityConverter,
	}
}

func (rdRd *RowDiffSource) AddInputRowConversion(oldConv, newConv *rowconv.RowConverter) {
	rdRd.oldRowConv = oldConv
	rdRd.newRowConv = newConv
}

// GetSchema gets the schema of the rows that this reader will return
func (rdRd *RowDiffSource) GetSchema() schema.Schema {
	return rdRd.joiner.GetSchema()
}

// NextDiff reads a row from a table.  If there is a bad row the returned error will be non nil, and callin IsBadRow(err)
// will be return true. This is a potentially non-fatal error and callers can decide if they want to continue on a bad row, or fail.
func (rdRd *RowDiffSource) NextDiff() (row.Row, pipeline.ImmutableProperties, error) {
	if rdRd.ad.isDone {
		return nil, pipeline.NoProps, io.EOF
	}

	diffs, err := rdRd.ad.GetDiffs(1, time.Second)

	if err != nil {
		return nil, pipeline.ImmutableProperties{}, err
	}

	if len(diffs) == 0 {
		if rdRd.ad.isDone {
			return nil, pipeline.NoProps, io.EOF
		}

		return nil, pipeline.NoProps, errors.New("timeout")
	}

	if len(diffs) != 1 {
		panic("only a single diff requested, multiple returned.  bug in AsyncDiffer")
	}

	d := diffs[0]
	rows := make(map[string]row.Row)
	if d.OldValue != nil {
		sch := rdRd.joiner.SchemaForName(From)
		if !rdRd.oldRowConv.IdentityConverter {
			sch = rdRd.oldRowConv.SrcSch
		}

		oldRow, err := row.FromNoms(sch, d.KeyValue.(types.Tuple), d.OldValue.(types.Tuple))

		if err != nil {
			return nil, pipeline.ImmutableProperties{}, err
		}

		rows[From], err = rdRd.oldRowConv.Convert(oldRow)

		if err != nil {
			return nil, pipeline.NoProps, err
		}
	}

	if d.NewValue != nil {
		sch := rdRd.joiner.SchemaForName(To)
		if !rdRd.newRowConv.IdentityConverter {
			sch = rdRd.newRowConv.SrcSch
		}

		newRow, err := row.FromNoms(sch, d.KeyValue.(types.Tuple), d.NewValue.(types.Tuple))

		if err != nil {
			return nil, pipeline.ImmutableProperties{}, err
		}

		rows[To], err = rdRd.newRowConv.Convert(newRow)

		if err != nil {
			return nil, pipeline.NoProps, err
		}
	}

	joinedRow, err := rdRd.joiner.Join(rows)

	if err != nil {
		return nil, pipeline.ImmutableProperties{}, err
	}

	return joinedRow, pipeline.ImmutableProperties{}, nil
}

// Close should release resources being held
func (rdRd *RowDiffSource) Close() error {
	rdRd.ad.Close()
	return nil
}
