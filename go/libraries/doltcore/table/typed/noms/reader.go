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

package noms

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

type StatsCB func(stats types.AppliedEditStats)

// NomsMapReader is a TableReader that reads rows from a noms table which is stored in a types.Map where the key is
// a types.Value and the value is a types.Tuple of field values.
type NomsMapReader struct {
	sch schema.Schema
	itr types.MapIterator
}

// NewNomsMapReader creates a NomsMapReader for a given noms types.Map
func NewNomsMapReader(ctx context.Context, m types.Map, sch schema.Schema) (*NomsMapReader, error) {
	itr, err := m.Iterator(ctx)

	if err != nil {
		return nil, err
	}

	return &NomsMapReader{sch, itr}, nil
}

// GetSchema gets the schema of the rows that this reader will return
func (nmr *NomsMapReader) GetSchema() schema.Schema {
	return nmr.sch
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and callin IsBadRow(err)
// will be return true. This is a potentially non-fatal error and callers can decide if they want to continue on a bad row, or fail.
func (nmr *NomsMapReader) ReadRow(ctx context.Context) (row.Row, error) {
	key, val, err := nmr.itr.Next(ctx)

	if err != nil {
		return nil, err
	} else if key == nil {
		return nil, io.EOF
	}

	return row.FromNoms(nmr.sch, key.(types.Tuple), val.(types.Tuple))
}

// Close should release resources being held
func (nmr *NomsMapReader) Close(ctx context.Context) error {
	nmr.itr = nil
	return nil
}

// VerifySchema checks that the incoming schema matches the schema from the existing table
func (nmr *NomsMapReader) VerifySchema(outSch schema.Schema) (bool, error) {
	return schema.VerifyInSchema(nmr.sch, outSch)
}
