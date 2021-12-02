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
	"github.com/dolthub/go-mysql-server/sql"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// KeyIterator is an interface for iterating through a collection of keys
type KeyIterator interface {
	// Next returns the next key in the collection. When all keys are exhausted nil, io.EOF must be returned.
	Next() (types.Value, error)
}

// SliceOfKeysIterator is a KeyIterator implementation backed by a slice of keys which are iterated in order
type SliceOfKeysIterator struct {
	keys []types.Tuple
	idx  int
}

// Next returns the next key in the slice. When all keys are exhausted nil, io.EOF is be returned.
func (sokItr *SliceOfKeysIterator) Next() (types.Value, error) {
	if sokItr.idx < len(sokItr.keys) {
		k := sokItr.keys[sokItr.idx]
		sokItr.idx++

		return k, nil
	}

	return nil, io.EOF
}

// NomsMapReaderForKeys implements TableReadCloser
type NomsMapReaderForKeys struct {
	sch    schema.Schema
	m      types.Map
	keyItr KeyIterator
}

// NewNomsMapReaderForKeys creates a NomsMapReaderForKeys for a given noms types.Map, and a list of keys
func NewNomsMapReaderForKeys(m types.Map, sch schema.Schema, keys []types.Tuple) *NomsMapReaderForKeys {
	return NewNomsMapReaderForKeyItr(m, sch, &SliceOfKeysIterator{keys, 0})
}

// NewNomsMapReaderForKeyItr creates a NomsMapReaderForKeys for a given noms types.Map, and a list of keys
func NewNomsMapReaderForKeyItr(m types.Map, sch schema.Schema, keyItr KeyIterator) *NomsMapReaderForKeys {
	return &NomsMapReaderForKeys{sch, m, keyItr}
}

// GetSchema gets the schema of the rows being read.
func (nmr *NomsMapReaderForKeys) GetSchema() schema.Schema {
	return nmr.sch
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and calling
// IsBadRow(err) will be return true. This is a potentially non-fatal error and callers can decide if they want to
// continue on a bad row, or fail.
func (nmr *NomsMapReaderForKeys) ReadRow(ctx context.Context) (row.Row, error) {
	var key types.Value
	var value types.Value
	var err error
	for value == nil {
		key, err = nmr.keyItr.Next()

		if err != nil {
			return nil, err
		}

		v, ok, err := nmr.m.MaybeGet(ctx, key)

		if err != nil {
			return nil, err
		}

		if ok {
			value = v
		}
	}

	return row.FromNoms(nmr.sch, key.(types.Tuple), value.(types.Tuple))
}


func (nmr *NomsMapReaderForKeys) GetSqlSchema() sql.Schema {
	panic("cant do this")
}

func (nmr *NomsMapReaderForKeys) ReadSqlRow(ctx context.Context) (sql.Row, error) {
	panic("cant do this")
}

// VerifySchema checks that the incoming schema matches the schema from the existing table
func (nmr *NomsMapReaderForKeys) VerifySchema(outSch schema.Schema) (bool, error) {
	return schema.VerifyInSchema(nmr.sch, outSch)
}

// Close should release resources being held
func (nmr *NomsMapReaderForKeys) Close(ctx context.Context) error {
	return nil
}
