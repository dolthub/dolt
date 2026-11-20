// Copyright 2026 Dolthub, Inc.
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

package index

import (
	"context"

	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// RowLocker locks the rows of one table as they are read, for one statement. A row is identified by its key in the
// table's primary index, which is the key every reader of the table has in hand or can make from an index entry
// without decoding the row.
type RowLocker interface {
	// LockRow locks the row whose key in the table's primary index is |key|. It returns false, with no error, for
	// a row that could not be locked and is to be left out of what is read.
	LockRow(ctx context.Context, key val.Tuple) (bool, error)
}

// NewRowLockingMapIter returns an iterator over the entries of |iter| that locks the row of each entry before
// returning it. |iter| must iterate a table's primary index. A nil |locker| returns |iter| itself.
func NewRowLockingMapIter(iter prolly.MapIter, locker RowLocker) prolly.MapIter {
	if locker == nil {
		return iter
	}

	return &rowLockingMapIter{iter: iter, locker: locker}
}

// NewRowLockingIndexIter returns an iterator over the entries of |iter| that locks the row of each entry before
// returning it. |iter| must iterate the storage of |idx|, which can be any index of its table. A nil |locker|
// returns |iter| itself.
func NewRowLockingIndexIter(iter prolly.MapIter, locker RowLocker, idx DoltIndex, ns tree.NodeStore) prolly.MapIter {
	if locker == nil {
		return iter
	}

	locking := newRowLockingIndexIter(locker, idx, ns)
	locking.iter = iter
	return locking
}

// newRowLockingIndexIter returns a rowLockingMapIter for the entries of |idx|, with no iterator to read yet.
func newRowLockingIndexIter(locker RowLocker, idx DoltIndex, ns tree.NodeStore) *rowLockingMapIter {
	locking := &rowLockingMapIter{locker: locker}
	if !idx.IsPrimaryKey() {
		// Every other index ends its keys with the fields of the primary index key.
		locking.pkMap = OrdinalMappingFromIndex(idx)
		locking.pkBld = val.NewTupleBuilder(idx.Schema().GetKeyDescriptor(ns), ns)
	}

	return locking
}

// rowLockingMapIter returns the entries of another iterator after locking the row of each. An entry whose row
// can't be locked is left out.
type rowLockingMapIter struct {
	iter   prolly.MapIter
	locker RowLocker
	// pkMap and pkBld make a primary index key from the key of an entry. They are unset when the entries are those
	// of the primary index.
	pkMap val.OrdinalMapping
	pkBld *val.TupleBuilder
}

var _ prolly.MapIter = (*rowLockingMapIter)(nil)

// Next implements prolly.MapIter.
func (i *rowLockingMapIter) Next(ctx context.Context) (val.Tuple, val.Tuple, error) {
	for {
		key, value, err := i.iter.Next(ctx)
		if err != nil || key == nil {
			return key, value, err
		}

		pk := key
		if i.pkBld != nil {
			for to := range i.pkMap {
				i.pkBld.PutRaw(to, key.GetField(i.pkMap.MapOrdinal(to)))
			}

			pk, err = i.pkBld.Build(ctx, sharePool)
			if err != nil {
				return nil, nil, err
			}
		}

		locked, err := i.locker.LockRow(ctx, pk)
		if err != nil {
			return nil, nil, err
		}

		if locked {
			return key, value, nil
		}
	}
}

// rowLockingLookupGen makes the iterators of another SecondaryLookupIterGen lock the rows they return. Like the
// generators it wraps, it supports one open iterator at a time.
type rowLockingLookupGen struct {
	SecondaryLookupIterGen
	iter *rowLockingMapIter
}

var _ SecondaryLookupIterGen = (*rowLockingLookupGen)(nil)

// New implements SecondaryLookupIterGen.
func (g *rowLockingLookupGen) New(ctx context.Context, key val.Tuple) (prolly.MapIter, error) {
	iter, err := g.SecondaryLookupIterGen.New(ctx, key)
	if err != nil {
		return nil, err
	}

	g.iter.iter = iter
	return g.iter, nil
}
