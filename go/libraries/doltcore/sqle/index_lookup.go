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

package sqle

import (
	"fmt"
	"io"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type IndexLookupKeyIterator interface {
	// NextKey returns the next key if it exists, and io.EOF if it does not.
	NextKey(ctx *sql.Context) (row.TaggedValues, error)
}

type doltIndexLookup struct {
	idx     DoltIndex
	keyIter IndexLookupKeyIterator
}

func (il *doltIndexLookup) Indexes() []string {
	return []string{il.idx.ID()}
}

// No idea what this is used for, examples aren't useful. From stepping through the code I know that we get index values
// by wrapping tables via the WithIndexLookup method. The iterator that this method returns yields []byte instead of
// sql.Row and its purpose is yet unclear.
func (il *doltIndexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	panic("implement me")
}

// RowIter returns a row iterator for this index lookup. The iterator will return the single matching row for the index.
func (il *doltIndexLookup) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	return &indexLookupRowIterAdapter{indexLookup: il, ctx: ctx}, nil
}

type doltIndexSinglePkKeyIter struct {
	hasReturned bool
	val         row.TaggedValues
}

var _ IndexLookupKeyIterator = (*doltIndexSinglePkKeyIter)(nil)

func (iter *doltIndexSinglePkKeyIter) NextKey(*sql.Context) (row.TaggedValues, error) {
	if iter.hasReturned {
		return nil, io.EOF
	}
	iter.hasReturned = true
	return iter.val, nil
}

type doltIndexMultiPkKeyIter struct {
	tableName    string
	tableMapIter types.MapIterator
	val          row.TaggedValues
}

var _ IndexLookupKeyIterator = (*doltIndexMultiPkKeyIter)(nil)

func (iter *doltIndexMultiPkKeyIter) NextKey(ctx *sql.Context) (row.TaggedValues, error) {
	var k types.Value
	var err error
IterateOverMap:
	for k, _, err = iter.tableMapIter.Next(ctx); k != nil && err == nil; k, _, err = iter.tableMapIter.Next(ctx) {
		key, err := row.ParseTaggedValues(k.(types.Tuple))
		if err != nil {
			return nil, err
		}
		for tag, val := range iter.val {
			indexVal, ok := key[tag]
			if !ok {
				return nil, fmt.Errorf("on table `%s`, attempted to gather value for tag `%v`", iter.tableName, tag)
			}
			if !val.Equals(indexVal) {
				continue IterateOverMap
			}
		}
		return key, nil
	}
	if err != nil {
		return nil, err
	}
	return nil, io.EOF
}

type doltIndexKeyIter struct {
	index        schema.InnerIndex
	indexMapIter types.MapIterator
	val          row.TaggedValues
}

var _ IndexLookupKeyIterator = (*doltIndexKeyIter)(nil)

func (iter *doltIndexKeyIter) NextKey(ctx *sql.Context) (row.TaggedValues, error) {
	var k types.Value
	var err error
IterateOverMap:
	for k, _, err = iter.indexMapIter.Next(ctx); k != nil && err == nil; k, _, err = iter.indexMapIter.Next(ctx) {
		indexKeyTaggedValues, err := row.ParseTaggedValues(k.(types.Tuple))
		if err != nil {
			return nil, err
		}
		for tag, val := range iter.val {
			indexVal, ok := indexKeyTaggedValues[tag]
			if !ok {
				return nil, fmt.Errorf("on index `%s`, attempted to gather value for tag `%v`", iter.index.Name(), tag)
			}
			if !val.Equals(indexVal) {
				continue IterateOverMap
			}
		}
		primaryKeys := make(row.TaggedValues)
		for _, tag := range iter.index.PrimaryKeys() {
			primaryKeys[tag] = indexKeyTaggedValues[tag]
		}
		return primaryKeys, nil
	}
	if err != nil {
		return nil, err
	}
	return nil, io.EOF
}
