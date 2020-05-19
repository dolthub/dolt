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
	"github.com/liquidata-inc/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
)

type IndexLookupKeyIterator interface {
	// NextKey returns the next key if it exists, and io.EOF if it does not.
	NextKey(ctx *sql.Context) (row.TaggedValues, error)
}

type doltIndexLookup struct {
	idx     DoltIndex
	keyIter IndexLookupKeyIterator
}

func (il *doltIndexLookup) String() string {
	// TODO: fix
	return fmt.Sprintf("%s:%s", il.idx.ID(), "")
}

var _ sql.IndexLookup = (*doltIndexLookup)(nil)

func (il *doltIndexLookup) Indexes() []string {
	return []string{il.idx.ID()}
}

// RowIter returns a row iterator for this index lookup. The iterator will return the single matching row for the index.
func (il *doltIndexLookup) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	return &indexLookupRowIterAdapter{indexLookup: il, ctx: ctx}, nil
}

type doltIndexKeyIter struct {
	indexMapIter table.TableReadCloser
}

var _ IndexLookupKeyIterator = (*doltIndexKeyIter)(nil)

func (iter *doltIndexKeyIter) NextKey(ctx *sql.Context) (row.TaggedValues, error) {
	indexRow, err := iter.indexMapIter.ReadRow(ctx)
	if err != nil {
		return nil, err
	}
	return row.GetTaggedVals(indexRow)
}
