// Copyright 2022 Dolthub, Inc.
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

package dtables

import (
	"io"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/hash"
)

const tagsDefaultRowCount = 10

var _ sql.Table = (*TagsTable)(nil)
var _ sql.IndexAddressableTable = (*TagsTable)(nil)
var _ sql.StatisticsTable = (*TagsTable)(nil)

// TagsTable is a sql.Table implementation that implements a system table which shows the dolt tags
type TagsTable struct {
	db        dsess.SqlDatabase
	tableName string
}

// NewTagsTable creates a TagsTable
func NewTagsTable(_ *sql.Context, tableName string, db dsess.SqlDatabase) sql.Table {
	return &TagsTable{tableName: tableName, db: db}
}

func (tt *TagsTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(tt.Schema(ctx))
	numRows, _, err := tt.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (tt *TagsTable) RowCount(_ *sql.Context) (uint64, bool, error) {
	return tagsDefaultRowCount, false, nil
}

// Name is a sql.Table interface function which returns the name of the table.
func (tt *TagsTable) Name() string {
	return tt.tableName
}

// String is a sql.Table interface function which returns the name of the table.
func (tt *TagsTable) String() string {
	return tt.tableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the tags system table.
func (tt *TagsTable) Schema(ctx *sql.Context) sql.Schema {
	return []*sql.Column{
		{Name: "tag_name", Type: types.Text, Source: tt.tableName, PrimaryKey: true},
		{Name: "tag_hash", Type: types.Text, Source: tt.tableName, PrimaryKey: true},
		{Name: "tagger", Type: types.Text, Source: tt.tableName, PrimaryKey: false},
		{Name: "email", Type: types.Text, Source: tt.tableName, PrimaryKey: false},
		{Name: "date", Type: types.Datetime3, Source: tt.tableName, PrimaryKey: false},
		{Name: "message", Type: types.Text, Source: tt.tableName, PrimaryKey: false},
	}
}

// Collation implements the sql.Table interface.
func (tt *TagsTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data. Currently, the data is unpartitioned.
func (tt *TagsTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (tt *TagsTable) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	refs, root, err := tt.tagRefs(ctx)
	if err != nil {
		return nil, err
	}
	return &indexedTagsIter{ddb: tt.db.DbData().Ddb, refs: refs, root: root}, nil
}

func (tt *TagsTable) tagRefs(ctx *sql.Context) ([]ref.DoltRef, hash.Hash, error) {
	root, err := dsess.TransactionRoot(ctx, tt.db)
	if err != nil {
		return nil, root, err
	}
	refs, err := tt.db.DbData().Ddb.GetRefsOfTypeByNomsRoot(ctx, map[ref.RefType]struct{}{ref.TagRefType: {}}, root)
	return refs, root, err
}

func (tt *TagsTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return []sql.Index{refNameIndex{tt.db.Name(), tt.Name(), "tag_name", "dolt_tags_name_idx"}}, nil
}

func (tt *TagsTable) PreciseMatch() bool { return true }

func (tt *TagsTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	return &indexedTagsTable{TagsTable: tt}
}

type indexedTagsTable struct {
	*TagsTable
	refIndexedTable
	once sync.Once
	err  error
}

var _ sql.IndexedTable = (*indexedTagsTable)(nil)

func (t *indexedTagsTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	t.once.Do(func() {
		var refs []ref.DoltRef
		refs, t.root, t.err = t.tagRefs(ctx)
		if t.err == nil {
			t.setRefs(refs)
		}
	})
	if t.err != nil {
		return nil, t.err
	}
	return t.refIndexedTable.LookupPartitions(ctx, lookup)
}

func (t *indexedTagsTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	p := part.(*refPartition)
	return &indexedTagsIter{ddb: t.db.DbData().Ddb, refs: p.refs, root: t.root, reverse: p.reverse}, nil
}

type indexedTagsIter struct {
	ddb     *doltdb.DoltDB
	refs    []ref.DoltRef
	root    hash.Hash
	pos     int
	reverse bool
}

func (i *indexedTagsIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.pos >= len(i.refs) {
		return nil, io.EOF
	}
	pos := i.pos
	if i.reverse {
		pos = len(i.refs) - 1 - pos
	}
	r := i.refs[pos].(ref.TagRef)
	i.pos++
	tag, err := i.ddb.ResolveTagAtRoot(ctx, r, i.root)
	if err != nil {
		return nil, err
	}
	h, err := tag.Commit.HashOf()
	if err != nil {
		return nil, err
	}
	return sql.NewRow(tag.Name, h.String(), tag.Meta.Name, tag.Meta.Email, tag.Meta.Time(), tag.Meta.Description), nil
}
func (i *indexedTagsIter) Close(*sql.Context) error { return nil }
