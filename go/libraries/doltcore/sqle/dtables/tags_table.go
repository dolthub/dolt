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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

var _ sql.Table = (*TagsTable)(nil)

// TagsTable is a sql.Table implementation that implements a system table which shows the dolt tags
type TagsTable struct {
	ddb *doltdb.DoltDB
}

// NewTagsTable creates a TagsTable
func NewTagsTable(_ *sql.Context, ddb *doltdb.DoltDB) sql.Table {
	return &TagsTable{ddb: ddb}
}

// Name is a sql.Table interface function which returns the name of the table which is defined by the constant
// TagsTableName
func (dt *TagsTable) Name() string {
	return doltdb.TagsTableName
}

// String is a sql.Table interface function which returns the name of the table which is defined by the constant
// TagsTableName
func (dt *TagsTable) String() string {
	return doltdb.TagsTableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the tags system table.
func (dt *TagsTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "tag_name", Type: sql.Text, Source: doltdb.TagsTableName, PrimaryKey: true},
		{Name: "tag_hash", Type: sql.Text, Source: doltdb.TagsTableName, PrimaryKey: true},
		{Name: "tagger", Type: sql.Text, Source: doltdb.TagsTableName, PrimaryKey: false},
		{Name: "email", Type: sql.Text, Source: doltdb.TagsTableName, PrimaryKey: false},
		{Name: "date", Type: sql.Datetime, Source: doltdb.TagsTableName, PrimaryKey: false},
		{Name: "message", Type: sql.Text, Source: doltdb.TagsTableName, PrimaryKey: false},
	}
}

// Partitions is a sql.Table interface function that returns a partition of the data. Currently, the data is unpartitioned.
func (dt *TagsTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (dt *TagsTable) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return NewTagsItr(ctx, dt.ddb)
}

// TagsItr is a sql.RowItr implementation which iterates over each commit as if it's a row in the table.
type TagsItr struct {
	tagsWithHash []doltdb.TagWithHash
	idx          int
}

// NewTagsItr creates a TagsItr from the current environment.
func NewTagsItr(ctx *sql.Context, ddb *doltdb.DoltDB) (*TagsItr, error) {
	tagsWithHash, err := ddb.GetTagsWithHashes(ctx)
	if err != nil {
		return nil, err
	}

	return &TagsItr{tagsWithHash, 0}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *TagsItr) Next(ctx *sql.Context) (sql.Row, error) {
	if itr.idx >= len(itr.tagsWithHash) {
		return nil, io.EOF
	}

	defer func() {
		itr.idx++
	}()

	twh := itr.tagsWithHash[itr.idx]
	return sql.NewRow(twh.Tag.Name,
		twh.Hash.String(),
		twh.Tag.Meta.Name,
		twh.Tag.Meta.Email,
		twh.Tag.Meta.Time(),
		twh.Tag.Meta.Description), nil
}

// Close closes the iterator.
func (itr *TagsItr) Close(*sql.Context) error {
	return nil
}
