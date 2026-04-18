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
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	storetypes "github.com/dolthub/dolt/go/store/types"
)

const tagsDefaultRowCount = 10

// doltTagsIndexName is the name of the index on the dolt_tags system table that covers
// the "tag_name" field.
const doltTagsIndexName = "dolt_tags_tag_name_idx"

var _ sql.Table = (*TagsTable)(nil)
var _ sql.StatisticsTable = (*TagsTable)(nil)
var _ sql.IndexedTable = (*TagsTable)(nil)
var _ sql.IndexAddressable = (*TagsTable)(nil)

// TagsTable is a sql.Table implementation that implements a system table which shows the dolt tags
type TagsTable struct {
	ddb       *doltdb.DoltDB
	tableName string
}

// NewTagsTable creates a TagsTable
func NewTagsTable(_ *sql.Context, tableName string, ddb *doltdb.DoltDB) sql.Table {
	return &TagsTable{tableName: tableName, ddb: ddb}
}

func (tt *TagsTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(tt.Schema())
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
func (tt *TagsTable) Schema() sql.Schema {
	dbName := tt.ddb.GetDatabaseName()
	return []*sql.Column{
		{Name: "tag_name", Type: types.Text, Source: tt.tableName, PrimaryKey: true, DatabaseSource: dbName},
		{Name: "tag_hash", Type: types.Text, Source: tt.tableName, PrimaryKey: true, DatabaseSource: dbName},
		{Name: "tagger", Type: types.Text, Source: tt.tableName, PrimaryKey: false, DatabaseSource: dbName},
		{Name: "email", Type: types.Text, Source: tt.tableName, PrimaryKey: false, DatabaseSource: dbName},
		{Name: "date", Type: types.Datetime3, Source: tt.tableName, PrimaryKey: false, DatabaseSource: dbName},
		{Name: "message", Type: types.Text, Source: tt.tableName, PrimaryKey: false, DatabaseSource: dbName},
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

// LookupPartitions implements sql.IndexedTable
func (tt *TagsTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if lookup.Index.ID() == doltTagsIndexName {
		partitions, err := parseMySQLRangeLookup(lookup)
		if err != nil {
			return nil, err
		}
		return NewSliceOfPartitionsItr(partitions), nil
	}
	return nil, fmt.Errorf("unsupported index: %s", lookup.Index.ID())
}

// IndexedAccess implements sql.IndexAddressable
func (tt *TagsTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	return tt
}

// GetIndexes implements sql.IndexAddressable
func (tt *TagsTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return []sql.Index{
		index.NewTagNameIndex(
			index.MockIndex(doltTagsIndexName,
				tt.ddb.GetDatabaseName(), tt.Name(), "tag_name", storetypes.StringKind, true)),
	}, nil
}

// PreciseMatch implements sql.IndexAddressable
func (tt *TagsTable) PreciseMatch() bool {
	return false
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (tt *TagsTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	if fp, ok := part.(*filteredPartition); ok {
		return NewFilteredTagsItr(ctx, tt.ddb,
			fp.lowerBound, fp.lowerBoundInclusive,
			fp.upperBound, fp.upperBoundInclusive)
	}
	return NewTagsItr(ctx, tt.ddb)
}

// TagsItr is a sql.RowItr implementation which iterates over each tag as if it's a row in the table.
type TagsItr struct {
	tagsWithHash []doltdb.TagWithHash
	idx          int

	// lowerBound is an optional filter for the lowest (alphabetically) tag name returned
	lowerBound          string
	lowerBoundInclusive bool
	// upperBound is an optional filter for the highest (alphabetically) tag name returned
	upperBound          string
	upperBoundInclusive bool
}

// NewTagsItr creates a TagsItr from the current environment.
func NewTagsItr(ctx *sql.Context, ddb *doltdb.DoltDB) (*TagsItr, error) {
	tagsWithHash, err := ddb.GetTagsWithHashes(ctx)
	if err != nil {
		return nil, err
	}

	return &TagsItr{tagsWithHash: tagsWithHash}, nil
}

// NewFilteredTagsItr creates a TagsItr that filters out tag names lower than
// |lowerBound| and higher than |upperBound|.
func NewFilteredTagsItr(ctx *sql.Context, ddb *doltdb.DoltDB, lowerBound string, lowerBoundInclusive bool, upperBound string, upperBoundInclusive bool) (*TagsItr, error) {
	itr, err := NewTagsItr(ctx, ddb)
	if err != nil {
		return nil, err
	}
	itr.lowerBound = lowerBound
	itr.lowerBoundInclusive = lowerBoundInclusive
	itr.upperBound = upperBound
	itr.upperBoundInclusive = upperBoundInclusive
	return itr, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed. If an upper
// or lower bound has been configured, this function will filter out tag
// names outside those bounds.
func (itr *TagsItr) Next(ctx *sql.Context) (sql.Row, error) {
	defer func() {
		itr.idx++
	}()

	for {
		if itr.idx >= len(itr.tagsWithHash) {
			return nil, io.EOF
		}

		twh := itr.tagsWithHash[itr.idx]
		tagName := twh.Tag.Name
		if outOfLowerBound(tagName, itr.lowerBound, itr.lowerBoundInclusive) ||
			outOfUpperBound(tagName, itr.upperBound, itr.upperBoundInclusive) {
			itr.idx++
			continue
		}

		return sql.NewRow(tagName, twh.Hash.String(), twh.Tag.Meta.Name, twh.Tag.Meta.Email, twh.Tag.Meta.Time(), twh.Tag.Meta.Description), nil
	}
}

// Close closes the iterator.
func (itr *TagsItr) Close(*sql.Context) error {
	return nil
}
