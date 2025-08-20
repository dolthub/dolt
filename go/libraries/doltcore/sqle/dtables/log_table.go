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

package dtables

import (
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
)

const logsDefaultRowCount = 100

// LogSchemaType represents different log table schema types
type LogSchemaType int

const (
	LogSchemaTypeAuto LogSchemaType = iota
	LogSchemaTypeCompact
	LogSchemaTypeFull
)

// LogTable is a sql.Table implementation that implements a system table which shows the dolt commit log
type LogTable struct {
	ddb               *doltdb.DoltDB
	head              *doltdb.Commit
	headCommitClosure *prolly.CommitClosure
	dbName            string
	tableName         string
	headHash          hash.Hash
	ctx				  *sql.Context
	schType			  LogSchemaType
}

var _ sql.Table = (*LogTable)(nil)
var _ sql.StatisticsTable = (*LogTable)(nil)
var _ sql.IndexAddressable = (*LogTable)(nil)

// NewLogTable creates a LogTable
func NewLogTable(_ *sql.Context, dbName, tableName string, ddb *doltdb.DoltDB, head *doltdb.Commit, ctx *sql.Context, schType LogSchemaType) sql.Table {
	return &LogTable{dbName: dbName, tableName: tableName, ddb: ddb, head: head, ctx: ctx, schType: schType}
}

// DataLength implements sql.StatisticsTable
func (dt *LogTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(dt.Schema())
	numRows, _, err := dt.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

// RowCount implements sql.StatisticsTable
func (dt *LogTable) RowCount(ctx *sql.Context) (uint64, bool, error) {
	cc, err := dt.head.GetCommitClosure(ctx)
	if err != nil {
		// TODO: remove this when we deprecate LD
		return logsDefaultRowCount, false, nil
	}
	if cc.IsEmpty() {
		return 1, true, nil
	}
	cnt, err := cc.Count()
	return uint64(cnt + 1), true, err
}

// Name is a sql.Table interface function which returns the name of the table
func (dt *LogTable) Name() string {
	return dt.tableName
}

// String is a sql.Table interface function which returns the name of the table
func (dt *LogTable) String() string {
	return dt.tableName
}

// buildCompactRow builds a 6-column compact row
func buildCompactRow(commitHash hash.Hash, meta *datas.CommitMeta, height uint64) sql.Row {
	values := []interface{}{
		commitHash.String(),
		datas.ValueOrDefault(meta.CommitterName, meta.Name),
		datas.ValueOrDefault(meta.CommitterEmail, meta.Email),
		meta.CommitterTime(), // Use committer time for semantic consistency
		meta.Description,
		height,
	}
	return sql.NewRow(values...)
}

// buildFullRow builds a 9-column extended row
func buildFullRow(commitHash hash.Hash, meta *datas.CommitMeta, height uint64) sql.Row {
	values := []interface{}{
		commitHash.String(),
		datas.ValueOrDefault(meta.CommitterName, meta.Name),
		datas.ValueOrDefault(meta.CommitterEmail, meta.Email),
		meta.CommitterTime(),
		meta.Description,
		height,
		meta.Name,
		meta.Email,
		meta.Time(),
	}
	return sql.NewRow(values...)
}

// BuildLogRowWithSchemaType builds a row based on the specified schema type
func BuildLogRowWithSchemaType(ctx *sql.Context, commitHash hash.Hash, meta *datas.CommitMeta, height uint64, schType LogSchemaType) sql.Row {
	switch schType {
	case LogSchemaTypeCompact:
		return buildCompactRow(commitHash, meta, height)
	case LogSchemaTypeFull:
		return buildFullRow(commitHash, meta, height)
	case LogSchemaTypeAuto:
		useCompactSchema, _ := dsess.GetBooleanSystemVar(ctx, dsess.DoltLogCompactSchema)
		if useCompactSchema {
			return buildCompactRow(commitHash, meta, height)
		} else {
			return buildFullRow(commitHash, meta, height)
		}
	}
	return nil
}

// BuildLogRow builds a row using only the session var to determine compactness (no override).
func BuildLogRow(ctx *sql.Context, commitHash hash.Hash, meta *datas.CommitMeta, height uint64) sql.Row {
	return BuildLogRowWithSchemaType(ctx, commitHash, meta, height, LogSchemaTypeAuto)
}

var LogSchemaCompact = sql.Schema{
	&sql.Column{Name: "commit_hash", Type: types.Text},
	&sql.Column{Name: "committer", Type: types.Text},
	&sql.Column{Name: "email", Type: types.Text},
	&sql.Column{Name: "date", Type: types.Datetime},
	&sql.Column{Name: "message", Type: types.Text},
	&sql.Column{Name: "commit_order", Type: types.Uint64},
}

var LogSchemaCommitterColumns = sql.Schema{
	&sql.Column{Name: "commit_hash", Type: types.Text},
	&sql.Column{Name: "committer", Type: types.Text},
	&sql.Column{Name: "committer_email", Type: types.Text},
	&sql.Column{Name: "committer_date", Type: types.Datetime},
	&sql.Column{Name: "message", Type: types.Text},
	&sql.Column{Name: "commit_order", Type: types.Uint64},
}

var LogSchemaAuthorColumns = sql.Schema{
	&sql.Column{Name: "author", Type: types.Text},
	&sql.Column{Name: "author_email", Type: types.Text},
	&sql.Column{Name: "author_date", Type: types.Datetime},
}

func GetLogTableSchema(ctx *sql.Context, tableName, dbName string) sql.Schema {
	var baseSchema sql.Schema

	useCompactSchema, _ := dsess.GetBooleanSystemVar(ctx, dsess.DoltLogCompactSchema)
	if useCompactSchema {
		baseSchema = make(sql.Schema, len(LogSchemaCompact))
		copy(baseSchema, LogSchemaCompact)
	} else {
		baseSchema = make(sql.Schema, len(LogSchemaCommitterColumns))
		copy(baseSchema, LogSchemaCommitterColumns)
		baseSchema = append(baseSchema, LogSchemaAuthorColumns...)
	}

	for _, col := range baseSchema {
		col.Source = tableName
		col.DatabaseSource = dbName
		if col.Name == "commit_hash" {
			col.PrimaryKey = true
		}
	}

	return baseSchema
}

// Schema is a sql.Table interface function that gets the sql.Schema of the log system table.
func (dt *LogTable) Schema() sql.Schema {
	var baseSchema sql.Schema

	switch dt.schType {
	case LogSchemaTypeCompact:
		baseSchema = make(sql.Schema, len(LogSchemaCompact))
		copy(baseSchema, LogSchemaCompact)
	case LogSchemaTypeFull:
		baseSchema = make(sql.Schema, len(LogSchemaCommitterColumns))
		copy(baseSchema, LogSchemaCommitterColumns)
		baseSchema = append(baseSchema, LogSchemaAuthorColumns...)
	case LogSchemaTypeAuto:
		return GetLogTableSchema(dt.ctx, dt.tableName, dt.dbName)
	}

	for _, col := range baseSchema {
		col.Source = dt.tableName
		col.DatabaseSource = dt.dbName
		if col.Name == "commit_hash" {
			col.PrimaryKey = true
		}
	}
	return baseSchema
}

// Collation implements the sql.Table interface.
func (dt *LogTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data.  Currently the data is unpartitioned.
func (dt *LogTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (dt *LogTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	switch p := p.(type) {
	case *doltdb.CommitPart:
		height, err := p.Commit().Height()
		if err != nil {
			return nil, err
		}
		return sql.RowsToRowIter(BuildLogRowWithSchemaType(ctx, p.Hash(), p.Meta(), height, dt.schType)), nil
	default:
		return NewLogItr(ctx, dt.ddb, dt.head, dt.schType)
	}
}

func (dt *LogTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return index.DoltCommitIndexes(dt.dbName, dt.Name(), dt.ddb, true)
}

// IndexedAccess implements sql.IndexAddressable
func (dt *LogTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	nt := *dt
	return &nt
}

// PreciseMatch implements sql.IndexAddressable
func (dt *LogTable) PreciseMatch() bool {
	return true
}

func (dt *LogTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if lookup.Index.ID() == index.CommitHashIndexId {
		return dt.commitHashPartitionIter(ctx, lookup)
	}

	return dt.Partitions(ctx)
}

func (dt *LogTable) commitHashPartitionIter(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	hashStrs, ok := index.LookupToPointSelectStr(lookup)
	if !ok {
		return nil, fmt.Errorf("failed to parse commit lookup ranges: %s", sql.DebugString(lookup.Ranges))
	}
	hashes, commits, metas := index.HashesToCommits(ctx, dt.ddb, hashStrs, nil, false)
	if len(hashes) == 0 {
		return sql.PartitionsToPartitionIter(), nil
	}
	var partitions []sql.Partition
	for i, h := range hashes {
		height, err := commits[i].Height()
		if err != nil {
			return nil, err
		}

		ok, err = dt.CommitIsInScope(ctx, height, h)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}

		partitions = append(partitions, doltdb.NewCommitPart(h, commits[i], metas[i]))

	}
	return sql.PartitionsToPartitionIter(partitions...), nil
}

// CommitIsInScope returns true if a given commit hash is head or is
// visible from the current head's ancestry graph.
func (dt *LogTable) CommitIsInScope(ctx context.Context, height uint64, h hash.Hash) (bool, error) {
	headHash, err := dt.HeadHash()
	if err != nil {
		return false, err
	}
	if headHash == h {
		return true, nil
	}
	cc, err := dt.HeadCommitClosure(ctx)
	if err != nil {
		return false, err
	}
	return cc.ContainsKey(ctx, h, height)
}

func (dt *LogTable) HeadCommitClosure(ctx context.Context) (*prolly.CommitClosure, error) {
	if dt.headCommitClosure == nil {
		cc, err := dt.head.GetCommitClosure(ctx)
		dt.headCommitClosure = &cc
		if err != nil {
			return nil, err
		}
	}
	return dt.headCommitClosure, nil
}

func (dt *LogTable) HeadHash() (hash.Hash, error) {
	if dt.headHash.IsEmpty() {
		var err error
		dt.headHash, err = dt.head.HashOf()
		if err != nil {
			return hash.Hash{}, err
		}
	}
	return dt.headHash, nil
}

// LogItr is a sql.RowItr implementation which iterates over each commit as if it's a row in the table.
type LogItr struct {
	child      doltdb.CommitItr[*sql.Context]
	schType	   LogSchemaType
}

// NewLogItr creates a LogItr from the current environment.
func NewLogItr(ctx *sql.Context, ddb *doltdb.DoltDB, head *doltdb.Commit, schType LogSchemaType) (*LogItr, error) {
	h, err := head.HashOf()
	if err != nil {
		return nil, err
	}

	child, err := commitwalk.GetTopologicalOrderIterator[*sql.Context](ctx, ddb, []hash.Hash{h}, nil)
	if err != nil {
		return nil, err
	}

	return &LogItr{child: child, schType: schType}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *LogItr) Next(ctx *sql.Context) (sql.Row, error) {
	h, optCmt, err := itr.child.Next(ctx)
	if err != nil {
		return nil, err
	}

	cm, ok := optCmt.ToCommit()
	if !ok {
		// Should have been caught by the commit walk.
		return nil, doltdb.ErrGhostCommitRuntimeFailure
	}

	meta, err := cm.GetCommitMeta(ctx)
	if err != nil {
		return nil, err
	}

	height, err := cm.Height()
	if err != nil {
		return nil, err
	}

	return BuildLogRowWithSchemaType(ctx, h, meta, height, itr.schType), nil
}

// Close closes the iterator.
func (itr *LogItr) Close(*sql.Context) error {
	return nil
}
