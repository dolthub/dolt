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
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/utils/gpg"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
)

const logsDefaultRowCount = 100

// LogTable is a sql.Table implementation that implements a system table which shows the dolt commit log
type LogTable struct {
	ddb               *doltdb.DoltDB
	head              *doltdb.Commit
	headCommitClosure *prolly.CommitClosure
	dbName            string
	tableName         string
	headHash          hash.Hash
	refs              *refsCache
	projectedCols     []string
	schema            sql.Schema
}

// refsCache holds the commit-hash-to-refs map shared by every iterator a LogTable spawns.
// LogTable embeds it through a pointer so shallow copies in IndexedAccess share one map.
// Failures are not cached, so a transient error on one call does not poison later reads.
// sync.OnceValues would be a tighter fit for "build once, share" but it caches the error
// from the first attempt, which is the behavior this struct deliberately avoids.
type refsCache struct {
	mu sync.Mutex
	m  map[hash.Hash][]string
}

func (c *refsCache) load() map[hash.Hash][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m
}

func (c *refsCache) store(m map[hash.Hash][]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m = m
}

var _ sql.Table = (*LogTable)(nil)
var _ sql.StatisticsTable = (*LogTable)(nil)
var _ sql.IndexAddressable = (*LogTable)(nil)
var _ sql.ProjectedTable = (*LogTable)(nil)

// WithProjections records the columns the caller will read so the row builder can skip
// populating columns the SELECT did not ask for. The signature column in particular is left
// empty unless projected by name, keeping signature data opt-in.
func (dt *LogTable) WithProjections(_ *sql.Context, colNames []string) (sql.Table, error) {
	nt := *dt
	nt.projectedCols = colNames
	return &nt, nil
}

// Projections returns the column names previously stored by WithProjections, or nil when the
// table has not been projected (caller wants every column).
func (dt *LogTable) Projections() []string {
	return dt.projectedCols
}

// LogRowOptions selects which opt-in columns BuildLogTableRow populates. Each column stays
// NULL when its flag is false so callers can distinguish "did not ask" from a real empty
// value such as an initial commit with no parents or an unsigned commit.
type LogRowOptions struct {
	ShowParents   bool
	ShowSignature bool
}

// LogRowOptionsFromProjection returns the row options implied by a projected-columns list,
// where the |parents| and |signature| columns are populated only when explicitly named.
func LogRowOptionsFromProjection(projected []string) LogRowOptions {
	var opts LogRowOptions
	for _, p := range projected {
		switch p {
		case "parents":
			opts.ShowParents = true
		case "signature":
			opts.ShowSignature = true
		}
	}
	return opts
}

// LogTableSchema is the dolt_log column shape shared by the system table and the
// dolt_log() table function. Source and DatabaseSource are left unset so multi-database
// servers can stamp them per instance via NewLogTableSchema. The parents and signature
// columns are nullable because they are populated only when explicitly requested.
var LogTableSchema = sql.Schema{
	&sql.Column{Name: "commit_hash", Type: types.Text, PrimaryKey: true},
	&sql.Column{Name: "committer", Type: types.Text},
	&sql.Column{Name: "email", Type: types.Text},
	&sql.Column{Name: "date", Type: types.Datetime3},
	&sql.Column{Name: "message", Type: types.Text},
	&sql.Column{Name: "commit_order", Type: types.Uint64},
	&sql.Column{Name: "parents", Type: types.Text, Nullable: true},
	&sql.Column{Name: "refs", Type: types.Text},
	&sql.Column{Name: "signature", Type: types.Text, Nullable: true},
	&sql.Column{Name: "author", Type: types.Text},
	&sql.Column{Name: "author_email", Type: types.Text},
	&sql.Column{Name: "author_date", Type: types.Datetime3},
}

// NewLogTableSchema returns LogTableSchema cloned with each column's Source and
// DatabaseSource set to |tableName| and |dbName|, so the SQL planner can disambiguate
// columns that belong to different databases on the same server.
func NewLogTableSchema(dbName, tableName string) sql.Schema {
	sch := make(sql.Schema, len(LogTableSchema))
	for i, col := range LogTableSchema {
		c := *col
		c.Source = tableName
		c.DatabaseSource = dbName
		sch[i] = &c
	}
	return sch
}

// NewLogTable creates a LogTable. The per-instance schema is built once at construction
// so each Schema call is a single field read instead of recomputing on every query.
func NewLogTable(ctx *sql.Context, dbName, tableName string, ddb *doltdb.DoltDB, head *doltdb.Commit) sql.Table {
	return &LogTable{
		dbName:    dbName,
		tableName: tableName,
		ddb:       ddb,
		head:      head,
		refs:      &refsCache{},
		schema:    NewLogTableSchema(dbName, tableName),
	}
}

// DataLength implements sql.StatisticsTable
func (dt *LogTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(dt.Schema(ctx))
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

// BuildLogTableRow builds a dolt_log row for |commit| at |height|, formatting the refs column
// from |refs| (looked up by commit hash) and |headHash|. The |opts| flags select whether to
// populate the opt-in parents and signature columns; both stay NULL when their flag is false.
func BuildLogTableRow(ctx *sql.Context, commit *doltdb.Commit, meta *datas.CommitMeta, height uint64, refs map[hash.Hash][]string, headHash hash.Hash, opts LogRowOptions) (sql.Row, error) {
	commitHash, err := commit.HashOf()
	if err != nil {
		return nil, err
	}

	var parentsCol interface{}
	if opts.ShowParents {
		parentHashes, err := commit.ParentHashes(ctx)
		if err != nil {
			return nil, err
		}
		parentStrs := make([]string, len(parentHashes))
		for i, h := range parentHashes {
			parentStrs[i] = h.String()
		}
		parentsCol = strings.Join(parentStrs, ", ")
	}

	var refsStr string
	commitRefs := refs[commitHash]
	if len(commitRefs) > 0 {
		if commitHash == headHash {
			refsStr = "HEAD -> "
		}
		refsStr += strings.Join(commitRefs, ", ")
	}
	var signatureCol interface{}
	if opts.ShowSignature {
		if len(meta.Signature) > 0 {
			out, err := gpg.Verify(ctx, []byte(meta.Signature))
			if err != nil {
				return nil, err
			}
			signatureCol = string(out)
		} else {
			signatureCol = ""
		}
	}
	return sql.NewRow(
		commitHash.String(),
		meta.Committer.Name,
		meta.Committer.Email,
		meta.Committer.Date.Time(),
		meta.Description,
		height,
		parentsCol,
		refsStr,
		signatureCol,
		meta.Author.Name,
		meta.Author.Email,
		meta.Author.Date.Time(),
	), nil
}

// Schema is a sql.Table interface function that gets the sql.Schema of the log system table.
func (dt *LogTable) Schema(_ *sql.Context) sql.Schema {
	return dt.schema
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
	// System table populates parents and signature only when the caller explicitly projects them.
	rowOpts := LogRowOptionsFromProjection(dt.projectedCols)

	refs, err := dt.getCachedRefs(ctx)
	if err != nil {
		return nil, err
	}

	switch p := p.(type) {
	case *doltdb.CommitPart:
		height, err := p.Commit().Height()
		if err != nil {
			return nil, err
		}
		headHash, err := dt.HeadHash()
		if err != nil {
			return nil, err
		}
		row, err := BuildLogTableRow(ctx, p.Commit(), p.Meta(), height, refs, headHash, rowOpts)
		if err != nil {
			return nil, err
		}
		return sql.RowsToRowIter(row), nil
	default:
		return dt.NewLogItr(ctx, dt.ddb, dt.head, refs, rowOpts)
	}
}

// getCachedRefs returns the commit-hash-to-refs map for the system table, building it on first
// successful call. Failures are not cached so a transient error on one call does not poison
// later partition reads.
func (dt *LogTable) getCachedRefs(ctx *sql.Context) (map[hash.Hash][]string, error) {
	if m := dt.refs.load(); m != nil {
		return m, nil
	}
	m, err := GetCommitHashToRefs(ctx, dt.ddb, cli.DecorateShort)
	if err != nil {
		return nil, err
	}
	dt.refs.store(m)
	return m, nil
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
		return nil, fmt.Errorf("failed to parse commit lookup ranges: %s", sql.DebugString(ctx, lookup.Ranges))
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
	child       doltdb.CommitItr[*sql.Context]
	cHashToRefs map[hash.Hash][]string
	headHash    hash.Hash
	rowOpts     LogRowOptions
}

// NewLogItr creates a LogItr from the current environment. |rowOpts| selects which opt-in
// columns the iterator should populate.
func (dt *LogTable) NewLogItr(ctx *sql.Context, ddb *doltdb.DoltDB, head *doltdb.Commit, cHashToRefs map[hash.Hash][]string, rowOpts LogRowOptions) (*LogItr, error) {
	h, err := head.HashOf()
	if err != nil {
		return nil, err
	}

	child, err := commitwalk.GetTopologicalOrderIterator[*sql.Context](ctx, ddb, []hash.Hash{h}, nil)
	if err != nil {
		return nil, err
	}

	return &LogItr{child: child, cHashToRefs: cHashToRefs, headHash: h, rowOpts: rowOpts}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *LogItr) Next(ctx *sql.Context) (sql.Row, error) {
	_, optCmt, meta, height, err := itr.child.Next(ctx)
	if err != nil {
		return nil, err
	}

	cm, ok := optCmt.ToCommit()
	if !ok {
		// Should have been caught by the commit walk.
		return nil, doltdb.ErrGhostCommitRuntimeFailure
	}

	if meta == nil {
		meta, err = cm.GetCommitMeta(ctx)
		if err != nil {
			return nil, err
		}
	}

	if height == 0 {
		height, err = cm.Height()
		if err != nil {
			return nil, err
		}
	}

	return BuildLogTableRow(ctx, cm, meta, height, itr.cHashToRefs, itr.headHash, itr.rowOpts)
}

// Close closes the iterator.
func (itr *LogItr) Close(*sql.Context) error {
	return nil
}

// GetCommitHashToRefs returns a map from commit hash to branch, remote-branch, and tag names.
// When |decoration| is "full" the ref names keep their refs/heads, refs/remotes, and refs/tags
// prefixes. When |decoration| is "no" the returned map is empty so the refs column renders
// blank. Any other value returns the short ref names. Callers should resolve "auto" before
// calling because this function has no tty signal of its own.
func GetCommitHashToRefs(ctx *sql.Context, ddb *doltdb.DoltDB, decoration string) (map[hash.Hash][]string, error) {
	cHashToRefs := map[hash.Hash][]string{}
	if decoration == cli.DecorateNo {
		return cHashToRefs, nil
	}

	branches, err := ddb.GetBranchesWithHashes(ctx)
	if err != nil {
		return nil, err
	}
	for _, b := range branches {
		refName := b.Ref.String()
		if decoration != cli.DecorateFull {
			refName = b.Ref.GetPath()
		}
		cHashToRefs[b.Hash] = append(cHashToRefs[b.Hash], refName)
	}

	remotes, err := ddb.GetRemotesWithHashes(ctx)
	if err != nil {
		return nil, err
	}
	for _, r := range remotes {
		refName := r.Ref.String()
		if decoration != cli.DecorateFull {
			refName = r.Ref.GetPath()
		}
		cHashToRefs[r.Hash] = append(cHashToRefs[r.Hash], refName)
	}

	tags, err := ddb.GetTagRefsWithHashes(ctx)
	if err != nil {
		return nil, err
	}
	for _, t := range tags {
		tagName := t.Ref.String()
		if decoration != cli.DecorateFull {
			tagName = t.Ref.GetPath()
		}
		tagName = fmt.Sprintf("tag: %s", tagName)
		cHashToRefs[t.Hash] = append(cHashToRefs[t.Hash], tagName)
	}

	return cHashToRefs, nil
}
