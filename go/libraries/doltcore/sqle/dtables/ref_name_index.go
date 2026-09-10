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
	"sort"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/hash"
)

// refNameIndex is a virtual index: it has no schema or physical index storage.
// Ref names use the binary collation of types.Text, so their SQL ordering is
// the same as the ordering of the ref map.
type refNameIndex struct {
	database, table, column, id string
}

var _ sql.Index = refNameIndex{}

// Describe the full string key as VARCHAR. The engine treats unique TEXT
// indexes without prefix lengths as content hashes, which only support equality.
// This virtual index stores no hashes and compares the entire name.
var refNameIndexType = types.MustCreateStringWithDefaults(sqltypes.VarChar, types.Text.MaxCharacterLength())

func (i refNameIndex) ID() string            { return i.id }
func (i refNameIndex) Database() string      { return i.database }
func (i refNameIndex) Table() string         { return i.table }
func (i refNameIndex) Expressions() []string { return []string{i.table + "." + i.column} }
func (i refNameIndex) IsUnique() bool        { return true }
func (i refNameIndex) IsSpatial() bool       { return false }
func (i refNameIndex) IsFullText() bool      { return false }
func (i refNameIndex) IsVector() bool        { return false }
func (i refNameIndex) Comment() string       { return "" }
func (i refNameIndex) IndexType() string     { return "BTREE" }
func (i refNameIndex) IsGenerated() bool     { return false }
func (i refNameIndex) ColumnExpressionTypes(*sql.Context) []sql.ColumnExpressionType {
	return []sql.ColumnExpressionType{{Expression: i.Expressions()[0], Type: refNameIndexType}}
}
func (i refNameIndex) CanSupport(_ *sql.Context, ranges ...sql.Range) bool {
	for _, r := range ranges {
		mr, ok := r.(sql.MySQLRange)
		if !ok || len(mr) != 1 {
			return false
		}
		for _, cut := range []sql.MySQLRangeCut{mr[0].LowerBound, mr[0].UpperBound} {
			switch cut := cut.(type) {
			case sql.Below:
				if _, ok := cut.Key.(string); !ok {
					return false
				}
			case sql.Above:
				if _, ok := cut.Key.(string); !ok {
					return false
				}
			case sql.BelowNull, sql.AboveNull, sql.AboveAll:
			default:
				return false
			}
		}
	}
	return true
}
func (i refNameIndex) CanSupportOrderBy(sql.Expression) bool { return false }
func (i refNameIndex) CoversColumns([]string) bool           { return false }
func (i refNameIndex) PrefixLengths() []uint16               { return nil }

// refIndexedTable owns one execution's snapshot. In a lookup join the engine
// calls LookupPartitions repeatedly with new ranges; only the first call reads
// refs from storage. No tag metadata or branch commits are loaded here.
type refIndexedTable struct {
	sql.Table
	once   sync.Once
	load   func(*sql.Context) ([]ref.DoltRef, hash.Hash, error)
	rows   func(*sql.Context, *refPartition) (sql.RowIter, error)
	refs   []ref.DoltRef
	names  []string
	byName map[string]int
	root   hash.Hash
	err    error
}

var _ sql.IndexedTable = (*refIndexedTable)(nil)

func refSQLName(r ref.DoltRef) string {
	if r.GetType() == ref.RemoteRefType {
		return "remotes/" + r.GetPath()
	}
	return r.GetPath()
}

func (t *refIndexedTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	t.once.Do(func() {
		t.refs, t.root, t.err = t.load(ctx)
		if t.err != nil {
			return
		}
		sort.Slice(t.refs, func(i, j int) bool { return refSQLName(t.refs[i]) < refSQLName(t.refs[j]) })
		t.names = make([]string, len(t.refs))
		t.byName = make(map[string]int, len(t.refs))
		for i, r := range t.refs {
			t.names[i] = refSQLName(r)
			t.byName[t.names[i]] = i
		}
	})
	if t.err != nil {
		return nil, t.err
	}
	if lookup.IsEmptyRange {
		return NewSliceOfPartitionsItr(nil), nil
	}
	ranges, ok := lookup.Ranges.(sql.MySQLRangeCollection)
	if !ok {
		return nil, fmt.Errorf("unsupported ref index ranges: %T", lookup.Ranges)
	}
	// Merge intervals so overlapping ranges cannot emit the same ref twice.
	type interval struct{ start, end int }
	var intervals []interval
	for _, r := range ranges {
		if len(r) != 1 {
			return nil, fmt.Errorf("ref name index requires one range column")
		}
		start, end, err := t.bounds(r[0])
		if err != nil {
			return nil, err
		}
		if start < end {
			intervals = append(intervals, interval{start, end})
		}
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i].start < intervals[j].start })
	var merged []interval
	for _, r := range intervals {
		if len(merged) > 0 && r.start <= merged[len(merged)-1].end {
			if r.end > merged[len(merged)-1].end {
				merged[len(merged)-1].end = r.end
			}
		} else {
			merged = append(merged, r)
		}
	}
	parts := make([]sql.Partition, len(merged))
	for i, r := range merged {
		parts[i] = &refPartition{refs: t.refs[r.start:r.end], root: t.root}
	}
	return NewSliceOfPartitionsItr(parts), nil
}

func (t *refIndexedTable) bounds(r sql.MySQLRangeColumnExpr) (int, int, error) {
	if lower, ok := r.LowerBound.(sql.Below); ok {
		if upper, ok := r.UpperBound.(sql.Above); ok {
			key, ok := lower.Key.(string)
			if !ok {
				return 0, 0, fmt.Errorf("unsupported ref name: %T", lower.Key)
			}
			if upperKey, ok := upper.Key.(string); ok && key == upperKey {
				if pos, ok := t.byName[key]; ok {
					return pos, pos + 1, nil
				}
				return 0, 0, nil
			}
		}
	}
	start, err := t.cutPosition(r.LowerBound)
	if err != nil {
		return 0, 0, err
	}
	end, err := t.cutPosition(r.UpperBound)
	return start, end, err
}

// A Below cut precedes equal keys; an Above cut follows them. Since names
// cannot be NULL, both NULL cuts precede every name, including the empty string.
func (t *refIndexedTable) cutPosition(c sql.MySQLRangeCut) (int, error) {
	var key any
	inclusive := false
	switch c := c.(type) {
	case sql.Below:
		key = c.Key
	case sql.Above:
		key = c.Key
		inclusive = true
	case sql.BelowNull, sql.AboveNull:
		return 0, nil
	case sql.AboveAll:
		return len(t.names), nil
	default:
		return 0, fmt.Errorf("unsupported ref range cut: %T", c)
	}
	s, ok := key.(string)
	if !ok {
		return 0, fmt.Errorf("unsupported ref name: %T", key)
	}
	return sort.Search(len(t.names), func(i int) bool {
		if inclusive {
			return t.names[i] > s
		}
		return t.names[i] >= s
	}), nil
}

func (t *refIndexedTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	return t.rows(ctx, p.(*refPartition))
}

type refPartition struct {
	refs []ref.DoltRef
	root hash.Hash
}

func (p *refPartition) Key() []byte {
	if len(p.refs) == 0 {
		return nil
	}
	return []byte(p.refs[0].String())
}
