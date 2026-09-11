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
	"strings"

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

var _ sql.OrderedIndex = refNameIndex{}

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
func (i refNameIndex) CanSupportOrderBy(sql.Expression) bool { return true }
func (i refNameIndex) CoversColumns(cols []string) bool {
	for _, col := range cols {
		if !strings.EqualFold(col, i.column) {
			return false
		}
	}
	return true
}
func (i refNameIndex) PrefixLengths() []uint16           { return nil }
func (i refNameIndex) Order(*sql.Context) sql.IndexOrder { return sql.IndexOrderAsc }
func (i refNameIndex) Reversible(*sql.Context) bool      { return true }

// refIndexedTable holds the ordered ref snapshot shared by lookups in one
// execution. Each system table loads its own snapshot and resolves its rows.
type refIndexedTable struct {
	refs   []ref.DoltRef
	byName map[string]int
	root   hash.Hash
}

func refSQLName(r ref.DoltRef) string {
	if r.GetType() == ref.RemoteRefType {
		return "remotes/" + r.GetPath()
	}
	return r.GetPath()
}

func (t *refIndexedTable) setRefs(refs []ref.DoltRef) {
	t.refs = refs
	sort.Slice(t.refs, func(i, j int) bool { return refSQLName(t.refs[i]) < refSQLName(t.refs[j]) })
	t.byName = make(map[string]int, len(refs))
	for i, r := range refs {
		t.byName[refSQLName(r)] = i
	}
}

// LookupPartitions returns one scan for each already-disjoint engine range.
func (t *refIndexedTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if lookup.IsEmptyRange {
		return NewSliceOfPartitionsItr(nil), nil
	}
	ranges, ok := lookup.Ranges.(sql.MySQLRangeCollection)
	if !ok {
		return nil, fmt.Errorf("unsupported ref index ranges: %T", lookup.Ranges)
	}
	var parts []sql.Partition
	for _, r := range ranges {
		switch r[0].Type() {
		case sql.RangeType_Empty, sql.RangeType_EqualNull:
			continue
		}
		var lower, upper string
		lowerInclusive := r[0].LowerBound.TypeAsLowerBound().Inclusive()
		upperInclusive := r[0].UpperBound.TypeAsUpperBound().Inclusive()
		if r[0].HasLowerBound() {
			lower = sql.GetMySQLRangeCutKey(r[0].LowerBound).(string)
		}
		if r[0].HasUpperBound() {
			upper = sql.GetMySQLRangeCutKey(r[0].UpperBound).(string)
		}
		start, end := 0, len(t.refs)
		if r[0].HasLowerBound() && r[0].HasUpperBound() && lower == upper {
			pos, ok := t.byName[lower]
			if !ok || !lowerInclusive || !upperInclusive {
				continue
			}
			start, end = pos, pos+1
		} else {
			if r[0].HasLowerBound() {
				start = sort.Search(len(t.refs), func(i int) bool {
					name := refSQLName(t.refs[i])
					return name > lower || lowerInclusive && name == lower
				})
			}
			if r[0].HasUpperBound() {
				end = sort.Search(len(t.refs), func(i int) bool {
					name := refSQLName(t.refs[i])
					return name > upper || !upperInclusive && name == upper
				})
			}
		}
		if start < end {
			parts = append(parts, &refPartition{refs: t.refs[start:end], reverse: lookup.IsReverse})
		}
	}
	return NewSliceOfPartitionsItr(parts), nil
}

type refPartition struct {
	refs    []ref.DoltRef
	reverse bool
}

func (p *refPartition) Key() []byte { return []byte(refSQLName(p.refs[0])) }
