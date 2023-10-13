// Copyright 2023 Dolthub, Inc.
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

package stats

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	types2 "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/dolt/go/store/hash"
)

type DoltStats struct {
	level         int
	chunks        []hash.Hash
	RowCount      uint64
	DistinctCount uint64
	NullCount     uint64
	AvgSize       uint64
	CreatedAt     time.Time
	Histogram     DoltHistogram
	Columns       []string
	Types         []sql.Type
}

func DoltStatsFromSql(stats *sql.Stats) (*DoltStats, error) {
	typs, err := parseTypeString(stats.Types)
	if err != nil {
		return nil, err
	}
	hist, err := DoltHistFromSql(stats.Histogram, typs)
	if err != nil {
		return nil, err
	}
	return &DoltStats{
		RowCount:      stats.Rows,
		DistinctCount: stats.Distinct,
		NullCount:     stats.Nulls,
		AvgSize:       stats.AvgSize,
		CreatedAt:     stats.CreatedAt,
		Histogram:     hist,
		Columns:       stats.Columns,
		Types:         typs,
	}, nil
}

func (s *DoltStats) toSql() *sql.Stats {
	typStrs := make([]string, len(s.Types))
	for i, typ := range s.Types {
		typStrs[i] = typ.String()
	}
	return &sql.Stats{
		Rows:      s.RowCount,
		Distinct:  s.DistinctCount,
		Nulls:     s.NullCount,
		AvgSize:   s.AvgSize,
		CreatedAt: s.CreatedAt,
		Histogram: s.Histogram.toSql(),
		Columns:   s.Columns,
		Types:     typStrs,
		Version:   0,
	}
}

type DoltHistogram []DoltBucket

type DoltBucket struct {
	Count      uint64
	Distinct   uint64
	Null       uint64
	Mcv        []sql.Row
	McvCount   []uint64
	BoundCount uint64
	UpperBound sql.Row
}

func DoltHistFromSql(hist sql.Histogram, types []sql.Type) (DoltHistogram, error) {
	ret := make([]DoltBucket, len(hist))
	var err error
	for i, b := range hist {
		upperBound := make(sql.Row, len(b.UpperBound))
		for i, v := range b.UpperBound {
			upperBound[i], _, err = types[i].Convert(v)
			if err != nil {
				return nil, fmt.Errorf("failed to convert %v to type %s", v, types[i].String())
			}
		}
		mcvs := make([]sql.Row, len(b.Mcv))
		for i, mcv := range b.Mcv {
			for _, v := range mcv {
				conv, _, err := types[i].Convert(v)
				if err != nil {
					return nil, fmt.Errorf("failed to convert %v to type %s", v, types[i].String())
				}
				mcvs[i] = append(mcvs[i], conv)
			}
		}
		ret[i] = DoltBucket{
			Count:      b.Count,
			Distinct:   b.Distinct,
			Null:       b.Null,
			Mcv:        mcvs,
			McvCount:   b.McvCount,
			BoundCount: b.BoundCount,
			UpperBound: nil,
		}
	}
	return ret, nil
}

func parseTypeString(types []string) ([]sql.Type, error) {
	if len(types) == 0 {
		return nil, nil
	}
	ret := make([]sql.Type, len(types))
	var err error
	typRegex := regexp.MustCompile("([a-z]+)\\((\\d+)\\)")
	for i, typ := range types {
		typMatch := typRegex.FindStringSubmatch(typ)
		colType := &sqlparser.ColumnType{}
		if typMatch == nil {
			colType.Type = typ
		} else {
			colType.Type = typMatch[1]
			if len(typMatch) > 2 {
				colType.Length = &sqlparser.SQLVal{Val: []byte(typMatch[2]), Type: sqlparser.IntVal}
			}
		}
		ret[i], err = types2.ColumnTypeToType(colType)
		if err != nil {
			return nil, fmt.Errorf("failed to parse histogram type: %s", typMatch)
		}
	}
	return ret, nil
}

func (s DoltHistogram) toSql() []sql.Bucket {
	ret := make([]sql.Bucket, len(s))
	for i, b := range s {
		upperBound := make([]interface{}, len(b.UpperBound))
		copy(upperBound, b.UpperBound)
		mcvs := make([][]interface{}, len(b.Mcv))
		for i, mcv := range b.Mcv {
			for _, v := range mcv {
				mcvs[i] = append(mcvs[i], v)
			}
		}
		ret[i] = sql.Bucket{
			Count:      b.Count,
			Distinct:   b.Distinct,
			Null:       b.Null,
			UpperBound: upperBound,
			BoundCount: b.BoundCount,
			Mcv:        mcvs,
			McvCount:   b.McvCount,
		}
	}
	return ret
}

type indexMeta struct {
	db    string
	table string
	index string
}

type statsMeta struct {
	db    string
	table string
	pref  string // comma separated
}

func NewProvider() *Provider {
	return &Provider{
		indexToStats: make(map[indexMeta][]statsMeta),
		stats:        make(map[statsMeta]*DoltStats),
	}
}

type Provider struct {
	latestRootAddr hash.Hash
	indexToStats   map[indexMeta][]statsMeta
	stats          map[statsMeta]*DoltStats
}

var _ sql.StatsProvider = (*Provider)(nil)

func (p *Provider) GetTableStats(ctx *sql.Context, db, table string) ([]*sql.Stats, error) {
	var ret []*sql.Stats
	for meta, stat := range p.stats {
		if strings.EqualFold(db, meta.db) && strings.EqualFold(table, meta.table) {
			ret = append(ret, stat.toSql())
		}
	}
	return ret, nil
}

func (p *Provider) SetStats(ctx *sql.Context, db, table string, stats *sql.Stats) error {
	meta := statsMeta{
		db:    strings.ToLower(db),
		table: strings.ToLower(table),
		pref:  strings.Join(stats.Columns, ","),
	}
	doltStats, err := DoltStatsFromSql(stats)
	if err != nil {
		return err
	}
	p.stats[meta] = doltStats
	return nil
}

func (p *Provider) GetStats(ctx *sql.Context, db, table string, cols []string) (*sql.Stats, bool) {
	meta := statsMeta{
		db:    strings.ToLower(db),
		table: strings.ToLower(table),
		pref:  strings.Join(cols, ","),
	}
	if s, ok := p.stats[meta]; ok {
		return s.toSql(), true
	}
	return nil, false
}

func (p *Provider) DropStats(ctx *sql.Context, db, table string, cols []string) error {
	meta := statsMeta{
		db:    strings.ToLower(db),
		table: strings.ToLower(table),
		pref:  strings.Join(cols, ","),
	}
	delete(p.stats, meta)
	return nil
}

func (p *Provider) RowCount(ctx *sql.Context, db, table string) (uint64, error) {
	var cnt uint64
	for meta, s := range p.stats {
		if strings.EqualFold(db, meta.db) && strings.EqualFold(table, meta.table) {
			if s.RowCount > cnt {
				cnt = s.RowCount
			}
		}
	}
	return cnt, nil
}

func (p *Provider) DataLength(ctx *sql.Context, db, table string) (uint64, error) {
	var avgSize uint64
	for meta, s := range p.stats {
		if strings.EqualFold(db, meta.db) && strings.EqualFold(table, meta.table) {
			if s.AvgSize > avgSize {
				avgSize = s.AvgSize
			}
		}
	}
	return 0, nil
}

func (p *Provider) RefreshTableStats(ctx *sql.Context, table sql.Table, db string) error {
	iat, ok := table.(sql.IndexAddressableTable)
	if !ok {
		return nil
	}
	indexes, err := iat.GetIndexes(ctx)
	if err != nil {
		return err
	}

	newIndexToStats := make(map[indexMeta][]statsMeta)
	tablePrefix := fmt.Sprintf("%s.", strings.ToLower(table.Name()))
	var idxMetas []indexMeta
	for _, idx := range indexes {
		idxMeta := indexMeta{
			db:    db,
			table: strings.ToLower(table.Name()),
			index: idx.ID(),
		}
		idxMetas = append(idxMetas, idxMeta)

		cols := make([]string, len(idx.Expressions()))
		for i, c := range idx.Expressions() {
			cols[i] = strings.TrimPrefix(strings.ToLower(c), tablePrefix)
		}

		// find all prefixes that don't already have statistics for this index
		// note: there can currently be duplicated prefixes for overlapping indexes
		for i := 1; i < len(cols)+1; i++ {
			pref := cols[:i]
			statMeta := statsMeta{
				db:    strings.ToLower(db),
				table: strings.ToLower(idx.Table()),
				pref:  strings.Join(pref, ","),
			}
			found := false
			for _, s := range newIndexToStats[idxMeta] {
				if s == statMeta {
					found = true
					break
				}
			}
			if !found {
				newIndexToStats[idxMeta] = append(newIndexToStats[idxMeta], statMeta)
			}
		}
	}

	// create statistics for |newIndexToStats| lists
	newStats, err := rebuildStats(ctx, indexes, idxMetas, newIndexToStats)
	if err != nil {
		return err
	}
	for meta, stats := range newStats {
		p.stats[meta] = stats
	}
	return nil
}
