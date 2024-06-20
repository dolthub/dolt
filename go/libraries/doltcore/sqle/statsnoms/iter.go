// Copyright 2024 Dolthub, Inc.
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

package statsnoms

import (
	"fmt"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"gopkg.in/errgo.v2/errors"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

var ErrIncompatibleVersion = errors.New("client stats version mismatch")

func NewStatsIter(ctx *sql.Context, m prolly.Map) (*statsIter, error) {
	iter, err := m.IterAll(ctx)
	if err != nil {
		return nil, err
	}
	kd, vd := m.Descriptors()
	keyBuilder := val.NewTupleBuilder(kd)
	valueBuilder := val.NewTupleBuilder(vd)
	ns := m.NodeStore()

	return &statsIter{
		iter:  iter,
		kb:    keyBuilder,
		vb:    valueBuilder,
		ns:    ns,
		planb: planbuilder.New(ctx, nil, sql.NewMysqlParser()),
	}, nil
}

// statsIter reads histogram buckets into string-compatible types.
// Values that are SQL rows should be converted with statsIter.ParseRow.
// todo: make a JSON compatible container for sql.Row w/ types so that we
// can eagerly convert to sql.Row without sacrificing string printing.
type statsIter struct {
	iter         prolly.MapIter
	kb, vb       *val.TupleBuilder
	ns           tree.NodeStore
	planb        *planbuilder.Builder
	currentQual  string
	currentTypes []sql.Type
}

var _ sql.RowIter = (*statsIter)(nil)

func (s *statsIter) Next(ctx *sql.Context) (sql.Row, error) {
	k, v, err := s.iter.Next(ctx)
	if err != nil {
		return nil, err
	}

	// deserialize K, V
	version, err := tree.GetField(ctx, s.vb.Desc, 0, v, s.ns)
	if err != nil {
		return nil, err
	}
	if version != schema.StatsVersion {
		return nil, fmt.Errorf("%w: write version %d does not match read version %d", ErrIncompatibleVersion, version, schema.StatsVersion)
	}

	var row sql.Row
	for i := 0; i < s.kb.Desc.Count(); i++ {
		f, err := tree.GetField(ctx, s.kb.Desc, i, k, s.ns)
		if err != nil {
			return nil, err
		}
		row = append(row, f)
	}

	for i := 0; i < s.vb.Desc.Count(); i++ {
		f, err := tree.GetField(ctx, s.vb.Desc, i, v, s.ns)
		if err != nil {
			return nil, err
		}
		row = append(row, f)
	}

	dbName := row[schema.StatsDbTag].(string)
	tableName := row[schema.StatsTableTag].(string)
	indexName := row[schema.StatsIndexTag].(string)
	position := row[schema.StatsPositionTag].(int64)
	_ = row[schema.StatsVersionTag]
	commit := hash.Parse(row[schema.StatsCommitHashTag].(string))
	rowCount := row[schema.StatsRowCountTag].(int64)
	distinctCount := row[schema.StatsDistinctCountTag].(int64)
	nullCount := row[schema.StatsNullCountTag].(int64)
	columnsStr := row[schema.StatsColumnsTag].(string)
	typesStr := row[schema.StatsTypesTag].(string)
	upperBoundStr := row[schema.StatsUpperBoundTag].(string)
	upperBoundCnt := row[schema.StatsUpperBoundCntTag].(int64)
	createdAt := row[schema.StatsCreatedAtTag].(time.Time)

	typs := strings.Split(typesStr, "\n")
	for i, t := range typs {
		typs[i] = strings.TrimSpace(t)
	}

	qual := sql.NewStatQualifier(dbName, tableName, indexName)
	if curQual := qual.String(); !strings.EqualFold(curQual, s.currentQual) {
		s.currentQual = curQual
		s.currentTypes, err = parseTypeStrings(typs)
		if err != nil {
			return nil, err
		}
	}

	mcvCountsStr := row[schema.StatsMcvCountsTag].(string)

	numMcvs := schema.StatsMcvCountsTag - schema.StatsMcv1Tag
	mcvs := make([]string, numMcvs)
	for i, v := range row[schema.StatsMcv1Tag:schema.StatsMcvCountsTag] {
		if v != nil {
			mcvs[i] = v.(string)
		}
	}

	return sql.Row{
		dbName,
		tableName,
		indexName,
		int(position),
		version,
		commit.String(),
		uint64(rowCount),
		uint64(distinctCount),
		uint64(nullCount),
		columnsStr,
		typesStr,
		upperBoundStr,
		uint64(upperBoundCnt),
		createdAt,
		mcvs[0], mcvs[1], mcvs[2], mcvs[3],
		mcvCountsStr,
	}, nil
}

func (s *statsIter) ParseRow(rowStr string) (sql.Row, error) {
	var row sql.Row
	for i, v := range strings.Split(rowStr, ",") {
		val, _, err := s.currentTypes[i].Convert(v)
		if err != nil {
			return nil, err
		}
		row = append(row, val)
	}
	return row, nil
}

func (s *statsIter) Close(context *sql.Context) error {
	return nil
}
