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
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/stats"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/statspro"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

func loadStats(ctx *sql.Context, db dsess.SqlDatabase, m prolly.Map) (map[sql.StatQualifier]*statspro.DoltStats, error) {
	qualToStats := make(map[sql.StatQualifier]*statspro.DoltStats)
	schemaName := db.SchemaName()
	iter, err := NewStatsIter(ctx, schemaName, m)
	if err != nil {
		return nil, err
	}
	currentStat := statspro.NewDoltStats()
	invalidTables := make(map[string]bool)
	for {
		row, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}

		// deserialize K, V
		dbName := row[schema.StatsDbTag].(string)
		tableName := row[schema.StatsTableTag].(string)
		indexName := row[schema.StatsIndexTag].(string)
		_ = row[schema.StatsVersionTag]
		commit := hash.Parse(row[schema.StatsCommitHashTag].(string))
		rowCount := row[schema.StatsRowCountTag].(uint64)
		distinctCount := row[schema.StatsDistinctCountTag].(uint64)
		nullCount := row[schema.StatsNullCountTag].(uint64)
		columns := strings.Split(row[schema.StatsColumnsTag].(string), ",")
		typesStr := row[schema.StatsTypesTag].(string)
		boundRowStr := row[schema.StatsUpperBoundTag].(string)
		upperBoundCnt := row[schema.StatsUpperBoundCntTag].(uint64)
		createdAt := row[schema.StatsCreatedAtTag].(time.Time)

		typs := strings.Split(typesStr, "\n")
		for i, t := range typs {
			typs[i] = strings.TrimSpace(t)
		}

		qual := sql.NewStatQualifier(dbName, schemaName, tableName, indexName)
		if _, ok := invalidTables[tableName]; ok {
			continue
		}

		if currentStat.Statistic.Qual.String() != qual.String() {
			if !currentStat.Statistic.Qual.Empty() {
				currentStat.UpdateActive()
				qualToStats[currentStat.Statistic.Qual] = currentStat
			}

			currentStat = statspro.NewDoltStats()

			tab, ok, err := db.GetTableInsensitive(ctx, qual.Table())
			if ok {
				currentStat.Statistic.Qual = qual
				currentStat.Statistic.Cols = columns
				currentStat.Statistic.LowerBnd, currentStat.Tb, currentStat.Statistic.Fds, currentStat.Statistic.Colset, err = loadRefdProps(ctx, db, tab, currentStat.Statistic.Qual, len(currentStat.Columns()))
				if err != nil {
					return nil, err
				}
			} else if !ok {
				ctx.GetLogger().Debugf("stats load: table previously collected is missing from root: %s", tableName)
				invalidTables[qual.Table()] = true
				continue
			} else if err != nil {
				return nil, err
			}
		}

		numMcvs := schema.StatsMcvCountsTag - schema.StatsMcv1Tag

		mcvCountsStr := strings.Split(row[schema.StatsMcvCountsTag].(string), ",")
		mcvCnts := make([]uint64, numMcvs)
		for i, v := range mcvCountsStr {
			if v == "" {
				continue
			}
			val, err := strconv.Atoi(v)
			if err != nil {
				return nil, err
			}
			mcvCnts[i] = uint64(val)
		}

		mcvs := make([]sql.Row, numMcvs)
		for i, v := range row[schema.StatsMcv1Tag:schema.StatsMcvCountsTag] {
			if v != nil && v != "" {
				row, err := DecodeRow(ctx, m.NodeStore(), v.(string), currentStat.Tb)
				if err != nil {
					return nil, err
				}
				mcvs[i] = row
			}
		}

		for i, v := range mcvCnts {
			if v == 0 {
				mcvs = mcvs[:i]
				mcvCnts = mcvCnts[:i]
				break
			}
		}

		if currentStat.Statistic.Hist == nil {
			currentStat.Statistic.Typs, err = parseTypeStrings(typs)
			if err != nil {
				return nil, err
			}
			currentStat.Statistic.Qual = qual
		}

		boundRow, err := DecodeRow(ctx, m.NodeStore(), boundRowStr, currentStat.Tb)
		if err != nil {
			return nil, err
		}

		bucket := statspro.DoltBucket{
			Chunk:   commit,
			Created: createdAt,
			Bucket: &stats.Bucket{
				RowCnt:      uint64(rowCount),
				DistinctCnt: uint64(distinctCount),
				NullCnt:     uint64(nullCount),
				McvVals:     mcvs,
				McvsCnt:     mcvCnts,
				BoundCnt:    upperBoundCnt,
				BoundVal:    boundRow,
			},
		}

		currentStat.Hist = append(currentStat.Hist, bucket)
		currentStat.Statistic.RowCnt += uint64(rowCount)
		currentStat.Statistic.DistinctCnt += uint64(distinctCount)
		currentStat.Statistic.NullCnt += uint64(rowCount)
		if currentStat.Statistic.Created.Before(createdAt) {
			currentStat.Statistic.Created = createdAt
		}
	}
	if !currentStat.Qualifier().Empty() {
		currentStat.UpdateActive()
		qualToStats[currentStat.Statistic.Qual] = currentStat
	}
	return qualToStats, nil
}

func parseTypeStrings(typs []string) ([]sql.Type, error) {
	var ret []sql.Type
	for _, typ := range typs {
		ct, err := planbuilder.ParseColumnTypeString(typ)
		if err != nil {
			return nil, err
		}
		ret = append(ret, ct)
	}
	return ret, nil
}

func loadRefdProps(ctx *sql.Context, db dsess.SqlDatabase, sqlTable sql.Table, qual sql.StatQualifier, cols int) (sql.Row, *val.TupleBuilder, *sql.FuncDepSet, sql.ColSet, error) {
	root, err := db.GetRoot(ctx)
	if err != nil {
		return nil, nil, nil, sql.ColSet{}, err
	}

	iat, ok := sqlTable.(sql.IndexAddressable)
	if !ok {
		return nil, nil, nil, sql.ColSet{}, nil
	}

	indexes, err := iat.GetIndexes(ctx)
	if err != nil {
		return nil, nil, nil, sql.ColSet{}, err
	}

	var sqlIdx sql.Index
	for _, i := range indexes {
		if strings.EqualFold(i.ID(), qual.Index()) {
			sqlIdx = i
			break
		}
	}

	if sqlIdx == nil {
		return nil, nil, nil, sql.ColSet{}, fmt.Errorf("%w: index not found: '%s'", statspro.ErrFailedToLoad, qual.Index())
	}

	fds, colset, err := stats.IndexFds(qual.Table(), sqlTable.Schema(), sqlIdx)
	if err != nil {
		return nil, nil, nil, sql.ColSet{}, err
	}
	table, ok, err := root.GetTable(ctx, doltdb.TableName{Name: sqlTable.Name()})
	if !ok {
		return nil, nil, nil, sql.ColSet{}, sql.ErrTableNotFound.New(qual.Table())
	}
	if err != nil {
		return nil, nil, nil, sql.ColSet{}, err
	}

	var idx durable.Index
	if qual.Index() == "primary" {
		idx, err = table.GetRowData(ctx)
	} else {
		idx, err = table.GetIndexRowData(ctx, qual.Index())
	}
	if err != nil {
		return nil, nil, nil, sql.ColSet{}, err
	}

	prollyMap := durable.ProllyMapFromIndex(idx)
	keyBuilder := val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(cols))
	buffPool := prollyMap.NodeStore().Pool()

	if cnt, err := prollyMap.Count(); err != nil {
		return nil, nil, nil, sql.ColSet{}, err
	} else if cnt == 0 {
		return nil, keyBuilder, nil, sql.ColSet{}, nil
	}
	firstIter, err := prollyMap.IterOrdinalRange(ctx, 0, 1)
	if err != nil {
		return nil, nil, nil, sql.ColSet{}, err
	}
	keyBytes, _, err := firstIter.Next(ctx)
	if err != nil {
		return nil, nil, nil, sql.ColSet{}, err
	}
	for i := range keyBuilder.Desc.Types {
		keyBuilder.PutRaw(i, keyBytes.GetField(i))
	}

	firstKey := keyBuilder.Build(buffPool)
	firstRow := make(sql.Row, keyBuilder.Desc.Count())
	for i := 0; i < keyBuilder.Desc.Count(); i++ {
		firstRow[i], err = tree.GetField(ctx, prollyMap.KeyDesc(), i, firstKey, prollyMap.NodeStore())
		if err != nil {
			return nil, nil, nil, sql.ColSet{}, err
		}
	}
	return firstRow, keyBuilder, fds, colset, nil
}

func loadFuncDeps(ctx *sql.Context, db dsess.SqlDatabase, qual sql.StatQualifier) (*sql.FuncDepSet, sql.ColSet, error) {
	tab, ok, err := db.GetTableInsensitive(ctx, qual.Table())
	if err != nil {
		return nil, sql.ColSet{}, err
	} else if !ok {
		return nil, sql.ColSet{}, fmt.Errorf("%w: table not found: '%s'", statspro.ErrFailedToLoad, qual.Table())
	}

	iat, ok := tab.(sql.IndexAddressable)
	if !ok {
		return nil, sql.ColSet{}, fmt.Errorf("%w: table does not have indexes: '%s'", statspro.ErrFailedToLoad, qual.Table())
	}

	indexes, err := iat.GetIndexes(ctx)
	if err != nil {
		return nil, sql.ColSet{}, err
	}

	var idx sql.Index
	for _, i := range indexes {
		if strings.EqualFold(i.ID(), qual.Index()) {
			idx = i
			break
		}
	}

	if idx == nil {
		return nil, sql.ColSet{}, fmt.Errorf("%w: index not found: '%s'", statspro.ErrFailedToLoad, qual.Index())
	}

	return stats.IndexFds(qual.Table(), tab.Schema(), idx)
}
