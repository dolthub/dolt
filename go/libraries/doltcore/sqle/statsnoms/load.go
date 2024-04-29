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

	iter, err := NewStatsIter(ctx, m)
	if err != nil {
		return nil, err
	}
	currentStat := statspro.NewDoltStats()
	var lowerBound sql.Row
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

		typs := strings.Split(typesStr, ",")
		for i, t := range typs {
			typs[i] = strings.TrimSpace(t)
		}

		numMcvs := schema.StatsMcvCountsTag - schema.StatsMcv1Tag

		mcvCountsStr := strings.Split(row[schema.StatsMcvCountsTag].(string), ",")
		mcvCnts := make([]uint64, numMcvs)
		for i, v := range mcvCountsStr {
			val, err := strconv.Atoi(v)
			if err != nil {
				return nil, err
			}
			mcvCnts[i] = uint64(val)
		}

		mcvs := make([]sql.Row, numMcvs)
		for i, v := range row[schema.StatsMcv1Tag:schema.StatsMcvCountsTag] {
			if v != nil {
				row, err := iter.ParseRow(v.(string))
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

		boundRow, err := iter.ParseRow(boundRowStr)
		if err != nil {
			return nil, err
		}

		qual := sql.NewStatQualifier(dbName, tableName, indexName)
		if currentStat.Statistic.Qual.String() != qual.String() {
			if !currentStat.Statistic.Qual.Empty() {
				currentStat.Statistic.LowerBnd, err = loadLowerBound(ctx, currentStat.Statistic.Qual)
				if err != nil {
					return nil, err
				}
				fds, colSet, err := loadFuncDeps(ctx, db, currentStat.Statistic.Qual)
				if err != nil {
					return nil, err
				}
				currentStat.Statistic.Fds = fds
				currentStat.Statistic.Colset = colSet
				currentStat.UpdateActive()
				qualToStats[currentStat.Statistic.Qual] = currentStat
			}

			currentStat = statspro.NewDoltStats()
			currentStat.Statistic.Qual = qual
			currentStat.Statistic.Cols = columns
			currentStat.Statistic.LowerBnd = lowerBound
		}

		if currentStat.Statistic.Hist == nil {
			currentStat.Statistic.Typs, err = stats.ParseTypeStrings(typs)
			if err != nil {
				return nil, err
			}
			currentStat.Statistic.Qual = qual
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
	currentStat.Statistic.LowerBnd, err = loadLowerBound(ctx, currentStat.Statistic.Qual)
	if err != nil {
		return nil, err
	}
	fds, colSet, err := loadFuncDeps(ctx, db, currentStat.Statistic.Qual)
	if err != nil {
		return nil, err
	}
	currentStat.Statistic.Fds = fds
	currentStat.Statistic.Colset = colSet
	currentStat.UpdateActive()
	qualToStats[currentStat.Statistic.Qual] = currentStat
	return qualToStats, nil
}

func loadLowerBound(ctx *sql.Context, qual sql.StatQualifier) (sql.Row, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	roots, ok := dSess.GetRoots(ctx, qual.Db())
	if !ok {
		return nil, nil
	}

	table, ok, err := roots.Head.GetTable(ctx, doltdb.TableName{Name: qual.Table()})
	if !ok {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	idx, err := table.GetIndexRowData(ctx, qual.Index())
	if err != nil {
		return nil, err
	}

	prollyMap := durable.ProllyMapFromIndex(idx)
	keyBuilder := val.NewTupleBuilder(prollyMap.KeyDesc())
	buffPool := prollyMap.NodeStore().Pool()

	firstIter, err := prollyMap.IterOrdinalRange(ctx, 0, 1)
	if err != nil {
		return nil, err
	}
	keyBytes, _, err := firstIter.Next(ctx)
	if err != nil {
		return nil, err
	}
	for i := range keyBuilder.Desc.Types {
		keyBuilder.PutRaw(i, keyBytes.GetField(i))
	}

	firstKey := keyBuilder.Build(buffPool)
	var firstRow sql.Row
	for i := 0; i < keyBuilder.Desc.Count(); i++ {
		firstRow[i], err = tree.GetField(ctx, prollyMap.KeyDesc(), i, firstKey, prollyMap.NodeStore())
		if err != nil {
			return nil, err
		}
	}
	return firstRow, nil
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
