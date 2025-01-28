package statspro

import (
	"context"
	"errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"strings"
)

type GcMarkJob struct {
	ctx   *sql.Context
	sqlDb dsess.SqlDatabase
	done  chan struct{}
}

func NewGcMarkJob(ctx *sql.Context, sqlDb dsess.SqlDatabase) GcMarkJob {
	return GcMarkJob{
		ctx:   ctx,
		sqlDb: sqlDb,
		done:  make(chan struct{}),
	}
}

func (j GcMarkJob) Finish() {
	close(j.done)
}

func (j GcMarkJob) String() string {
	b := strings.Builder{}
	b.WriteString("gcMark: ")
	b.WriteString(j.sqlDb.RevisionQualifiedName())
	return b.String()
}

func (sc *StatsCoord) gcMark(ctx context.Context, j GcMarkJob) (int, error) {
	tableNames, err := j.sqlDb.GetTableNames(j.ctx)
	if err != nil {
		if errors.Is(err, doltdb.ErrBranchNotFound) {
			return 0, nil
		}
		return 0, err
	}

	var bucketCnt int
	for _, tableName := range tableNames {
		sqlTable, dTab, err := GetLatestTable(j.ctx, tableName, j.sqlDb)
		if err != nil {
			return 0, err
		}
		indexes, err := sqlTable.GetIndexes(j.ctx)
		if err != nil {
			return 0, err
		}

		for _, sqlIdx := range indexes {
			var idx durable.Index
			var err error
			if strings.EqualFold(sqlIdx.ID(), "PRIMARY") {
				idx, err = dTab.GetRowData(ctx)
			} else {
				idx, err = dTab.GetIndexRowData(ctx, sqlIdx.ID())
			}
			if err != nil {
				return 0, err
			}

			schHash, _, err := sqlTable.IndexCacheKey(j.ctx)
			key := templateCacheKey{h: schHash.Hash, idxName: sqlIdx.ID()}
			sc.kv.GetTemplate(key)

			idxCnt := len(sqlIdx.Expressions())

			prollyMap := durable.ProllyMapFromIndex(idx)
			levelNodes, err := tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
			if err != nil {
				return 0, err
			}

			bucketCnt += len(levelNodes)

			firstNodeHash := levelNodes[0].HashOf()
			sc.kv.GetBound(firstNodeHash)

			for _, n := range levelNodes {
				err = sc.kv.MarkBucket(ctx, n.HashOf(), val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(idxCnt)))
				if err != nil {
					return 0, err
				}
			}
		}
	}
	return bucketCnt, nil
}
