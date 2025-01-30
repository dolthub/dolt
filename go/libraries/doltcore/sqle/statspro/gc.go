package statspro

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"log"
	"strings"
)

type GcMarkJob struct {
	sqlDb dsess.SqlDatabase
	done  chan struct{}
}

func NewGcMarkJob(sqlDb dsess.SqlDatabase) GcMarkJob {
	return GcMarkJob{
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

func (sc *StatsCoord) gcMark(sqlCtx *sql.Context, j GcMarkJob) (int, error) {
	dSess := dsess.DSessFromSess(sqlCtx.Session)
	db, err := dSess.Provider().Database(sqlCtx, j.sqlDb.AliasedName())
	if err != nil {
		return 0, err
	}
	sqlDb, err := sqle.RevisionDbForBranch(sqlCtx, db.(dsess.SqlDatabase), j.sqlDb.Revision(), j.sqlDb.Revision()+"/"+j.sqlDb.AliasedName())
	if err != nil {
		return 0, err
	}
	tableNames, err := sqlDb.GetTableNames(sqlCtx)
	if err != nil {
		return 0, err
	}

	var bucketCnt int
	for _, tableName := range tableNames {
		sqlTable, dTab, err := GetLatestTable(sqlCtx, tableName, j.sqlDb)
		if err != nil {
			return 0, err
		}
		indexes, err := sqlTable.GetIndexes(sqlCtx)
		if err != nil {
			return 0, err
		}

		for _, sqlIdx := range indexes {
			var idx durable.Index
			var err error
			if strings.EqualFold(sqlIdx.ID(), "PRIMARY") {
				idx, err = dTab.GetRowData(sqlCtx)
			} else {
				idx, err = dTab.GetIndexRowData(sqlCtx, sqlIdx.ID())
			}
			if err != nil {
				return 0, err
			}

			schHash, _, err := sqlTable.IndexCacheKey(sqlCtx)
			key := templateCacheKey{h: schHash.Hash, idxName: sqlIdx.ID()}
			sc.kv.GetTemplate(key)

			idxCnt := len(sqlIdx.Expressions())

			prollyMap := durable.ProllyMapFromIndex(idx)
			levelNodes, err := tree.GetHistogramLevel(sqlCtx, prollyMap.Tuples(), bucketLowCnt)
			if err != nil {
				return 0, err
			}

			if len(levelNodes) == 0 {
				log.Println("db-table has no hashes: ", sqlDb.AliasedName())
				continue
			}

			bucketCnt += len(levelNodes)

			firstNodeHash := levelNodes[0].HashOf()
			sc.kv.GetBound(firstNodeHash)

			for _, n := range levelNodes {
				err = sc.kv.MarkBucket(sqlCtx, n.HashOf(), val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(idxCnt)))
				if err != nil {
					return 0, err
				}
			}
		}
	}
	return bucketCnt, nil
}
