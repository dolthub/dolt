// Copyright 2025 Dolthub, Inc.
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

package statspro

import (
	"context"
	"errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"log"
	"strconv"
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

func (sc *StatsCoord) runGc(ctx context.Context, done chan struct{}) (err error) {
	defer func() {
		if err != nil {
			sc.enableGc.Store(true)
			close(done)
		}
	}()

	if !sc.enableGc.Swap(false) {
		return nil
	}

	if sc.Debug {
		log.Println("stats gc number: ", strconv.Itoa(int(sc.gcCounter.Load())))
	}

	sc.gcCounter.Add(1)

	sc.gcMu.Lock()
	defer sc.gcMu.Unlock()

	sqlCtx, err := sc.ctxGen(ctx)
	if err != nil {
		return err
	}

	if err := sc.kv.StartGc(ctx, int(sc.bucketCap)); err != nil {
		return err
	}

	// Can't take |dbMu| and provider lock, so copy dbs out.
	// Unlike branch updates, it is OK if GC misses databases
	// added in-between GC start and end because stats collection
	// is paused for the duration.
	sc.dbMu.Lock()
	dbs := make([]dsess.SqlDatabase, len(sc.dbs))
	copy(dbs, sc.dbs)
	sc.ddlGuard = true
	sc.dbMu.Unlock()

	var bucketCnt int
	for _, db := range dbs {
		j := NewGcMarkJob(db)
		cnt, err := sc.gcMark(sqlCtx, j)
		if sql.ErrDatabaseNotFound.Is(err) {
			// concurrent delete
			continue
		} else if errors.Is(err, doltdb.ErrWorkingSetNotFound) {
			// branch registered but no data
			continue
		} else if err != nil {
			return err
		}
		bucketCnt += cnt
	}

	//sc.bucketCnt.Store(int64(bucketCnt))
	sc.bucketCap = sc.kv.Cap()
	sc.kv.FinishGc()

	// Avoid GC starving the loop, only re-enable after
	// letting a block of other work through.
	if err := sc.unsafeAsyncSend(ctx, NewControl("re-enable GC", func(sc *StatsCoord) error {
		sc.enableGc.Store(true)
		close(done)
		return nil
	})); err != nil {
		return err
	}

	return nil
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

			idxLen := len(sqlIdx.Expressions())

			prollyMap := durable.ProllyMapFromIndex(idx)
			levelNodes, err := tree.GetHistogramLevel(sqlCtx, prollyMap.Tuples(), bucketLowCnt)
			if err != nil {
				return 0, err
			}

			if len(levelNodes) == 0 {
				continue
			}

			bucketCnt += len(levelNodes)

			firstNodeHash := levelNodes[0].HashOf()
			sc.kv.GetBound(firstNodeHash, idxLen)

			for _, n := range levelNodes {
				err = sc.kv.MarkBucket(sqlCtx, n.HashOf(), val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(idxLen)))
				if err != nil {
					return 0, err
				}
			}
		}
	}
	return bucketCnt, nil
}
