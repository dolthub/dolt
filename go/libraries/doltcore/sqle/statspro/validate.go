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

package statspro

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"log"
	"strings"
)

func generateDeps(
	sqlCtx *sql.Context,
	sqlDb dsess.SqlDatabase,
	tCb func(key templateCacheKey),
	bCb func(h hash.Hash),
	hCb func(h hash.Hash, tupB *val.TupleBuilder) error,
) error {
	dSess := dsess.DSessFromSess(sqlCtx.Session)
	db, err := dSess.Provider().Database(sqlCtx, sqlDb.AliasedName())
	if err != nil {
		return err
	}
	sqlDb, err = sqle.RevisionDbForBranch(sqlCtx, db.(dsess.SqlDatabase), sqlDb.Revision(), sqlDb.Revision()+"/"+sqlDb.AliasedName())
	if err != nil {
		return err
	}
	tableNames, err := sqlDb.GetTableNames(sqlCtx)
	if err != nil {
		return err
	}

	var bucketCnt int
	for _, tableName := range tableNames {
		sqlTable, dTab, err := GetLatestTable(sqlCtx, tableName, sqlDb)
		if err != nil {
			return err
		}
		indexes, err := sqlTable.GetIndexes(sqlCtx)
		if err != nil {
			return err
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
				return err
			}

			schHash, _, err := sqlTable.IndexCacheKey(sqlCtx)
			key := templateCacheKey{h: schHash.Hash, idxName: sqlIdx.ID()}
			tCb(key)

			idxCnt := len(sqlIdx.Expressions())

			prollyMap := durable.ProllyMapFromIndex(idx)
			levelNodes, err := tree.GetHistogramLevel(sqlCtx, prollyMap.Tuples(), bucketLowCnt)
			if err != nil {
				return err
			}

			if len(levelNodes) == 0 {
				log.Println("db-table has no hashes: ", sqlDb.AliasedName())
				continue
			}

			bucketCnt += len(levelNodes)

			firstNodeHash := levelNodes[0].HashOf()
			bCb(firstNodeHash)

			for _, n := range levelNodes {
				err = hCb(n.HashOf(), val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(idxCnt)))
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// validateState expects all tracked databases to be fully cached,
// and returns an error including any gaps.
func (sc *StatsCoord) validateState(ctx context.Context) error {
	sc.dbMu.Lock()
	dbs := make([]dsess.SqlDatabase, len(sc.dbs))
	copy(dbs, sc.dbs)
	sc.dbMu.Unlock()

	sc.gcMu.Lock()
	defer sc.gcMu.Unlock()

	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()

	sqlCtx, err := sc.ctxGen(ctx)
	if err != nil {
		return err
	}

	b := strings.Builder{}
	for i, db := range dbs {
		_ = i
		generateDeps(sqlCtx, db, func(key templateCacheKey) {
			_, ok := sc.kv.GetTemplate(key)
			if !ok {
				fmt.Fprintf(&b, "stats db (%s) missing cache template (%s)\n", db.RevisionQualifiedName(), key.String())
			}
		}, func(h hash.Hash) {
			_, ok := sc.kv.GetBound(h)
			if !ok {
				fmt.Fprintf(&b, "stats db (%s) missing cache bound (%s)\n", db.RevisionQualifiedName(), h.String()[:5])
			}
		}, func(h hash.Hash, tupB *val.TupleBuilder) error {
			_, ok, err := sc.kv.GetBucket(ctx, h, tupB)
			if err != nil {
				return err
			}
			if !ok {
				fmt.Fprintf(&b, "stats db (%s) missing cache chunk (%s)\n", db.RevisionQualifiedName(), h.String()[:5])
			}
			return nil
		})
	}
	return fmt.Errorf(b.String())
}
