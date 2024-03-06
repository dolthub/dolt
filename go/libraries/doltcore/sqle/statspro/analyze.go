package statspro

import (
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/go-mysql-server/sql"
	"strings"
)

func (p *Provider) RefreshTableStats(ctx *sql.Context, table sql.Table, db string) error {
	dSess := dsess.DSessFromSess(ctx.Session)
	branch, err := dSess.GetBranch()
	if err != nil {
		return err
	}

	tableName := strings.ToLower(table.Name())
	dbName := strings.ToLower(db)

	iat, ok := table.(sql.IndexAddressableTable)
	if !ok {
		return nil
	}
	indexes, err := iat.GetIndexes(ctx)
	if err != nil {
		return err
	}

	// it's important to update session references every call
	sqlTable, dTab, err := p.getLatestDoltDb(ctx, tableName, dbName, branch)
	if err != nil {
		return err
	}

	statDb, ok := p.statDbs[dbName]
	if !ok {
		fs, err := p.pro.FileSystemForDatabase(dbName)
		if err != nil {
			return err
		}
		sourceDb, ok := p.pro.BaseDatabase(ctx, dbName)
		if !ok {
			return sql.ErrDatabaseNotFound.New(dbName)
		}
		statDb, err = p.sf.Init(ctx, sourceDb, p.pro.DbFactoryUrl(), fs, env.GetCurrentUserHomeDir)
		if err != nil {
			ctx.Warn(0, err.Error())
			return nil
		}
		p.statDbs[dbName] = statDb
	}

	tablePrefix := fmt.Sprintf("%s.", tableName)
	var idxMetas []indexMeta
	for _, idx := range indexes {
		cols := make([]string, len(idx.Expressions()))
		for i, c := range idx.Expressions() {
			cols[i] = strings.TrimPrefix(strings.ToLower(c), tablePrefix)
		}

		qual := sql.NewStatQualifier(db, table.Name(), strings.ToLower(idx.ID()))
		curStat, ok := statDb.GetStat(branch, qual)
		if !ok {
			curStat = NewDoltStats()
			curStat.Qual = qual
		}
		idxMeta, err := newIdxMeta(ctx, curStat, dTab, idx, cols)
		if err != nil {
			return err
		}
		idxMetas = append(idxMetas, idxMeta)
	}

	newTableStats, err := createNewStatsBuckets(ctx, sqlTable, dTab, indexes, idxMetas)
	if err != nil {
		return err
	}

	// merge new chunks with preexisting chunks
	for _, idxMeta := range idxMetas {
		stat := newTableStats[idxMeta.qual]
		targetChunks := MergeNewChunks(idxMeta.allAddrs, idxMeta.keepChunks, stat.Histogram)
		stat.Chunks = idxMeta.allAddrs
		stat.Histogram = targetChunks
		stat.UpdateActive()
		if err := statDb.SetStat(ctx, branch, idxMeta.qual, stat); err != nil {
			return err
		}
	}

	return statDb.Flush(ctx, branch)
}

func (p *Provider) getLatestDoltDb(ctx *sql.Context, tableName string, dbName string, asOf string) (sql.Table, *doltdb.Table, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	prov := dSess.Provider()

	sqlDb, err := prov.Database(ctx, dbName)
	if err != nil {
		return nil, nil, err
	}

	sqlTable, ok, err := sqlDb.(sqle.Database).GetTableInsensitiveAsOf(ctx, tableName, asOf)
	//sqlTable, ok, err := sqlDb.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, fmt.Errorf("statistics refresh error: table not found %s", tableName)
	}

	var dTab *doltdb.Table
	switch t := sqlTable.(type) {
	case *sqle.AlterableDoltTable:
		dTab, err = t.DoltTable.DoltTable(ctx)
	case *sqle.WritableDoltTable:
		dTab, err = t.DoltTable.DoltTable(ctx)
	case *sqle.DoltTable:
		dTab, err = t.DoltTable(ctx)
	default:
		err = fmt.Errorf("failed to unwrap dolt table from type: %T", sqlTable)
	}
	if err != nil {
		return nil, nil, err
	}
	return sqlTable, dTab, nil
}

func newIdxMeta(ctx *sql.Context, curStats *DoltStats, doltTable *doltdb.Table, sqlIndex sql.Index, cols []string) (indexMeta, error) {
	var idx durable.Index
	var err error
	if strings.EqualFold(sqlIndex.ID(), "PRIMARY") {
		idx, err = doltTable.GetRowData(ctx)
	} else {
		idx, err = doltTable.GetIndexRowData(ctx, sqlIndex.ID())
	}
	if err != nil {
		return indexMeta{}, err
	}

	prollyMap := durable.ProllyMapFromIndex(idx)

	// get newest histogram target level hashes
	levelNodes, err := tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
	if err != nil {
		return indexMeta{}, err
	}

	var addrs []hash.Hash
	var keepChunks []DoltBucket
	var missingAddrs float64
	var missingChunks []tree.Node
	var missingOffsets [][]uint64
	var offset uint64

	for _, n := range levelNodes {
		// Compare the previous histogram chunks to the newest tree chunks.
		// Partition the newest chunks into 1) preserved or 2) missing.
		// Missing chunks will need to be scanned on a stats update, so
		// track the (start, end) ordinal offsets to simplify the read iter.
		treeCnt, err := n.TreeCount()
		if err != nil {
			return indexMeta{}, err
		}

		addrs = append(addrs, n.HashOf())
		if bucketIdx, ok := curStats.Active[n.HashOf()]; !ok {
			missingChunks = append(missingChunks, n)
			missingOffsets = append(missingOffsets, []uint64{offset, offset + uint64(treeCnt)})
			missingAddrs++
		} else {
			keepChunks = append(keepChunks, curStats.Histogram[bucketIdx])
		}
		offset += uint64(treeCnt)
	}

	var dropChunks []DoltBucket
	for _, h := range curStats.Chunks {
		var match bool
		for _, b := range keepChunks {
			if b.Chunk == h {
				match = true
				break
			}
		}
		if !match {
			dropChunks = append(dropChunks, curStats.Histogram[curStats.Active[h]])
		}
	}

	return indexMeta{
		qual:           curStats.Qual,
		cols:           cols,
		newNodes:       missingChunks,
		updateOrdinals: missingOffsets,
		keepChunks:     keepChunks,
		dropChunks:     dropChunks,
		allAddrs:       addrs,
	}, nil
}
