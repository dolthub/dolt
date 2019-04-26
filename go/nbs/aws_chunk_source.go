// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"sync"
	"time"

	"github.com/attic-labs/noms/go/d"
)

func newAWSChunkSource(ctx context.Context, ddb *ddbTableStore, s3 *s3ObjectReader, al awsLimits, name addr, chunkCount uint32, indexCache *indexCache, stats *Stats) chunkSource {
	if indexCache != nil {
		indexCache.lockEntry(name)
		defer indexCache.unlockEntry(name)
		if index, found := indexCache.get(name); found {
			tra := &awsTableReaderAt{al: al, ddb: ddb, s3: s3, name: name, chunkCount: chunkCount}
			return &chunkSourceAdapter{newTableReader(index, tra, s3BlockSize), name}
		}
	}

	t1 := time.Now()
	indexBytes, tra := func() ([]byte, tableReaderAt) {
		if al.tableMayBeInDynamo(chunkCount) {
			data, err := ddb.ReadTable(ctx, name, stats)
			if data != nil {
				return data, &dynamoTableReaderAt{ddb: ddb, h: name}
			}
			d.PanicIfTrue(err == nil) // There MUST be either data or an error
			d.PanicIfNotType(err, tableNotInDynamoErr{})
		}

		size := indexSize(chunkCount) + footerSize
		buff := make([]byte, size)

		n, err := s3.ReadFromEnd(ctx, name, buff, stats)
		d.PanicIfError(err)
		d.PanicIfFalse(size == uint64(n))
		return buff, &s3TableReaderAt{s3: s3, h: name}
	}()
	stats.IndexBytesPerRead.Sample(uint64(len(indexBytes)))
	stats.IndexReadLatency.SampleTimeSince(t1)

	index := parseTableIndex(indexBytes)
	if indexCache != nil {
		indexCache.put(name, index)
	}
	return &chunkSourceAdapter{newTableReader(index, tra, s3BlockSize), name}
}

type awsTableReaderAt struct {
	once sync.Once
	tra  tableReaderAt

	al  awsLimits
	ddb *ddbTableStore
	s3  *s3ObjectReader

	name       addr
	chunkCount uint32
}

func (atra *awsTableReaderAt) hash() addr {
	return atra.name
}

func (atra *awsTableReaderAt) ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (n int, err error) {
	atra.once.Do(func() { atra.tra = atra.getTableReaderAt(ctx, stats) })
	return atra.tra.ReadAtWithStats(ctx, p, off, stats)
}

func (atra *awsTableReaderAt) getTableReaderAt(ctx context.Context, stats *Stats) tableReaderAt {
	if atra.al.tableMayBeInDynamo(atra.chunkCount) {
		data, err := atra.ddb.ReadTable(ctx, atra.name, stats)
		if data != nil {
			return &dynamoTableReaderAt{ddb: atra.ddb, h: atra.name}
		}
		d.PanicIfTrue(err == nil) // There MUST be either data or an error
		d.PanicIfNotType(err, tableNotInDynamoErr{})
	}

	return &s3TableReaderAt{s3: atra.s3, h: atra.name}
}
