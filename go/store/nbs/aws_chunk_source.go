// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"errors"
	"sync"
	"time"
)

func newAWSChunkSource(ctx context.Context, ddb *ddbTableStore, s3 *s3ObjectReader, al awsLimits, name addr, chunkCount uint32, indexCache *indexCache, stats *Stats) (chunkSource, error) {
	if indexCache != nil {
		indexCache.lockEntry(name)
		defer indexCache.unlockEntry(name)
		if index, found := indexCache.get(name); found {
			tra := &awsTableReaderAt{al: al, ddb: ddb, s3: s3, name: name, chunkCount: chunkCount}
			return &chunkSourceAdapter{newTableReader(index, tra, s3BlockSize), name}, nil
		}
	}

	t1 := time.Now()
	indexBytes, tra, err := func() ([]byte, tableReaderAt, error) {
		if al.tableMayBeInDynamo(chunkCount) {
			data, err := ddb.ReadTable(ctx, name, stats)

			if data == nil && err == nil { // There MUST be either data or an error
				return nil, &dynamoTableReaderAt{}, errors.New("no data available")
			}

			if data != nil {
				return data, &dynamoTableReaderAt{ddb: ddb, h: name}, nil
			}

			if _, ok := err.(tableNotInDynamoErr); !ok {
				return nil, &dynamoTableReaderAt{}, err
			}
		}

		size := indexSize(chunkCount) + footerSize
		buff := make([]byte, size)

		n, err := s3.ReadFromEnd(ctx, name, buff, stats)

		if err != nil {
			return nil, &dynamoTableReaderAt{}, err
		}

		if size != uint64(n) {
			return nil, &dynamoTableReaderAt{}, errors.New("failed to read all data")
		}

		return buff, &s3TableReaderAt{s3: s3, h: name}, nil
	}()

	if err != nil {
		return &chunkSourceAdapter{}, err
	}

	stats.IndexBytesPerRead.Sample(uint64(len(indexBytes)))
	stats.IndexReadLatency.SampleTimeSince(t1)

	index, err := parseTableIndex(indexBytes)

	if err != nil {
		return emptyChunkSource{}, err
	}

	if indexCache != nil {
		indexCache.put(name, index)
	}

	return &chunkSourceAdapter{newTableReader(index, tra, s3BlockSize), name}, nil
}

type awsTableReaderAt struct {
	once     sync.Once
	getTRErr error
	tra      tableReaderAt

	al  awsLimits
	ddb *ddbTableStore
	s3  *s3ObjectReader

	name       addr
	chunkCount uint32
}

func (atra *awsTableReaderAt) ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (int, error) {
	atra.once.Do(func() {
		atra.tra, atra.getTRErr = atra.getTableReaderAt(ctx, stats)
	})

	if atra.getTRErr != nil {
		return 0, atra.getTRErr
	}

	return atra.tra.ReadAtWithStats(ctx, p, off, stats)
}

func (atra *awsTableReaderAt) getTableReaderAt(ctx context.Context, stats *Stats) (tableReaderAt, error) {
	if atra.al.tableMayBeInDynamo(atra.chunkCount) {
		data, err := atra.ddb.ReadTable(ctx, atra.name, stats)

		if data == nil && err == nil { // There MUST be either data or an error
			return &dynamoTableReaderAt{}, errors.New("no data available")
		}

		if data != nil {
			return &dynamoTableReaderAt{ddb: atra.ddb, h: atra.name}, nil
		}

		if _, ok := err.(tableNotInDynamoErr); !ok {
			return &dynamoTableReaderAt{}, err
		}
	}

	return &s3TableReaderAt{s3: atra.s3, h: atra.name}, nil
}
