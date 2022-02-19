// Copyright 2019-2021 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
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

type indexParserF func([]byte) (tableIndex, error)

func newAWSChunkSource(ctx context.Context, ddb *ddbTableStore, s3 *s3ObjectReader, al awsLimits, name addr, chunkCount uint32, indexCache *indexCache, stats *Stats, parseIndex indexParserF) (cs chunkSource, err error) {
	if indexCache != nil {
		indexCache.lockEntry(name)
		defer func() {
			unlockErr := indexCache.unlockEntry(name)

			if err == nil {
				err = unlockErr
			}
		}()

		if index, found := indexCache.get(name); found {
			tra := &awsTableReaderAt{al: al, ddb: ddb, s3: s3, name: name, chunkCount: chunkCount}
			tr, err := newTableReader(index, tra, s3BlockSize)
			if err != nil {
				return &chunkSourceAdapter{}, err
			}
			return &chunkSourceAdapter{tr, name}, nil
		}
	}

	t1 := time.Now()
	index, tra, err := func() (tableIndex, tableReaderAt, error) {
		if al.tableMayBeInDynamo(chunkCount) {
			data, err := ddb.ReadTable(ctx, name, stats)

			if data == nil && err == nil { // There MUST be either data or an error
				return onHeapTableIndex{}, &dynamoTableReaderAt{}, errors.New("no data available")
			}

			if data != nil {
				stats.IndexBytesPerRead.Sample(uint64(len(data)))
				ind, err := parseTableIndexByCopy(data)
				if err != nil {
					return onHeapTableIndex{}, nil, err
				}
				return ind, &dynamoTableReaderAt{ddb: ddb, h: name}, nil
			}

			if _, ok := err.(tableNotInDynamoErr); !ok {
				return onHeapTableIndex{}, &dynamoTableReaderAt{}, err
			}
		}
		size := indexSize(chunkCount) + footerSize
		buff := make([]byte, size)
		n, _, err := s3.ReadFromEnd(ctx, name, buff, stats)
		if err != nil {
			return onHeapTableIndex{}, &dynamoTableReaderAt{}, err
		}
		if size != uint64(n) {
			return onHeapTableIndex{}, &dynamoTableReaderAt{}, errors.New("failed to read all data")
		}
		stats.IndexBytesPerRead.Sample(uint64(len(buff)))
		ind, err := parseTableIndex(buff)
		if err != nil {
			return onHeapTableIndex{}, &dynamoTableReaderAt{}, err
		}
		return ind, &s3TableReaderAt{s3: s3, h: name}, nil
	}()

	if err != nil {
		return &chunkSourceAdapter{}, err
	}

	stats.IndexReadLatency.SampleTimeSince(t1)

	if err != nil {
		return emptyChunkSource{}, err
	}

	if ohi, ok := index.(onHeapTableIndex); indexCache != nil && ok {
		indexCache.put(name, ohi)
	}

	tr, err := newTableReader(index, tra, s3BlockSize)
	if err != nil {
		return &chunkSourceAdapter{}, err
	}
	return &chunkSourceAdapter{tr, name}, nil
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
