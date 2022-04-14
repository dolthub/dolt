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

func newAWSChunkSource(ctx context.Context, ddb *ddbTableStore, s3 *s3ObjectReader, al awsLimits, name addr, chunkCount uint32, q MemoryQuotaProvider, stats *Stats) (cs chunkSource, err error) {
	var tra tableReaderAt
	index, err := loadTableIndex(stats, chunkCount, q, func(p []byte) error {
		if al.tableMayBeInDynamo(chunkCount) {
			data, err := ddb.ReadTable(ctx, name, stats)
			if data == nil && err == nil { // There MUST be either data or an error
				return errors.New("no data available")
			}
			if data != nil {
				if len(p) > len(data) {
					return errors.New("not enough data for chunk count")
				}
				indexBytes := data[len(data)-len(p):]
				copy(p, indexBytes)
				tra = &dynamoTableReaderAt{ddb: ddb, h: name}
				return nil
			}
			if _, ok := err.(tableNotInDynamoErr); !ok {
				return err
			}
		}

		n, _, err := s3.ReadFromEnd(ctx, name, p, stats)
		if err != nil {
			return err
		}
		if len(p) != n {
			return errors.New("failed to read all data")
		}
		tra = &s3TableReaderAt{h: name, s3: s3}
		return nil
	})
	if err != nil {
		return &chunkSourceAdapter{}, err
	}

	tr, err := newTableReader(index, tra, s3BlockSize)
	if err != nil {
		_ = index.Close()
		return &chunkSourceAdapter{}, err
	}
	return &chunkSourceAdapter{tr, name}, nil
}

func loadTableIndex(stats *Stats, chunkCount uint32, q MemoryQuotaProvider, loadIndexBytes func(p []byte) error) (tableIndex, error) {
	ti, err := newMmapTableIndex(chunkCount)
	if err != nil {
		return nil, err
	}

	t1 := time.Now()
	err = loadIndexBytes(ti.indexDataBuff)
	if err != nil {
		_ = ti.mmapped.Unmap()
		return onHeapTableIndex{}, err
	}
	stats.IndexReadLatency.SampleTimeSince(t1)
	stats.IndexBytesPerRead.Sample(uint64(len(ti.indexDataBuff)))

	err = ti.parseIndexBuffer(q)
	if err != nil {
		_ = ti.mmapped.Unmap()
		return nil, err
	}

	return ti, nil
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
