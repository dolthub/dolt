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
	"time"
)

func tableExistsInChunkSource(ctx context.Context, ddb *ddbTableStore, s3 *s3ObjectReader, al awsLimits, name addr, chunkCount uint32, q MemoryQuotaProvider, stats *Stats) (bool, error) {
	idxSz := int(indexSize(chunkCount) + footerSize)
	offsetSz := int((chunkCount - (chunkCount / 2)) * offsetSize)
	buf, err := q.AcquireQuotaBytes(ctx, uint64(idxSz+offsetSz))
	if err != nil {
		return false, err
	}
	p := buf[:idxSz]

	if al.tableMayBeInDynamo(chunkCount) {
		data, err := ddb.ReadTable(ctx, name, nil)
		if err != nil {
			return false, err
		}
		if data == nil {
			return false, nil
		}
		if len(p) > len(data) {
			return false, errors.New("not enough data for chunk count")
		}
		return true, nil
	}

	n, _, err := s3.ReadFromEnd(ctx, name, p, stats)
	if err != nil {
		return false, err
	}
	if len(p) != n {
		return false, errors.New("failed to read all data")
	}
	return true, nil
}

func newAWSChunkSource(ctx context.Context, ddb *ddbTableStore, s3 *s3ObjectReader, al awsLimits, name addr, chunkCount uint32, q MemoryQuotaProvider, stats *Stats) (cs chunkSource, err error) {
	var tra tableReaderAt
	index, err := loadTableIndex(ctx, stats, chunkCount, q, func(p []byte) error {
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

func loadTableIndex(ctx context.Context, stats *Stats, cnt uint32, q MemoryQuotaProvider, loadIndexBytes func(p []byte) error) (tableIndex, error) {
	idxSz := int(indexSize(cnt) + footerSize)
	offsetSz := int((cnt - (cnt / 2)) * offsetSize)
	buf, err := q.AcquireQuotaBytes(ctx, uint64(idxSz+offsetSz))
	if err != nil {
		return nil, err
	}

	t1 := time.Now()
	if err := loadIndexBytes(buf[:idxSz]); err != nil {
		return nil, err
	}
	stats.IndexReadLatency.SampleTimeSince(t1)
	stats.IndexBytesPerRead.Sample(uint64(len(buf)))

	return parseTableIndexWithOffsetBuff(buf[:idxSz], buf[idxSz:], q)
}
