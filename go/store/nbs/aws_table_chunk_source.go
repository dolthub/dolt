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

	"github.com/dolthub/dolt/go/store/hash"
)

func newAWSTableFileChunkSource(ctx context.Context, s3 *s3ObjectReader, al awsLimits, name hash.Hash, chunkCount uint32, q MemoryQuotaProvider, stats *Stats) (cs chunkSource, err error) {
	var tra tableReaderAt
	index, err := loadTableIndex(ctx, stats, chunkCount, q, func(p []byte) error {
		n, err := s3.readS3ObjectFromEnd(ctx, name.String(), p, stats)
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
	buf, err := q.AcquireQuotaBytes(ctx, idxSz+offsetSz)
	if err != nil {
		return nil, err
	}

	t1 := time.Now()
	if err := loadIndexBytes(buf[:idxSz]); err != nil {
		q.ReleaseQuotaBytes(len(buf))
		return nil, err
	}
	stats.IndexReadLatency.SampleTimeSince(t1)
	stats.IndexBytesPerRead.Sample(uint64(len(buf)))

	idx, err := parseTableIndexWithOffsetBuff(buf[:idxSz], buf[idxSz:], q)
	if err != nil {
		q.ReleaseQuotaBytes(len(buf))
	}
	return idx, err
}
