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
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/hash"
)

const (
	s3RangePrefix = "bytes"
	s3BlockSize   = (1 << 10) * 512 // 512K
)

type s3TableReaderAt struct {
	s3 *s3ObjectReader
	h  hash.Hash
}

func (s3tra *s3TableReaderAt) Close() error {
	return nil
}

func (s3tra *s3TableReaderAt) clone() (tableReaderAt, error) {
	return s3tra, nil
}

func (s3tra *s3TableReaderAt) Reader(ctx context.Context) (io.ReadCloser, error) {
	return s3tra.s3.reader(ctx, s3tra.h.String())
}

func (s3tra *s3TableReaderAt) ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (n int, err error) {
	return s3tra.s3.ReadAt(ctx, s3tra.h.String(), p, off, stats)
}

const maxS3ReadFromEndReqSize = 256 * 1024 * 1024       // 256MB
const preferredS3ReadFromEndReqSize = 128 * 1024 * 1024 // 128MB

func readS3TableFileFromEnd(ctx context.Context, s3or *s3ObjectReader, name string, p []byte, stats *Stats) (n int, err error) {
	defer func(t1 time.Time) {
		stats.S3BytesPerRead.Sample(uint64(len(p)))
		stats.S3ReadLatency.SampleTimeSince(t1)
	}(time.Now())
	totalN := uint64(0)
	if len(p) > maxS3ReadFromEndReqSize {
		// If we're bigger than 256MB, parallelize the read...
		// Read the footer first and capture the size of the entire table file.
		n, sz, err := s3or.readRange(ctx, name, p[len(p)-footerSize:], fmt.Sprintf("%s=-%d", s3RangePrefix, footerSize))
		if err != nil {
			return n, err
		}
		totalN += uint64(n)
		eg, egctx := errgroup.WithContext(ctx)
		start := 0
		for start < len(p)-footerSize {
			// Make parallel read requests of up to 128MB.
			end := start + preferredS3ReadFromEndReqSize
			if end > len(p)-footerSize {
				end = len(p) - footerSize
			}
			bs := p[start:end]
			rangeStart := sz - uint64(len(p)) + uint64(start)
			rangeEnd := sz - uint64(len(p)) + uint64(end) - 1
			eg.Go(func() error {
				n, _, err := s3or.readRange(egctx, name, bs, fmt.Sprintf("%s=%d-%d", s3RangePrefix, rangeStart, rangeEnd))
				if err != nil {
					return err
				}
				atomic.AddUint64(&totalN, uint64(n))
				return nil
			})
			start = end
		}
		err = eg.Wait()
		if err != nil {
			return 0, err
		}
		return int(totalN), nil
	}
	n, _, err = s3or.readRange(ctx, name, p, fmt.Sprintf("%s=-%d", s3RangePrefix, len(p)))
	return n, err
}
