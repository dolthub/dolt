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
	"io"
)

const (
	s3RangePrefix = "bytes"
	s3BlockSize   = (1 << 10) * 512 // 512K
)

type s3TableReaderAt struct {
	s3  *s3ObjectReader
	key string
}

func (s3tra *s3TableReaderAt) Close() error {
	return nil
}

func (s3tra *s3TableReaderAt) clone() (tableReaderAt, error) {
	return s3tra, nil
}

func (s3tra *s3TableReaderAt) Reader(ctx context.Context) (io.ReadCloser, error) {
	return s3tra.s3.reader(ctx, s3tra.key)
}

func (s3tra *s3TableReaderAt) ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (n int, err error) {
	return s3tra.s3.ReadAt(ctx, s3tra.key, p, off, stats)
}

const maxS3ReadFromEndReqSize = 256 * 1024 * 1024       // 256MB
const preferredS3ReadFromEndReqSize = 128 * 1024 * 1024 // 128MB
