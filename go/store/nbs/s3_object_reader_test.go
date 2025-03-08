// Copyright 2025 Dolthub, Inc.
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

package nbs

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3ObjectReader(t *testing.T) {
	t.Run("LargeReadFromObjectEnd", func(t *testing.T) {
		ns := "5c2d1e52-e9bc-4779-881a-9087ad9a9f7b"
		obj := "rhi4dnrr2ov6420h7j419lc4f72u7egf"
		key := fmt.Sprintf("%s/%s", ns, obj)
		s3client := makeFakeS3(t)
		s3or := &s3ObjectReader{
			s3client,
			"dolthub-chunks-prod",
			nil,
			"5c2d1e52-e9bc-4779-881a-9087ad9a9f7b",
		}
		sz := maxS3ReadFromEndReqSize*2 + maxS3ReadFromEndReqSize/2
		data := make([]byte, sz)
		for i := range data {
			data[i] = byte((i % 256))
		}
		rd := make([]byte, sz-256)
		s3client.data[key] = data
		n, rdSz, err := s3or.readS3ObjectFromEnd(context.Background(), obj, rd, &Stats{})
		require.NoError(t, err)
		assert.Equal(t, uint64(sz), rdSz)
		assert.Equal(t, n, len(rd))
		assert.Equal(t, data[256:], rd)
	})
}
