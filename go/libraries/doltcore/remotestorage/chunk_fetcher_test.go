// Copyright 2024 Dolthub, Inc.
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

package remotestorage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestFetcherHashSetToGetDlLocsReqsThread(t *testing.T) {
	t.Run("ImmediateClose", func(t *testing.T) {
		reqCh := make(chan hash.HashSet)
		close(reqCh)

		resCh := make(chan *remotesapi.GetDownloadLocsRequest)

		err := fetcherHashSetToGetDlLocsReqsThread(context.Background(), reqCh, nil, resCh, 32, "", testIdFunc)
		assert.NoError(t, err)
		_, ok := <-resCh
		assert.False(t, ok)
	})

	t.Run("CanceledContext", func(t *testing.T) {
		reqCh := make(chan hash.HashSet)
		resCh := make(chan *remotesapi.GetDownloadLocsRequest)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := fetcherHashSetToGetDlLocsReqsThread(ctx, reqCh, nil, resCh, 32, "", testIdFunc)
		assert.Error(t, err)
	})

	t.Run("BatchesAsExpected", func(t *testing.T) {
		reqCh := make(chan hash.HashSet)
		resCh := make(chan *remotesapi.GetDownloadLocsRequest)

		eg, ctx := errgroup.WithContext(context.Background())
		eg.Go(func() error {
			return fetcherHashSetToGetDlLocsReqsThread(ctx, reqCh, nil, resCh, 8, "", testIdFunc)
		})

		// First send a batch of 16 hashes.
		{
			hs := make(hash.HashSet)
			for i := 0; i < 16; i++ {
				var h hash.Hash
				h[0] = byte(i)
				hs.Insert(h)
			}
			reqCh <- hs
		}
		// And read the two requests that get formed...
		apiReq := <-resCh
		assert.NotNil(t, apiReq)
		assert.Len(t, apiReq.ChunkHashes, 8)
		apiReq = <-resCh
		assert.NotNil(t, apiReq)
		assert.Len(t, apiReq.ChunkHashes, 8)

		// Next send 12 batches of one...
		{
			for i := 0; i < 12; i++ {
				hs := make(hash.HashSet)
				var h hash.Hash
				h[0] = byte(i)
				hs.Insert(h)
				reqCh <- hs
			}
		}
		// Read one batch of 8...
		apiReq = <-resCh
		assert.NotNil(t, apiReq)
		assert.Len(t, apiReq.ChunkHashes, 8)

		// Send 8 more batches of one...
		{
			for i := 12; i < 20; i++ {
				hs := make(hash.HashSet)
				var h hash.Hash
				h[0] = byte(i)
				hs.Insert(h)
				reqCh <- hs
			}
		}
		// Read a batch of 8 and a batch of 4...
		apiReq = <-resCh
		assert.NotNil(t, apiReq)
		assert.Len(t, apiReq.ChunkHashes, 8)
		apiReq = <-resCh
		assert.NotNil(t, apiReq)
		assert.Len(t, apiReq.ChunkHashes, 4)

		close(reqCh)
		assert.NoError(t, eg.Wait())
	})
}

func testIdFunc() (*remotesapi.RepoId, string) {
	return new(remotesapi.RepoId), ""
}
