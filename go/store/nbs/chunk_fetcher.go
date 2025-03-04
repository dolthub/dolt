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

package nbs

import (
	"context"

	"github.com/dolthub/dolt/go/store/hash"
)

// A ChunkFetcher is a batching, stateful, potentially concurrent interface to
// fetch lots of chunks from a ChunkStore. A caller is expected to call
// `Get()` and `Recv()` concurrently. Unless there is an error, for every
// single Hash passed to Get, a corresponding Recv() call will deliver the
// contents of the chunk. When a caller is done with a ChunkFetcher, they
// should call |CloseSend()|. After CloseSend, all requested hashes have been
// delivered through Recv(), Recv() will return `io.EOF`.
//
// A ChunkFetcher should be Closed() when it is no longer needed. In non-error
// cases, this will typically be after Recv() has delivererd io.EOF. If Close
// is called before Recv() delivers io.EOF, there is no guarantee that all
// requested chunks will be delivered through Recv().
//
// In contrast to interfaces to like GetManyCompressed on ChunkStore, if the
// chunk is not in the underlying database, then Recv() will return an
// nbs.CompressedChunk with its Hash set, but with empty contents.
//
// Other than an io.EOF from Recv(), any |error| returned from any method
// indicates an underlying problem wtih fetching requested chunks from the
// ChunkStore. A ChunkFetcher is single use and cannot be used effectively
// after an error is returned.
type ChunkFetcher interface {
	Get(ctx context.Context, hashes hash.HashSet) error

	CloseSend() error

	Recv(context.Context) (ToChunker, error)

	Close() error
}
