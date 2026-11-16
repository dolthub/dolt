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
	"time"

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

// StatsRecorder is an optional sink, supplied by the caller that creates a
// ChunkFetcher, which receives callbacks as the fetcher downloads byte ranges
// from the underlying ChunkStore. It is defined here, in a package both the
// puller (store/datas/pull) and the remote chunk store
// (libraries/doltcore/remotestorage) depend on, so that a per-operation
// recorder can be threaded into the fetcher without an import cycle.
//
// The |size| reported to these callbacks is the length of the (possibly
// coalesced) byte range being downloaded, which includes any "dark" bytes
// fetched between requested chunks as a result of range coalescing. This is
// distinct from the number of decompressed chunk bytes ultimately delivered to
// the caller.
//
// Implementations must be safe for concurrent use; callbacks fire from multiple
// download goroutines.
type StatsRecorder interface {
	// RecordTimeToFirstByte is called once per download attempt, after the
	// response headers for the range request have been received.
	RecordTimeToFirstByte(retry int, size uint64, d time.Duration)
	// RecordDownloadAttemptStart is called at the start of every download
	// attempt for a range. |retry| is 0 for the first attempt and increments
	// for each subsequent retry of the same range.
	RecordDownloadAttemptStart(retry int, offset, size uint64)
	// RecordDownloadComplete is called once per range, after the entire range
	// has been successfully downloaded (across any retries).
	RecordDownloadComplete(retry int, size uint64, d time.Duration)
}
