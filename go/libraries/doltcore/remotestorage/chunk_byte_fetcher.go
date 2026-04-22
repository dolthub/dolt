// Prototype: client-side driver for the StreamChunks RPC. Enabled by
// DOLT_PROTO_CHUNKS=1 on the client. See FETCH_PERFORMANCE_NOTES.md
// (Section E) and the RPC definition in chunkstore.proto.

package remotestorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

// Client-side knobs for the StreamChunks path, read once at package
// init. Most are env-gated so we can experiment without rebuilding
// the server. See FETCH_PERFORMANCE_NOTES.md Section E.
var (
	// Batcher (Nagle on the client side).
	//
	// streamChunksMinBatchSize: a sub-threshold batch is held for up
	//   to streamChunksBatchDelay waiting for more hashes to coalesce
	//   before it is dispatched. Batches at or above the threshold
	//   dispatch immediately.
	// streamChunksMaxBatchSize: hard cap on hashes per outgoing Get.
	streamChunksMinBatchSize = envIntOr("DOLT_PROTO_CHUNKS_MIN_BATCH", 64)
	streamChunksMaxBatchSize = envIntOr("DOLT_PROTO_CHUNKS_MAX_BATCH", 512)
	streamChunksBatchDelay   = envDurationOr("DOLT_PROTO_CHUNKS_BATCH_DELAY", 2*time.Millisecond)

	// Server-side knobs shipped to the server via the Handshake.
	// Zero means "let the server pick its default"; valid non-zero
	// values override the server's default.
	streamChunksServerConcurrency = envNonNegIntOr("DOLT_PROTO_CHUNKS_SERVER_CONCURRENCY", 0)
	streamChunksServerMinBatch    = envNonNegIntOr("DOLT_PROTO_CHUNKS_SERVER_MIN_BATCH", 0)
	streamChunksServerMaxBatch    = envNonNegIntOr("DOLT_PROTO_CHUNKS_SERVER_MAX_BATCH", 0)
	streamChunksServerDebounce    = envDurationOr("DOLT_PROTO_CHUNKS_SERVER_DEBOUNCE", 0)
)

// envDurationOr mirrors envIntOr (in chunk_store.go) for time.Duration
// values. An unset or unparseable value returns |def|. Durations may
// be zero.
func envDurationOr(name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "remotestorage: ignoring invalid %s=%q, using default %v\n", name, v, def)
		return def
	}
	return d
}

// envNonNegIntOr mirrors envIntOr but permits zero as a valid value
// so callers can distinguish "unset" (use code default) from
// "explicitly zero" (use the zero semantics). Negative values are
// rejected with a warning.
func envNonNegIntOr(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		fmt.Fprintf(os.Stderr, "remotestorage: ignoring invalid %s=%q, using default %d\n", name, v, def)
		return def
	}
	return n
}

// fetcherHashSetToGetsThread is the StreamChunks-path batcher.
// Converts HashSets arriving on |reqCh| into *StreamChunksRequest_Get
// batches on |resCh|, up to getLocsBatchSize hashes each. Unlike the
// generic fetcherHashSetToReqsThread, this one applies Nagle-like
// accumulation to sub-threshold batches so the DAG-walk-tail doesn't
// trickle one-hash Gets up the wire when more hashes are seconds away.
//
// The assignment of get_seq to each Get happens downstream in
// fetcherStreamChunksThreads, right before Send; we don't stamp it
// here because the send goroutine is the one that has to guarantee
// monotonicity.
func fetcherHashSetToGetsThread(
	ctx context.Context,
	reqCh chan hash.HashSet,
	abortCh chan struct{},
	resCh chan *remotesapi.StreamChunksRequest_Get,
) error {
	var addrs [][]byte

	// Opportunistically drain any HashSets already queued on reqCh.
	// Appends all available hashes to |addrs| and returns. If reqCh
	// is closed, sets the outer reqCh local to nil via the return.
	drain := func(cur chan hash.HashSet) chan hash.HashSet {
		for {
			select {
			case hs, ok := <-cur:
				if !ok {
					return nil
				}
				for h := range hs {
					addrs = append(addrs, h[:])
				}
			default:
				return cur
			}
		}
	}

	var timerC <-chan time.Time
	var timerFired bool
	for {
		closing := reqCh == nil
		if closing && len(addrs) == 0 {
			close(resCh)
			return nil
		}

		var sendCh chan *remotesapi.StreamChunksRequest_Get
		var sendVal *remotesapi.StreamChunksRequest_Get
		var thisLen int
		if len(addrs) > 0 {
			end := len(addrs)
			st := end - streamChunksMaxBatchSize
			if st < 0 {
				st = 0
			}
			thisLen = end - st

			sendNow := thisLen >= streamChunksMinBatchSize || closing || timerFired
			if sendNow {
				flat := make([]byte, 0, hash.ByteLen*thisLen)
				for _, h := range addrs[st:end] {
					flat = append(flat, h...)
				}
				sendVal = &remotesapi.StreamChunksRequest_Get{ChunkHashes: flat}
				sendCh = resCh
			} else if timerC == nil {
				// Arm the accumulation timer for the current
				// sub-threshold batch. We do not reset it when
				// more hashes arrive — the timer bounds the
				// total wait since we first had something to
				// send, not since we last received.
				timerC = time.After(streamChunksBatchDelay)
			}
		}

		select {
		case hs, ok := <-reqCh:
			if !ok {
				reqCh = nil
				break
			}
			for h := range hs {
				addrs = append(addrs, h[:])
			}
			reqCh = drain(reqCh)
		case <-timerC:
			timerC = nil
			timerFired = true
		case sendCh <- sendVal:
			addrs = addrs[:len(addrs)-thisLen]
			timerC = nil
			timerFired = false
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-abortCh:
			return errors.New("early shutdown before all chunks fetched")
		}
	}
}

// fetcherStreamChunksThreads opens the StreamChunks bidi stream and
// runs the full send/receive loop. Each batch of hashes read from
// |getCh| becomes one StreamChunksRequest_Get tagged with a
// monotonic get_seq; each chunk the server sends back is rehydrated
// into a nbs.ToChunker and delivered on |resCh|. Missing hashes (on
// a final-flagged Chunk) are delivered as empty nbs.CompressedChunk
// values, matching the existing missing-chunk protocol on this
// channel.
//
// Deliberately does not use reliable.MakeCall: the RPC has no 1:1 req/
// resp correspondence, so a transparent stream rebuild would need
// request-replay logic we do not want to design in the prototype. On
// any stream error we tear down the whole fetcher.
//
// Gets are pipelined: the server handles multiple batches in parallel
// on the same stream, and responses for different get_seq values may
// interleave on the wire. The client demultiplexes via a map from
// get_seq to the batch's chunk_hashes buffer.
func fetcherStreamChunksThreads(
	ctx context.Context,
	getCh <-chan *remotesapi.StreamChunksRequest_Get,
	resCh chan nbs.ToChunker,
	client remotesapi.ChunkStoreServiceClient,
	getRepoId func() (*remotesapi.RepoId, string),
	repoPath string,
	storeRepoToken func(string),
	host string,
) error {
	repoId, repoToken := getRepoId()
	stream, err := client.StreamChunks(ctx)
	if err != nil {
		return NewRpcError(err, "StreamChunks", host, nil)
	}

	handshake := &remotesapi.StreamChunksRequest{
		Payload: &remotesapi.StreamChunksRequest_Handshake_{
			Handshake: &remotesapi.StreamChunksRequest_Handshake{
				RepoId:             repoId,
				RepoToken:          repoToken,
				RepoPath:           repoPath,
				MaxConcurrentGets:  uint32(streamChunksServerConcurrency),
				MinBatchSize:       uint32(streamChunksServerMinBatch),
				MaxBatchSize:       uint32(streamChunksServerMaxBatch),
				BatchDebounceNanos: uint64(streamChunksServerDebounce.Nanoseconds()),
			},
		},
	}
	if err := stream.Send(handshake); err != nil {
		return NewRpcError(err, "StreamChunks", host, handshake)
	}

	ack, err := stream.Recv()
	if err != nil {
		return NewRpcError(err, "StreamChunks", host, handshake)
	}
	if ack.GetHandshake() == nil {
		return NewRpcError(errors.New("expected handshake ack, got a different response"), "StreamChunks", host, handshake)
	}
	if tok := ack.GetHandshake().RepoToken; tok != "" {
		storeRepoToken(tok)
	}

	// Demultiplexing state: get_seq -> chunk_hashes buffer of the
	// corresponding in-flight Get. Populated by the sender before
	// Send, removed by the receiver on a final-flagged Chunk with
	// the same get_seq. Access is via pendingMu; both threads touch
	// it at per-batch frequency, not per-chunk, so contention is low.
	var pendingMu sync.Mutex
	pending := make(map[uint64][]byte)

	eg, ctx := errgroup.WithContext(ctx)

	// Sender.
	eg.Go(func() error {
		var nextSeq uint64 = 1
		for {
			select {
			case get, ok := <-getCh:
				if !ok {
					// No more hash batches to request. Close our
					// half of the bidi stream so the server sees
					// EOF and exits cleanly after draining.
					return stream.CloseSend()
				}
				seq := nextSeq
				nextSeq++
				get.GetSeq = seq

				// Record the batch before Send so the receiver
				// can always find it by the time any response
				// arrives.
				pendingMu.Lock()
				pending[seq] = get.ChunkHashes
				pendingMu.Unlock()

				req := &remotesapi.StreamChunksRequest{
					Payload: &remotesapi.StreamChunksRequest_Get_{Get: get},
				}
				if err := stream.Send(req); err != nil {
					return NewRpcError(err, "StreamChunks", host, req)
				}
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
	})

	// Receiver.
	eg.Go(func() error {
		// dict_id -> DecompBundle. Stream-scoped, lifetime of this
		// call. Entries are never evicted; the server will simply
		// re-send the Dictionary if it needs to re-introduce one.
		dicts := make(map[uint32]*nbs.DecompBundle)

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				close(resCh)
				return nil
			}
			if err != nil {
				return NewRpcError(err, "StreamChunks", host, nil)
			}

			switch payload := resp.Payload.(type) {
			case *remotesapi.StreamChunksResponse_Dictionary_:
				dict := payload.Dictionary
				if dict.DictId == 0 {
					return NewRpcError(errors.New("server emitted Dictionary with dict_id=0"), "StreamChunks", host, nil)
				}
				bundle, berr := nbs.NewDecompBundle(dict.Data)
				if berr != nil {
					return NewRpcError(fmt.Errorf("NewDecompBundle: %w", berr), "StreamChunks", host, nil)
				}
				dicts[dict.DictId] = bundle

			case *remotesapi.StreamChunksResponse_Chunk_:
				chunk := payload.Chunk
				if chunk.GetSeq == 0 {
					return NewRpcError(errors.New("server sent Chunk with get_seq=0"), "StreamChunks", host, nil)
				}

				pendingMu.Lock()
				batch, ok := pending[chunk.GetSeq]
				if chunk.Final {
					delete(pending, chunk.GetSeq)
				}
				pendingMu.Unlock()
				if !ok {
					return NewRpcError(fmt.Errorf("server referenced unknown get_seq %d", chunk.GetSeq), "StreamChunks", host, nil)
				}

				// Deliver the chunk payload (if any) before
				// processing missing_indexes so the caller never
				// sees a chunk arrive after its batch's misses.
				if len(chunk.Data) > 0 {
					bs, ok := hashAtIndex(batch, chunk.RequestIndex)
					if !ok {
						return NewRpcError(errors.New("server returned Chunk.request_index out of range"), "StreamChunks", host, nil)
					}
					var h hash.Hash
					copy(h[:], bs)

					var tc nbs.ToChunker
					if chunk.DictId == 0 {
						cc, cerr := nbs.NewCompressedChunk(h, chunk.Data)
						if cerr != nil {
							return NewRpcError(fmt.Errorf("NewCompressedChunk: %w", cerr), "StreamChunks", host, nil)
						}
						tc = cc
					} else {
						bundle, ok := dicts[chunk.DictId]
						if !ok {
							return NewRpcError(fmt.Errorf("server referenced unknown dict_id %d", chunk.DictId), "StreamChunks", host, nil)
						}
						tc = nbs.NewArchiveToChunker(h, bundle, chunk.Data)
					}
					select {
					case resCh <- tc:
					case <-ctx.Done():
						return context.Cause(ctx)
					}
				}

				if chunk.Final {
					for _, idx := range chunk.MissingIndexes {
						bs, ok := hashAtIndex(batch, idx)
						if !ok {
							return NewRpcError(errors.New("server returned missing_index out of range"), "StreamChunks", host, nil)
						}
						var h hash.Hash
						copy(h[:], bs)
						select {
						case resCh <- nbs.CompressedChunk{H: h}:
						case <-ctx.Done():
							return context.Cause(ctx)
						}
					}
				}

			case *remotesapi.StreamChunksResponse_Handshake:
				return NewRpcError(errors.New("server emitted handshake ack after initial handshake"), "StreamChunks", host, nil)

			default:
				return NewRpcError(fmt.Errorf("server emitted unknown StreamChunks response payload: %T", resp.Payload), "StreamChunks", host, nil)
			}
		}
	})

	return eg.Wait()
}
