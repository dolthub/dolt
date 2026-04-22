// Prototype: client-side driver for the StreamChunks RPC. Enabled by
// DOLT_PROTO_CHUNKS=1 on the client. See FETCH_PERFORMANCE_NOTES.md
// (Section E) and the RPC definition in chunkstore.proto.

package remotestorage

import (
	"context"
	"errors"
	"fmt"
	"io"

	"golang.org/x/sync/errgroup"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

// fetcherStreamChunksThreads opens the StreamChunks bidi stream and
// runs the full send/receive loop. Each batch of hashes read from
// |getCh| becomes one StreamChunksRequest_Get; each chunk the server
// sends back is rehydrated into a nbs.ToChunker and delivered on
// |resCh|. Missing hashes (on a final-flagged Chunk) are delivered as
// empty nbs.CompressedChunk values, matching the existing missing-chunk
// protocol on this channel.
//
// Deliberately does not use reliable.MakeCall: the RPC has no 1:1 req/
// resp correspondence, so a transparent stream rebuild would need
// request-replay logic we do not want to design in the prototype. On
// any stream error we tear down the whole fetcher.
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
				RepoId:    repoId,
				RepoToken: repoToken,
				RepoPath:  repoPath,
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

	// pendingCh carries the |chunk_hashes| buffer of each in-flight
	// Get to the receive goroutine. The server guarantees all
	// responses for Get N arrive before any response for Get N+1, so
	// the receiver treats this strictly as a FIFO and pops on each
	// final-flagged Chunk. Buffered generously so the sender rarely
	// blocks on the channel; the gRPC layer has its own per-stream
	// flow control.
	pendingCh := make(chan []byte, 1024)

	eg, ctx := errgroup.WithContext(ctx)

	// Sender.
	eg.Go(func() error {
		defer close(pendingCh)
		for {
			select {
			case get, ok := <-getCh:
				if !ok {
					// No more hash batches to request. Close our
					// half of the bidi stream so the server sees
					// EOF and exits cleanly after draining.
					return stream.CloseSend()
				}
				select {
				case pendingCh <- get.ChunkHashes:
				case <-ctx.Done():
					return context.Cause(ctx)
				}
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

		var currentBatch []byte
		popNext := func() error {
			var ok bool
			select {
			case currentBatch, ok = <-pendingCh:
				if !ok {
					return errors.New("server sent a chunk with no pending Get")
				}
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}

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
				if currentBatch == nil {
					if err := popNext(); err != nil {
						return err
					}
				}

				// Deliver the chunk payload (if any) before
				// processing the final transition so the caller
				// never sees a chunk arrive out of its batch.
				if len(chunk.Data) > 0 {
					bs, ok := hashAtIndex(currentBatch, chunk.RequestIndex)
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
						bs, ok := hashAtIndex(currentBatch, idx)
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
					currentBatch = nil
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
