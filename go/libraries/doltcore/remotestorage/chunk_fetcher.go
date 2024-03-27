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
	"errors"
	"io"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

// A remotestorage.ChunkFetcher is a pipelined chunk fetcher for fetching a
// large number of chunks where the downloads may benefit from range
// coallescing, hedging, automatic retries, pipelining of download location
// retrieval with the fetching of the actual chunk bytes, etc.
//
// It is expected that one goroutine will be calling `Get()` with batches of
// addresses to retrieve, and another goroutine will be calling `Recv()`,
// reading fetched chunks.
//
// When all addresses have been delivered, calling `CloseSend()` will
// eventually cause the `Recv()` thread to read an `io.EOF` error, which
// indicates that all requested chunks have been delivered.
type ChunkFetcher struct {
	eg    *errgroup.Group
	egCtx context.Context

	toGetCh chan hash.HashSet
	resCh   chan nbs.CompressedChunk

	doneCh chan struct{}
}

func NewChunkFetcher(ctx context.Context) *ChunkFetcher {
	eg, ctx := errgroup.WithContext(ctx)
	ret := &ChunkFetcher{
		eg:    eg,
		egCtx: ctx,

		toGetCh: make(chan hash.HashSet),
		resCh:   make(chan nbs.CompressedChunk),

		doneCh: make(chan struct{}),
	}
	return ret
}

func (f *ChunkFetcher) Get(ctx context.Context, hashes hash.HashSet) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-f.egCtx.Done():
		return context.Cause(f.egCtx)
	case f.toGetCh <- hashes:
		return nil
	}
}

func (f *ChunkFetcher) CloseSend() error {
	close(f.toGetCh)
	return nil
}

func (f *ChunkFetcher) Recv(ctx context.Context) (nbs.CompressedChunk, error) {
	select {
	case <-ctx.Done():
		return nbs.CompressedChunk{}, context.Cause(ctx)
	case <-f.egCtx.Done():
		return nbs.CompressedChunk{}, context.Cause(f.egCtx)
	case cc, ok := <-f.resCh:
		if !ok {
			return nbs.CompressedChunk{}, io.EOF
		}
		return cc, nil
	}
}

func (f *ChunkFetcher) Close() error {
	close(f.doneCh)
	return f.eg.Wait()
}

// Reads HashSets from reqCh and batches all the received addresses
// into |GetDownloadLocsRequest| messages with up to |batchSize| chunk hashes
// in them. It delivers the messages to be send on |resCh|.
func fetcherHashSetToGetDlLocsReqsThread(ctx context.Context, reqCh chan hash.HashSet, resCh chan *remotesapi.GetDownloadLocsRequest, batchSize int, repoPath string, idFunc func() (*remotesapi.RepoId, string)) error {
	var addrs [][]byte
	var outbound [][]byte
	for {
		if reqCh == nil && len(addrs) == 0 {
			close(resCh)
			return nil
		}

		var thisResCh chan *remotesapi.GetDownloadLocsRequest
		var thisRes *remotesapi.GetDownloadLocsRequest

		if len(addrs) > 0 {
			end := len(addrs)
			st := end - batchSize
			if st < 0 {
				st = 0
			}
			if outbound == nil {
				outbound = make([][]byte, end-st)
			}
			outbound = append(outbound[:0], addrs[st:end]...)
			id, token := idFunc()
			thisRes = &remotesapi.GetDownloadLocsRequest{RepoId: id, RepoPath: repoPath, RepoToken: token, ChunkHashes: outbound[:]}
			thisResCh = resCh
		}

		select {
		case hs, ok := <-reqCh:
			if !ok {
				reqCh = nil
				break
			}
			for h := range hs {
				h := h
				addrs = append(addrs, h[:])
			}
		case thisResCh <- thisRes:
			addrs = addrs[:len(addrs)-len(thisRes.ChunkHashes)]
			outbound = nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

// Reads request messages off |reqCh| and sends them to a streaming RPC to turn
// them into download locations, which it delivers to |resCh|.
//
// On success, exactly one slice will be delivered on |resCh| for every message
// delivered in |reqCh|, and they will be delivered in order.
//
// This function handles backoff and retries for the underlying streaming RPC.

//
//
//   This this should do:
//
//   1) Read from |reqCh| immediately.
//   2) Not open StreamDownloadLocations unless it has something to deliver.
//   3) Before Send()ing on the stream, save the req to inflight.
//   4) Anytime we re-open a channel due to retry and backoff, deliver all inflight, in order, before reading anew from reqCh.
//   5) Anytime we successfully deliver a res, remove its corresponding req from inflight.
//   6) If reqCh is quiet for too long, shut down the stream.
//   7) If delivery on resCh takes too long, shut down the stream.
//
// States:
//
// InitialState --- we start here:
//   Stream is nil, reqCh is open, initialReqs is empty.
//   reqCh - req comes in: add req to initialReqs, goto WantsOpenWithRead.
//   reqCh - is closed: close(resCh), return nil.
// WantsOpenWithRead:
//   In this state, stream is nil, reqCh is open, initialReqs is non-empty.
//   Open stream with |client.StreamDownloadLocations(ctx)|. If err, potentially go to BackoffWantsOpenWithRead; otherwise, return err.
//   After opened successfully, go to OpenForInitialSend.
// OpenForInitialSend:
//   In this state, stream is non-nil, reqCh is open, initialReqs is non-empty.
//   No reads from reqCh.
//   One Send() call for each element in initialReqs, in order.
//   For each successful Recv(), send it on resCh.
//   For each successful send on resCh, pop the message from initialReqs.
//   Ones all initialReqs are successfully Send(), transition to OpenWithRead.
// OpenWithRead:
//   In this state, stream is non-nil, reqCh is open, initialReqs is potentially non-empty, but all initialReqs are sent.
//   Reads from reqCh.
//   reqCh - req comes in: add req to initialReqs (inflight), Send it.
//     error on Send: goto BackoffWantsOpenWithRead or return err.
//   reqCh - is closed: stream.CloseSend, goto OpenCloseSend.
//   reqCh - timeout: only if len(initialReqs) == 0, then, stream.CloseSend, goto InitialState
//   Recv() success - send on resCh
//     send success: remove from initialReqs
//     send failure: ctx is tearing down, return err
//     send timeout: ??? --- maybe close it down and transition to BackoffWantsOpenWithRead
// BackoffWantsOpenWithRead
//   initialReqs is non-empty.
//   after backoff: WantsOpenWithRead
//   ctx.Done(): return err.
//

type StateFunc func() (StateFunc, error)

func fetcherRPCDownloadLocsThread(ctx context.Context, reqCh chan *remotesapi.GetDownloadLocsRequest, resCh chan []*remotesapi.DownloadLoc, client remotesapi.ChunkStoreServiceClient) error {
	var state_InitialState StateFunc
	var state_WantsOpenWithRead StateFunc
	var state_OpenForInitialSend StateFunc
	var state_OpenWithRead StateFunc
	var state_BackoffWantsOpenWithRead StateFunc

	var streamCtx context.Context
	var streamCancel func(error)

	var stream remotesapi.ChunkStoreService_StreamDownloadLocationsClient
	var backoffDuration time.Duration
	bo := grpcBackOff(ctx)

	var initialReqs []*remotesapi.GetDownloadLocsRequest
	var initialReqsMu sync.Mutex

	processError := func(err error) (StateFunc, error) {
		err = processGrpcErr(err)
		pe := new(backoff.PermanentError)
		if errors.As(err, &pe) {
			return nil, pe.Err
		}
		backoffDuration = bo.NextBackOff()
		if backoffDuration == backoff.Stop {
			return nil, err
		}
		return state_BackoffWantsOpenWithRead, nil
	}

	state_InitialState = func() (StateFunc, error) {
		select {
		case req, ok := <-reqCh:
			if !ok {
				close(resCh)
				return nil, nil
			}
			initialReqs = append(initialReqs, req)
			return state_WantsOpenWithRead, nil
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}

	state_BackoffWantsOpenWithRead = func() (StateFunc, error) {
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-time.After(backoffDuration):
			return state_WantsOpenWithRead, nil
		}
	}

	state_WantsOpenWithRead = func() (StateFunc, error) {
		var err error
		streamCtx, streamCancel = context.WithCancelCause(ctx)
		stream, err = client.StreamDownloadLocations(streamCtx)
		if err != nil {
			return processError(err)
		}
		return state_OpenForInitialSend, nil
	}

	state_OpenForInitialSend = func() (StateFunc, error) {
		// We want to run reads and writes concurrently until we're able to deliver
		// all initialReqs on the Sending thread. Then we shut down our
		// readers and writers and transition to the state where we are
		// reading from reqCh.

		completed := 0
		doneCh := make(chan struct{})
		eg, ctx := errgroup.WithContext(ctx)
		eg.Go(func() error {
			for i := range initialReqs {
				err := stream.Send(initialReqs[i])
				if err != nil {
					// TODO: NewRpcError
					streamCancel(err)
					return err
				}
			}
			close(doneCh)
			return nil
		})
		eg.Go(func() error {
			for {
				if completed == len(initialReqs) {
					return nil
				}
				select {
				case <-doneCh:
					return nil
				default:
				}
				resp, err := stream.Recv()
				if err != nil {
					streamCancel(err)
					// TODO: NewRpcError
					return err
				}

				// TODO: repoToken.Store()

				select {
				case resCh <- resp.Locs:
					completed += 1
				case <-ctx.Done():
					return context.Cause(ctx)
					// we don't read doneCh here -- we need to deliver the thing we read...

					// TODO: a timeout delivering here could would
					// ideally put us in a state where we shut down
					// the stream but hold onto this result, and
					// then spin the stream back up (if needed),
					// after this is delivered successfully.
				}
			}
		})

		err := eg.Wait()
		copy(initialReqs, initialReqs[completed:])
		for i := len(initialReqs) - completed; i < len(initialReqs); i++ {
			initialReqs[i] = nil
		}
		initialReqs = initialReqs[:len(initialReqs)-completed]
		if err != nil {
			return processError(err)
		}
		return state_OpenWithRead, nil
	}

	state_OpenWithRead = func() (StateFunc, error) {

		var localNext func() (StateFunc, error)
		eg, ctx := errgroup.WithContext(ctx)
		eg.Go(func() error {
			timeoutD := time.Second * 20
			ticker := time.NewTicker(timeoutD)
			for {
				select {
				case req, ok := <-reqCh:
					if !ok {
						return stream.CloseSend()
					}
					initialReqsMu.Lock()
					initialReqs = append(initialReqs, req)
					initialReqsMu.Unlock()
					err := stream.Send(req)
					if err != nil {
						// TODO: NewRpcError
						streamCancel(err)
						return err
					}
					ticker.Reset(timeoutD)
				case <-ctx.Done():
					err := context.Cause(ctx)
					streamCancel(err)
					return err
				case <-ticker.C:
					// If we timeout here, we assume
					// quiesced get state for now and we
					// don't want to hold the stream open
					// unnecessarily. We shut it down and
					// move back into our InitialState.
					localNext = state_InitialState
					bo.Reset()
					return stream.CloseSend()
				}
			}
		})
		eg.Go(func() error {
			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					// TODO: NewRpcError
					streamCancel(err)
					return err
				}

				// TODO: repoToken.Store()

				select {
				case resCh <- resp.Locs:
					initialReqsMu.Lock()
					copy(initialReqs, initialReqs[1:])
					initialReqs[len(initialReqs)-1] = nil
					initialReqs = initialReqs[:len(initialReqs)-1]
					initialReqsMu.Unlock()
				case <-ctx.Done():
					err := context.Cause(ctx)
					streamCancel(err)
					return err

					// TODO: a timeout delivering would ideally put
					// us in a state where we shut down the stream
					// but hold onto this particular result, and
					// then spin the stream back up (if needed),
					// after this is delivered successfully.
				}
			}
		})

		err := eg.Wait()
		if err != nil {
			return processError(err)
		}
		// If we finished successfully, everything should be processed...
		if len(initialReqs) != 0 {
			panic("should have received and delivered something for every request...")
		}
		return localNext, nil
	}

	var curState = state_InitialState

	for {
		nextState, err := curState()
		if err != nil {
			return err
		}
		if nextState == nil {
			close(resCh)
			return nil
		}
		curState = nextState
	}
}
