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

package pull

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/hash"
)

type HasManyer interface {
	HasMany(context.Context, hash.HashSet) (hash.HashSet, error)
}

type TrackerConfig struct {
	HasManyer HasManyer
	BatchSize int
}

const hasManyThreadCount = 3

// A PullChunkTracker keeps track of seen chunk addresses and returns every
// seen chunk address which is not already in the destination database exactly
// once. A Puller instantiantes one of these with the initial set of addresses
// to pull, and repeatedly calls |GetChunksToFetch|. It passes in all
// references it finds in the fetched chunks to |Seen|, and continues to call
// |GetChunksToFetch| and deliver new addresses to |Seen| until
// |GetChunksToFetch| returns |false| from its |more| return boolean.
//
// PullChunkTracker is able to call |HasMany| on the destination database in
// parallel with other work the Puller does and abstracts out the logic for
// keeping track of seen, unchecked and to pull hcunk addresses.
type PullChunkTracker struct {
	seen hash.HashSet
	cfg  TrackerConfig

	uncheckedCh chan hash.Hash
	processedCh chan struct{}
	doneCh      chan struct{}
	reqCh       chan *trackerGetAbsentReq
}

func NewPullChunkTracker(cfg TrackerConfig) *PullChunkTracker {
	ret := &PullChunkTracker{
		seen:        make(hash.HashSet),
		cfg:         cfg,
		uncheckedCh: make(chan hash.Hash),
		processedCh: make(chan struct{}),
		doneCh:      make(chan struct{}),
		reqCh:       make(chan *trackerGetAbsentReq),
	}
	return ret
}

func (t *PullChunkTracker) Run(ctx context.Context, initial hash.HashSet) error {
	defer close(t.doneCh)
	t.seen.InsertAll(initial)
	return t.reqRespThread(ctx, initial)
}

func (t *PullChunkTracker) Seen(ctx context.Context, h hash.Hash) {
	if !t.seen.Has(h) {
		t.seen.Insert(h)
		t.addUnchecked(ctx, h)
	}
}

// Call this for every returned hash that has been successfully processed.
//
// GetChunksToFetch() requires a matching |TickProcessed| call for each
// returned Hash before it will return |hasMany == false|.
func (t *PullChunkTracker) TickProcessed(ctx context.Context) {
	select {
	case t.processedCh <- struct{}{}:
	case <-ctx.Done():
	}
}

func (t *PullChunkTracker) Close() {
	close(t.uncheckedCh)
	<-t.doneCh
}

func (t *PullChunkTracker) addUnchecked(ctx context.Context, h hash.Hash) {
	select {
	case t.uncheckedCh <- h:
	case <-ctx.Done():
	}
}

func (t *PullChunkTracker) GetChunksToFetch(ctx context.Context) (hash.HashSet, bool, error) {
	var req trackerGetAbsentReq
	req.ready = make(chan struct{})

	select {
	case t.reqCh <- &req:
	case <-ctx.Done():
		return nil, false, context.Cause(ctx)
	}

	select {
	case <-req.ready:
	case <-ctx.Done():
		return nil, false, context.Cause(ctx)
	}

	return req.hs, req.ok, req.err
}

// The main logic of the PullChunkTracker, receives requests from other threads
// and responds to them.
func (t *PullChunkTracker) reqRespThread(ctx context.Context, initial hash.HashSet) error {
	doneCh := make(chan struct{})
	hasManyReqCh := make(chan trackerHasManyReq)
	hasManyRespCh := make(chan trackerHasManyResp)

	eg, ctx := errgroup.WithContext(ctx)

	for range hasManyThreadCount {
		eg.Go(func() error {
			return hasManyThread(ctx, t.cfg.HasManyer, hasManyReqCh, hasManyRespCh, doneCh)
		})
	}

	eg.Go(func() error {
		defer close(doneCh)

		unchecked := make([]hash.HashSet, 0)
		absent := make([]hash.HashSet, 0)

		var err error
		outstanding := 0
		unprocessed := 0

		if len(initial) > 0 {
			unchecked = append(unchecked, initial)
			outstanding += 1
		}

		for {
			var thisReqCh = t.reqCh
			if len(absent) == 0 && (outstanding != 0 || unprocessed != 0) {
				// If we are waiting for a HasMany response and we don't currently have any
				// absent addresses to return, block any absent requests.
				thisReqCh = nil
			}

			var thisHasManyReqCh chan trackerHasManyReq
			var hasManyReq trackerHasManyReq
			if len(unchecked) > 0 {
				hasManyReq.hs = unchecked[0]
				thisHasManyReqCh = hasManyReqCh
			}

			select {
			case h, ok := <-t.uncheckedCh:
				// uncheckedCh closing means the PullChunkTracker was closed.
				if !ok {
					return nil
				}
				// |h| is a hash we need to check against HasMany on the destination.
				// It could become a hash we fetch from source and add to the destination
				// or it could already be present.
				if len(unchecked) == 0 || len(unchecked[len(unchecked)-1]) >= t.cfg.BatchSize {
					outstanding += 1
					unchecked = append(unchecked, make(hash.HashSet))
				}
				unchecked[len(unchecked)-1].Insert(h)
			case resp := <-hasManyRespCh:
				// A hasMany response came back.
				outstanding -= 1
				if resp.err != nil {
					err = errors.Join(err, resp.err)
				} else if len(resp.hs) > 0 {
					// Add all the resp.hs hashes, those which are not already present
					// in dest, to our batches of absent hashes we will return through
					// GetChunksToFetch.
					absent = appendAbsent(absent, resp.hs, t.cfg.BatchSize)
				}
			case thisHasManyReqCh <- hasManyReq:
				// We delivered a hasMany request to a hasManyThread.
				// Remove it here. We do not need to update |outstanding|, since
				// it was updated when we created the new batch in |unchecked|.
				_, unchecked = pop(unchecked)
			case <-t.processedCh:
				// TickProcessed helps us keep track of chunks which we returned from GetChunksToFetch
				// and which are still being processed by WalkAddrs. This only gets called after all
				// unchecked hashes in the chunks have been delivered to us.
				unprocessed -= 1
			case req := <-thisReqCh:
				// A request for GetChunksToFetch.
				if err != nil {
					// Deliver an error we experienced. HasMany can error, and this is where
					// a client sees it, for example.
					req.err = err
					close(req.ready)
					err = nil
				} else if len(absent) == 0 {
					// We have no more chunks to deliver. If len(absent) == 0 but we had
					// unprocessed stuff, we would have had a |nil| |thisReqCh|. The fact that
					// we accepted the request means we are ready to tell the client that there
					// are no more chunks to fetch.
					req.ok = false
					close(req.ready)
				} else {
					// |absent[0]| is as full a batch as we have.
					req.ok = true
					req.hs, absent = pop(absent)
					unprocessed += len(req.hs)
					close(req.ready)
				}
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
	})

	return eg.Wait()
}

// pop returns the first element of s and the remaining elements of
// s. It copies any suffix to the front of |s| and nils the last
// element of |s| so that memory doesn't leak through |s[1:]| retaining
// s[0].
func pop[T any](s []T) (T, []T) {
	ret := s[0]
	copy(s[:], s[1:])
	var empty T
	s[len(s)-1] = empty
	return ret, s[:len(s)-1]
}

// appendAbsent adds all elements in |toadd| to HashSets at the end of |absent|. It creates new HashSets
// at the end of |absent| if inserting into an existing set would cause its size to exceed |sz|.
//
// As a special case, |appendAbsent| just returns []hash.HashSet{toadd} if |absent| is already empty.
func appendAbsent(absent []hash.HashSet, toadd hash.HashSet, sz int) []hash.HashSet {
	if len(toadd) == 0 {
		return absent
	}
	// Don't bother splitting up toadd to |sz| if it is the first batch.
	if len(absent) == 0 {
		absent = append(absent, toadd)
		return absent
	}
	for h := range toadd {
		if len(absent[len(absent)-1]) >= sz {
			absent = append(absent, make(hash.HashSet))
		}
		absent[len(absent)-1].Insert(h)
	}
	return absent
}

// Run by a PullChunkTracker, calls HasMany on a batch of addresses and delivers the results.
func hasManyThread(ctx context.Context, hasManyer HasManyer, reqCh <-chan trackerHasManyReq, respCh chan<- trackerHasManyResp, doneCh <-chan struct{}) error {
	for {
		select {
		case req := <-reqCh:
			hs, err := hasManyer.HasMany(ctx, req.hs)
			if err != nil {
				select {
				case respCh <- trackerHasManyResp{err: err}:
				case <-ctx.Done():
					return context.Cause(ctx)
				case <-doneCh:
					return nil
				}
			} else {
				select {
				case respCh <- trackerHasManyResp{hs: hs}:
				case <-ctx.Done():
					return context.Cause(ctx)
				case <-doneCh:
					return nil
				}
			}
		case <-doneCh:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

// Sent by the tracker thread to a HasMany thread, includes a batch of
// addresses to HasMany. The response comes back to the tracker thread on a
// separate channel as a |trackerHasManyResp|.
type trackerHasManyReq struct {
	hs hash.HashSet
}

// Sent by the HasMany thread back to the tracker thread.
// If HasMany returned an error, it will be returned here.
type trackerHasManyResp struct {
	hs  hash.HashSet
	err error
}

// Sent by a client calling |GetChunksToFetch| to the tracker thread. The
// tracker thread will return a batch of chunk addresses that need to be
// fetched from source and added to destination.
//
// This will block until HasMany requests are completed.
//
// If |ok| is |false|, then the Tracker is closing because every absent address
// has been delivered.
type trackerGetAbsentReq struct {
	err   error
	hs    hash.HashSet
	ready chan struct{}
	ok    bool
}
