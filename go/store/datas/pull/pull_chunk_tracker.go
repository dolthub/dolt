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
	"sync"

	"github.com/dolthub/dolt/go/store/hash"
)

type HasManyer interface {
	HasMany(context.Context, hash.HashSet) (hash.HashSet, error)
}

type TrackerConfig struct {
	BatchSize int

	HasManyer HasManyer
}

const hasManyThreadCount = 3

type PullChunkTracker struct {
	ctx  context.Context
	seen hash.HashSet
	cfg  TrackerConfig
	wg   sync.WaitGroup

	uncheckedCh chan hash.Hash
	reqCh       chan *trackerGetAbsentReq
}

func NewPullChunkTracker(ctx context.Context, initial hash.HashSet, cfg TrackerConfig) *PullChunkTracker {
	ret := &PullChunkTracker{
		ctx:         ctx,
		seen:        make(hash.HashSet),
		cfg:         cfg,
		uncheckedCh: make(chan hash.Hash),
		reqCh:       make(chan *trackerGetAbsentReq),
	}
	ret.seen.InsertAll(initial)
	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		ret.thread(initial)
	}()
	return ret
}

func (t *PullChunkTracker) Seen(h hash.Hash) {
	if !t.seen.Has(h) {
		t.seen.Insert(h)
		t.addUnchecked(h)
	}
}

func (t *PullChunkTracker) Close() {
	close(t.uncheckedCh)
	t.wg.Wait()
}

func (t *PullChunkTracker) addUnchecked(h hash.Hash) {
	select {
	case t.uncheckedCh <- h:
	case <-t.ctx.Done():
	}
}

func (t *PullChunkTracker) GetChunksToFetch() (hash.HashSet, bool, error) {
	var req trackerGetAbsentReq
	req.ready = make(chan struct{})

	select {
	case t.reqCh <- &req:
	case <-t.ctx.Done():
		return nil, false, t.ctx.Err()
	}

	select {
	case <-req.ready:
	case <-t.ctx.Done():
		return nil, false, t.ctx.Err()
	}

	return req.hs, req.ok, req.err
}

func (t *PullChunkTracker) thread(initial hash.HashSet) {
	doneCh := make(chan struct{})
	hasManyReqCh := make(chan trackerHasManyReq)
	hasManyRespCh := make(chan trackerHasManyResp)

	var wg sync.WaitGroup
	wg.Add(hasManyThreadCount)

	for i := 0; i < hasManyThreadCount; i++ {
		go func() {
			defer wg.Done()
			hasManyThread(t.ctx, t.cfg.HasManyer, hasManyReqCh, hasManyRespCh, doneCh)
		}()
	}

	defer func() {
		close(doneCh)
		wg.Wait()
	}()

	unchecked := make([]hash.HashSet, 0)
	absent := make([]hash.HashSet, 0)

	var err error
	outstanding := 0

	if len(initial) > 0 {
		unchecked = append(unchecked, initial)
		outstanding += 1
	}

	for {
		var thisReqCh = t.reqCh
		if outstanding != 0 && len(absent) == 0 {
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
			if !ok {
				return
			}
			if len(unchecked) == 0 || len(unchecked[len(unchecked)-1]) >= t.cfg.BatchSize {
				outstanding += 1
				unchecked = append(unchecked, make(hash.HashSet))
			}
			unchecked[len(unchecked)-1].Insert(h)
		case resp := <-hasManyRespCh:
			outstanding -= 1
			if resp.err != nil {
				err = errors.Join(err, resp.err)
			} else if len(resp.hs) > 0 {
				absent = append(absent, resp.hs)
			}
		case thisHasManyReqCh <- hasManyReq:
			copy(unchecked[:], unchecked[1:])
			if len(unchecked) > 1 {
				unchecked[len(unchecked)-1] = nil
			}
			unchecked = unchecked[:len(unchecked)-1]
		case req := <-thisReqCh:
			if err != nil {
				req.err = err
				close(req.ready)
				err = nil
			} else if len(absent) == 0 {
				req.ok = false
				close(req.ready)
			} else {
				req.ok = true
				req.hs = absent[0]
				var i int
				for i = 1; i < len(absent); i++ {
					if len(req.hs)+len(absent[i]) < t.cfg.BatchSize {
						req.hs.InsertAll(absent[i])
					} else {
						break
					}
				}
				copy(absent[:], absent[i:])
				for j := range absent[:len(absent)-i] {
					absent[len(absent)-i+j] = nil
				}
				absent = absent[:len(absent)-i]
				close(req.ready)
			}
		case <-t.ctx.Done():
			return
		}
	}
}

// Run by a PullChunkTracker, calls HasMany on a batch of addresses and delivers the results.
func hasManyThread(ctx context.Context, hasManyer HasManyer, reqCh <-chan trackerHasManyReq, respCh chan<- trackerHasManyResp, doneCh <-chan struct{}) {
	for {
		select {
		case req := <-reqCh:
			hs, err := hasManyer.HasMany(ctx, req.hs)
			if err != nil {
				select {
				case respCh <- trackerHasManyResp{err: err}:
				case <-ctx.Done():
					return
				case <-doneCh:
					return
				}
			} else {
				select {
				case respCh <- trackerHasManyResp{hs: hs}:
				case <-ctx.Done():
					return
				case <-doneCh:
					return
				}
			}
		case <-doneCh:
			return
		case <-ctx.Done():
			return
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
	hs    hash.HashSet
	err   error
	ok    bool
	ready chan struct{}
}
