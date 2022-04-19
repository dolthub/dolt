// Copyright 2020 Dolthub, Inc.
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

package types

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/hash"
)

type parallelRefWalkerWork struct {
	vals ValueSlice
	res  chan []hash.Hash
}

type parallelRefWalker struct {
	ctx         context.Context
	eg          *errgroup.Group
	concurrency int
	nbf         *NomsBinFormat
	work        chan parallelRefWalkerWork
}

func (w *parallelRefWalker) goWork() error {
	for {
		select {
		case <-w.ctx.Done():
			return w.ctx.Err()
		case work, ok := <-w.work:
			if !ok {
				return nil
			}
			var res []hash.Hash
			for _, v := range work.vals {
				err := v.walkRefs(w.nbf, func(r Ref) error {
					res = append(res, r.TargetHash())
					return nil
				})
				if err != nil {
					return err
				}
			}
			select {
			case work.res <- res:
				break
			case <-w.ctx.Done():
				return w.ctx.Err()
			}
		}
	}
}

func (w *parallelRefWalker) sendWork(work parallelRefWalkerWork) error {
	select {
	case w.work <- work:
		return nil
	case <-w.ctx.Done():
		return w.ctx.Err()
	}
}

func (w *parallelRefWalker) sendAllWork(vals ValueSlice) (int, chan []hash.Hash, error) {
	resCh := make(chan []hash.Hash, w.concurrency)
	i, numSent := 0, 0
	step := len(vals)/w.concurrency + 1
	for i < len(vals) {
		j := i + step
		if j > len(vals) {
			j = len(vals)
		}
		if err := w.sendWork(parallelRefWalkerWork{
			vals[i:j],
			resCh,
		}); err != nil {
			return 0, nil, err
		}
		i = j
		numSent++
	}
	return numSent, resCh, nil
}

func (w *parallelRefWalker) GetRefs(visited hash.HashSet, vals ValueSlice) ([]hash.Hash, error) {
	res := []hash.Hash{}
	numSent, resCh, err := w.sendAllWork(vals)
	if err != nil {
		return nil, err
	}
	for i := 0; i < numSent; i++ {
		select {
		case b := <-resCh:
			for _, r := range b {
				if !visited.Has(r) {
					res = append(res, r)
					visited.Insert(r)
				}
			}
		case <-w.ctx.Done():
			return nil, w.ctx.Err()
		}
	}
	return res, nil
}

func (w *parallelRefWalker) GetRefSet(visited hash.HashSet, vals ValueSlice) (hash.HashSet, error) {
	res := make(hash.HashSet)
	numSent, resCh, err := w.sendAllWork(vals)
	if err != nil {
		return nil, err
	}
	for i := 0; i < numSent; i++ {
		select {
		case b := <-resCh:
			for _, r := range b {
				if !visited.Has(r) {
					res[r] = struct{}{}
					visited.Insert(r)
				}
			}
		case <-w.ctx.Done():
			return nil, w.ctx.Err()
		}
	}
	return res, nil
}

func (w *parallelRefWalker) Close() error {
	close(w.work)
	return w.eg.Wait()
}

// |parallelRefWalker| provides a way to walk the |Ref|s in a |ValueSlice|
// using background worker threads to exploit hardware parallelism in cases
// where walking the merkle-DAG can become CPU bound. Construct a
// |parllelRefWalker| with a configured level of |concurrency| and then call
// |GetRefs(hash.HashSet, ValueSlice)| with the |ValueSlice| to get back a
// slice of |hash.Hash| for all the |Ref|s which appear in the values of
// |ValueSlice|. |GetRefs| will not return any |Ref|s which already appear in
// the |visited| set, and it will add all |Ref|s returned to the |visited| set.
// The worker threads should be shutdown with |Close()| after the walker is no
// longer needed.
//
// If any errors are encountered when walking |Ref|s, |parallelRefWalker| will
// enter a terminal error state where it will always return a non-|nil|
// |error|. A |parallelRefWalker| will also enter a terminal error state if the
// |ctx| provided to |newParallelRefWalker| is canceled or exceeds its
// deadline. |GetRefs| must not be called on |parallelRefWalker| after |Close|
// is called.
func newParallelRefWalker(ctx context.Context, nbf *NomsBinFormat, concurrency int) *parallelRefWalker {
	eg, ctx := errgroup.WithContext(ctx)
	res := &parallelRefWalker{
		ctx,
		eg,
		concurrency,
		nbf,
		make(chan parallelRefWalkerWork, concurrency),
	}
	for i := 0; i < concurrency; i++ {
		res.eg.Go(res.goWork)
	}
	return res
}
